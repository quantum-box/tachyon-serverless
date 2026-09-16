//! End-to-end tests: this test binary plays the HOST. It listens on a unix
//! socket, spawns the real bridge binary, performs the handshake with the
//! protocol crate and drives the session. The user process is the bridge
//! binary itself in `self-test-user` mode, so no other artifact is needed.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tachyon_serverless_protocol::{
    FrameCodec, GuestErrorKind, GuestMessage, HostMessage, LogPhase, LogStream, MAX_FRAME_BYTES,
    MAX_RESPONSE_PAYLOAD_BYTES, PROTOCOL_VERSION, decode_message, encode_message,
};
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

const T: Duration = Duration::from_secs(20);
const ENV_ID: &str = "env_01hzzzzzzzzzzzzzzzzzzzzzzb";

fn bridge_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_tachyon-serverless-runtime-bridge"))
}

/// What the host tells the bridge to run, and how the bridge is observed.
struct Launch {
    entrypoint: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    max_response_bytes: u64,
    /// Run the bridge with `RUST_LOG=trace` and capture its stderr.
    capture_stderr: bool,
}

impl Launch {
    /// The bridge binary itself in `self-test-user` mode.
    fn selftest(env: Vec<(String, String)>, max_response_bytes: u64) -> Self {
        Self {
            entrypoint: bridge_bin().to_string_lossy().into_owned(),
            args: vec!["self-test-user".into()],
            env,
            max_response_bytes,
            capture_stderr: false,
        }
    }
}

struct Session {
    child: Child,
    host: Framed<UnixStream, FrameCodec>,
    _dir: tempfile::TempDir,
    logs: Vec<GuestMessage>,
    /// Every frame received, in order (for post-mortem assertions).
    all: Vec<GuestMessage>,
    /// Everything the bridge wrote to stderr (when captured).
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl Session {
    async fn start(env: Vec<(String, String)>, max_response_bytes: u64) -> Self {
        Self::start_with(Launch::selftest(env, max_response_bytes)).await
    }

    async fn start_with(launch: Launch) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("bridge.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let mut cmd = Command::new(bridge_bin());
        cmd.arg("--transport")
            .arg("unix")
            .arg("--unix-path")
            .arg(&sock)
            .arg("--environment-id")
            .arg(ENV_ID)
            .arg("--runtime-api-addr")
            .arg("127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        if launch.capture_stderr {
            cmd.env("RUST_LOG", "trace")
                .env("NO_COLOR", "1")
                .stderr(Stdio::piped());
        } else if std::env::var_os("BRIDGE_TEST_VERBOSE").is_none() {
            cmd.stderr(Stdio::null());
        }
        let mut child = cmd.spawn().expect("spawn bridge");
        // Read continuously so a verbose bridge never blocks on a full pipe.
        let stderr = child.stderr.take().map(|mut pipe| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = pipe.read_to_end(&mut buf).await;
                buf
            })
        });
        let (stream, _) = timeout(T, listener.accept())
            .await
            .expect("accept")
            .unwrap();
        let mut session = Self {
            child,
            host: Framed::new(stream, FrameCodec),
            _dir: dir,
            logs: Vec::new(),
            all: Vec::new(),
            stderr,
        };
        match session.recv().await.expect("hello") {
            GuestMessage::Hello {
                protocol_version,
                bridge_version,
                environment_id,
                architecture,
                ..
            } => {
                assert_eq!(protocol_version, PROTOCOL_VERSION);
                assert_eq!(bridge_version, env!("CARGO_PKG_VERSION"));
                assert_eq!(environment_id, ENV_ID);
                assert_eq!(architecture, std::env::consts::ARCH);
            }
            other => panic!("expected hello, got {other:?}"),
        }
        let working_dir = session._dir.path().to_string_lossy().into_owned();
        session
            .send(HostMessage::HelloAck {
                environment_id: ENV_ID.into(),
                epoch: 1,
                entrypoint: launch.entrypoint,
                args: launch.args,
                env: launch.env,
                working_dir,
                init_timeout_ms: 15_000,
                max_response_bytes: launch.max_response_bytes,
                max_log_line_bytes: 4096,
            })
            .await;
        session
    }

    /// Captured bridge stderr; waits for the pipe to close (bridge exit).
    async fn stderr_text(&mut self) -> String {
        let task = self.stderr.take().expect("stderr was not captured");
        let bytes = timeout(T, task).await.expect("stderr eof").unwrap();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// Wait until the user process has logged its pid line.
    async fn wait_user_pid(&mut self) -> u32 {
        loop {
            if let Some(pid) = self.user_pid() {
                return pid;
            }
            assert!(self.recv().await.is_some(), "closed before the pid line");
        }
    }

    async fn send(&mut self, msg: HostMessage) {
        timeout(T, self.host.send(encode_message(&msg).unwrap()))
            .await
            .expect("send timeout")
            .unwrap();
    }

    /// Next frame of any kind; `None` when the bridge closed the connection.
    async fn recv(&mut self) -> Option<GuestMessage> {
        let frame = timeout(T, self.host.next()).await.expect("recv timeout")?;
        let msg: GuestMessage = decode_message(&frame.unwrap()).unwrap();
        self.all.push(msg.clone());
        if matches!(msg, GuestMessage::Log { .. }) {
            self.logs.push(msg.clone());
        }
        Some(msg)
    }

    /// Next frame that is neither a heartbeat nor a log line.
    async fn next_significant(&mut self) -> Option<GuestMessage> {
        loop {
            match self.recv().await? {
                GuestMessage::Heartbeat { .. } | GuestMessage::Log { .. } => continue,
                other => return Some(other),
            }
        }
    }

    async fn wait_ready(&mut self) {
        match self.next_significant().await {
            Some(GuestMessage::Ready { .. }) => {}
            other => panic!("expected ready, got {other:?}"),
        }
    }

    async fn invoke(&mut self, attempt_id: &str, payload: serde_json::Value) {
        self.send(HostMessage::Invoke {
            invocation_id: "inv_01hzzzzzzzzzzzzzzzzzzzzzzb".into(),
            attempt_id: attempt_id.into(),
            epoch: 1,
            event_type: "tachyon.invoke.v1".into(),
            deadline_ms: 4_102_444_800_000,
            trace_id: "trace-1".into(),
            payload,
        })
        .await;
    }

    /// Read until the bridge closes the connection.
    async fn drain(&mut self) {
        while self.next_significant().await.is_some() {}
    }

    async fn wait_exit(&mut self) -> std::process::ExitStatus {
        timeout(T, self.child.wait())
            .await
            .expect("bridge exit timeout")
            .unwrap()
    }

    /// PID printed by the self-test user process on its first stdout line.
    fn user_pid(&self) -> Option<u32> {
        self.logs.iter().find_map(|m| match m {
            GuestMessage::Log { line, .. } => line
                .strip_prefix("selftest pid=")
                .and_then(|p| p.trim().parse().ok()),
            _ => None,
        })
    }
}

fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 only checks for existence.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

async fn assert_pid_gone(pid: u32) {
    for _ in 0..100 {
        if !pid_alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("user process {pid} still alive");
}

#[tokio::test]
async fn echo_roundtrip_then_shutdown() {
    let mut s = Session::start(vec![("HELLO".into(), "world".into())], 1 << 20).await;
    s.wait_ready().await;
    s.invoke("att_echo", serde_json::json!({"name": "tachyon"}))
        .await;
    match s.next_significant().await {
        Some(GuestMessage::Response {
            attempt_id,
            epoch,
            payload,
            handler_ms,
        }) => {
            assert_eq!(attempt_id, "att_echo");
            assert_eq!(epoch, 1);
            assert_eq!(payload["echo"], serde_json::json!({"name": "tachyon"}));
            assert_eq!(payload["environment_id"], ENV_ID);
            assert!(handler_ms.is_some());
        }
        other => panic!("expected response, got {other:?}"),
    }
    let pid = s.user_pid().expect("init log line with pid");
    assert!(pid_alive(pid));

    s.send(HostMessage::Shutdown {
        reason: "test done".into(),
    })
    .await;
    s.drain().await;
    let status = s.wait_exit().await;
    assert_eq!(status.code(), Some(0));
    assert_pid_gone(pid).await;

    // Log frames: the pid line is init phase on stdout; the handler line is
    // stamped with the attempt id.
    assert!(s.logs.iter().any(|m| matches!(
        m,
        GuestMessage::Log { stream: LogStream::Stdout, phase: LogPhase::Init, attempt_id: None, line, .. }
            if line.starts_with("selftest pid=")
    )));
    assert!(s.logs.iter().any(|m| matches!(
        m,
        GuestMessage::Log { stream: LogStream::Stderr, phase: LogPhase::Handler, attempt_id: Some(a), line, .. }
            if a == "att_echo" && line.contains("selftest handled attempt att_echo")
    )));
    assert!(s.logs.iter().any(|m| matches!(
        m,
        GuestMessage::Log {
            stream: LogStream::Bridge,
            ..
        }
    )));
}

#[tokio::test]
async fn panic_and_handler_error_are_reported() {
    let mut s = Session::start(vec![], 1 << 20).await;
    s.wait_ready().await;

    s.invoke("att_panic", serde_json::json!({"panic": true}))
        .await;
    match s.next_significant().await {
        Some(GuestMessage::Error {
            attempt_id,
            epoch,
            error,
            error_type,
            message,
            ..
        }) => {
            assert_eq!(attempt_id, "att_panic");
            assert_eq!(epoch, 1);
            assert_eq!(error, GuestErrorKind::Panic);
            assert_eq!(error_type, "Runtime.Panic");
            assert!(message.contains("self-test panic requested"), "{message}");
        }
        other => panic!("expected panic error, got {other:?}"),
    }

    // The process survived the panic and keeps serving.
    s.invoke("att_fail", serde_json::json!({"fail": true}))
        .await;
    match s.next_significant().await {
        Some(GuestMessage::Error {
            attempt_id,
            error,
            error_type,
            ..
        }) => {
            assert_eq!(attempt_id, "att_fail");
            assert_eq!(error, GuestErrorKind::Handler);
            assert_eq!(error_type, "SelfTest.Failure");
        }
        other => panic!("expected handler error, got {other:?}"),
    }

    s.send(HostMessage::Shutdown {
        reason: "done".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
}

#[tokio::test]
async fn init_error_exits_with_3() {
    let mut s = Session::start(vec![("SELFTEST_FAIL_INIT".into(), "1".into())], 1 << 20).await;
    match s.next_significant().await {
        Some(GuestMessage::InitError {
            error_type,
            message,
            ..
        }) => {
            assert_eq!(error_type, "Runtime.InitError");
            assert!(message.contains("SELFTEST_FAIL_INIT"), "{message}");
        }
        other => panic!("expected init error, got {other:?}"),
    }
    assert!(
        s.next_significant().await.is_none(),
        "connection must close"
    );
    assert_eq!(s.wait_exit().await.code(), Some(3));
}

#[tokio::test]
async fn user_exit_before_ready_is_init_error() {
    // The user process is told to fail init; the bridge maps a plain exit to
    // InitError as well. Use a non-existent entrypoint via a bogus payload
    // path: simplest is an entrypoint that exits immediately.
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("bridge.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let mut child = Command::new(bridge_bin())
        .args(["--transport", "unix", "--unix-path"])
        .arg(&sock)
        .args([
            "--environment-id",
            ENV_ID,
            "--runtime-api-addr",
            "127.0.0.1:0",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (stream, _) = timeout(T, listener.accept()).await.unwrap().unwrap();
    let mut host = Framed::new(stream, FrameCodec);
    let frame = timeout(T, host.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(
        decode_message::<GuestMessage>(&frame).unwrap(),
        GuestMessage::Hello { .. }
    ));
    host.send(
        encode_message(&HostMessage::HelloAck {
            environment_id: ENV_ID.into(),
            epoch: 1,
            entrypoint: "/bin/sh".into(),
            args: vec!["-c".into(), "echo booting; exit 7".into()],
            env: vec![],
            working_dir: dir.path().to_string_lossy().into_owned(),
            init_timeout_ms: 15_000,
            max_response_bytes: 1 << 20,
            max_log_line_bytes: 4096,
        })
        .unwrap(),
    )
    .await
    .unwrap();
    let mut saw_log = false;
    loop {
        let frame = timeout(T, host.next())
            .await
            .unwrap()
            .expect("closed early")
            .unwrap();
        match decode_message::<GuestMessage>(&frame).unwrap() {
            GuestMessage::Log { line, phase, .. } if line == "booting" => {
                assert_eq!(phase, LogPhase::Init);
                saw_log = true;
            }
            GuestMessage::Log { .. } | GuestMessage::Heartbeat { .. } => {}
            GuestMessage::InitError {
                error_type,
                exit_code,
                ..
            } => {
                assert_eq!(error_type, "Runtime.InitExit");
                assert_eq!(exit_code, Some(7));
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(
        saw_log,
        "init-phase stdout must be forwarded before InitError"
    );
    assert_eq!(
        timeout(T, child.wait()).await.unwrap().unwrap().code(),
        Some(3)
    );
}

#[tokio::test]
async fn hello_reject_exits_with_2() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("bridge.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let mut child = Command::new(bridge_bin())
        .args(["--transport", "unix", "--unix-path"])
        .arg(&sock)
        .args([
            "--environment-id",
            ENV_ID,
            "--runtime-api-addr",
            "127.0.0.1:0",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (stream, _) = timeout(T, listener.accept()).await.unwrap().unwrap();
    let mut host = Framed::new(stream, FrameCodec);
    let _hello = timeout(T, host.next()).await.unwrap().unwrap().unwrap();
    host.send(
        encode_message(&HostMessage::HelloReject {
            reason: "unknown environment".into(),
        })
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        timeout(T, child.wait()).await.unwrap().unwrap().code(),
        Some(2)
    );
}

#[tokio::test]
async fn cancel_kills_after_grace() {
    let mut s = Session::start(vec![], 1 << 20).await;
    s.wait_ready().await;
    s.invoke("att_slow", serde_json::json!({"sleep_ms": 5000}))
        .await;
    // Give the user process time to pick the event up.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pid = s.user_pid().expect("pid log line");
    s.send(HostMessage::Cancel {
        attempt_id: "att_slow".into(),
        grace_ms: 200,
    })
    .await;
    let started = std::time::Instant::now();
    s.drain().await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "cancel must not wait for the handler"
    );
    let crash = s.all.iter().find_map(|m| match m {
        GuestMessage::Error {
            attempt_id, error, ..
        } if attempt_id == "att_slow" => Some(error.clone()),
        _ => None,
    });
    match crash {
        Some(GuestErrorKind::Crash { signal, .. }) => assert_eq!(signal, Some(libc::SIGKILL)),
        other => panic!("expected crash error for the cancelled attempt, got {other:?}"),
    }
    assert!(
        s.all
            .iter()
            .any(|m| matches!(m, GuestMessage::Exited { .. }))
    );
    assert_eq!(s.wait_exit().await.code(), Some(0));
    assert_pid_gone(pid).await;
}

#[tokio::test]
async fn second_invoke_while_busy_is_protocol_error() {
    let mut s = Session::start(vec![], 1 << 20).await;
    s.wait_ready().await;
    s.invoke("att_first", serde_json::json!({"sleep_ms": 800}))
        .await;
    s.invoke("att_second", serde_json::json!({"x": 1})).await;
    match s.next_significant().await {
        Some(GuestMessage::Error {
            attempt_id, error, ..
        }) => {
            assert_eq!(attempt_id, "att_second");
            assert_eq!(error, GuestErrorKind::Protocol);
        }
        other => panic!("expected protocol error, got {other:?}"),
    }
    match s.next_significant().await {
        Some(GuestMessage::Response { attempt_id, .. }) => assert_eq!(attempt_id, "att_first"),
        other => panic!("expected response for the first attempt, got {other:?}"),
    }
    // The environment is free again.
    s.invoke("att_third", serde_json::json!({"y": 2})).await;
    match s.next_significant().await {
        Some(GuestMessage::Response {
            attempt_id,
            payload,
            ..
        }) => {
            assert_eq!(attempt_id, "att_third");
            assert_eq!(payload["echo"]["y"], 2);
        }
        other => panic!("expected response, got {other:?}"),
    }
    s.send(HostMessage::Shutdown {
        reason: "done".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
}

#[tokio::test]
async fn shutdown_leaves_no_child() {
    let mut s = Session::start(vec![], 1 << 20).await;
    s.wait_ready().await;
    // Make sure the pid line has been forwarded before shutting down.
    let mut pid = s.user_pid();
    while pid.is_none() {
        assert!(s.recv().await.is_some());
        pid = s.user_pid();
    }
    let pid = pid.unwrap();
    assert!(pid_alive(pid));
    let started = std::time::Instant::now();
    s.send(HostMessage::Shutdown {
        reason: "idle".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
    // The SDK exits on SIGTERM while idle, so this is fast (< 2 s grace).
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_pid_gone(pid).await;
}

#[tokio::test]
async fn oversized_response_is_reported() {
    let mut s = Session::start(vec![], 64).await;
    s.wait_ready().await;
    s.invoke("att_big", serde_json::json!({"blob": "x".repeat(200)}))
        .await;
    match s.next_significant().await {
        Some(GuestMessage::Error {
            attempt_id,
            error,
            error_type,
            ..
        }) => {
            assert_eq!(attempt_id, "att_big");
            assert!(matches!(
                error,
                GuestErrorKind::ResponseTooLarge { max_bytes: 64, size_bytes } if size_bytes > 64
            ));
            assert_eq!(error_type, "Runtime.ResponseTooLarge");
        }
        other => panic!("expected response_too_large, got {other:?}"),
    }
    // Small responses still work afterwards.
    s.invoke("att_small", serde_json::json!(1)).await;
    match s.next_significant().await {
        Some(GuestMessage::Response {
            attempt_id,
            payload,
            ..
        }) => {
            assert_eq!(attempt_id, "att_small");
            assert_eq!(payload["echo"], 1);
        }
        other => panic!("expected response, got {other:?}"),
    }
    s.send(HostMessage::Shutdown {
        reason: "done".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
}

#[tokio::test]
async fn hello_ack_env_never_reaches_bridge_logs() {
    const MARKER: &str = "tsls-secret-marker-5d1c0e9a";
    let mut launch = Launch::selftest(vec![("DEMO_SECRET".into(), MARKER.into())], 1 << 20);
    launch.capture_stderr = true;
    let mut s = Session::start_with(launch).await;
    s.wait_ready().await;
    s.invoke("att_secret", serde_json::json!({"name": "tachyon"}))
        .await;
    match s.next_significant().await {
        Some(GuestMessage::Response { attempt_id, .. }) => assert_eq!(attempt_id, "att_secret"),
        other => panic!("expected response, got {other:?}"),
    }
    s.send(HostMessage::Shutdown {
        reason: "done".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));

    let stderr = s.stderr_text().await;
    // The capture is real (otherwise absence would prove nothing).
    assert!(stderr.contains("handshake complete"), "{stderr}");
    assert!(stderr.contains("env_vars=1"), "{stderr}");
    assert!(
        !stderr.contains(MARKER),
        "a HelloAck env value leaked into the bridge log:\n{stderr}"
    );
    assert!(
        s.all.iter().all(|m| !format!("{m:?}").contains(MARKER)),
        "a HelloAck env value leaked into a frame sent to the host"
    );
}

#[tokio::test]
async fn response_larger_than_a_frame_is_reported_not_silenced() {
    // A host limit above what one frame can carry (e.g. a misconfigured
    // gateway): the bridge must still answer instead of going silent.
    let mut s = Session::start(vec![], u64::MAX).await;
    s.wait_ready().await;
    s.invoke(
        "att_huge",
        serde_json::json!({"blob_bytes": MAX_FRAME_BYTES + 1024}),
    )
    .await;
    match s.next_significant().await {
        Some(GuestMessage::Error {
            attempt_id,
            error,
            error_type,
            ..
        }) => {
            assert_eq!(attempt_id, "att_huge");
            assert_eq!(error_type, "Runtime.ResponseTooLarge");
            assert!(
                matches!(
                    error,
                    GuestErrorKind::ResponseTooLarge { size_bytes, max_bytes }
                        if max_bytes == MAX_RESPONSE_PAYLOAD_BYTES && size_bytes > max_bytes
                ),
                "{error:?}"
            );
        }
        other => panic!("expected response_too_large, got {other:?}"),
    }
    // The user process got its 413 and keeps serving; frames keep flowing.
    s.invoke("att_after", serde_json::json!(2)).await;
    match s.next_significant().await {
        Some(GuestMessage::Response {
            attempt_id,
            payload,
            ..
        }) => {
            assert_eq!(attempt_id, "att_after");
            assert_eq!(payload["echo"], 2);
        }
        other => panic!("expected response, got {other:?}"),
    }
    s.send(HostMessage::Shutdown {
        reason: "done".into(),
    })
    .await;
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
}

#[tokio::test]
async fn bridge_sigterm_during_shutdown_kills_a_sigterm_ignoring_user_at_once() {
    // A user process that ignores SIGTERM and never exits on its own.
    let mut s = Session::start_with(Launch {
        entrypoint: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "trap '' TERM; echo selftest pid=$$; while :; do sleep 1; done".into(),
        ],
        env: vec![("PATH".into(), "/bin:/usr/bin".into())],
        max_response_bytes: 1 << 20,
        capture_stderr: false,
    })
    .await;
    let pid = s.wait_user_pid().await;
    assert!(pid_alive(pid));

    s.send(HostMessage::Shutdown {
        reason: "completed".into(),
    })
    .await;
    // Let the bridge start its 2 s SIGTERM grace, which the user ignores.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(pid_alive(pid), "the user process must ignore SIGTERM");

    // The provider terminates the bridge (its SIGKILL follows later).
    let bridge_pid = s.child.id().expect("bridge running");
    // SAFETY: plain signal to the bridge process this test spawned.
    unsafe {
        libc::kill(bridge_pid as libc::pid_t, libc::SIGTERM);
    }
    let signalled = std::time::Instant::now();
    while pid_alive(pid) {
        assert!(
            signalled.elapsed() < Duration::from_secs(1),
            "user process {pid} outlived the bridge's SIGTERM by more than 1 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    s.drain().await;
    assert_eq!(s.wait_exit().await.code(), Some(0));
}

#[tokio::test]
async fn host_disconnect_kills_user_and_exits_4() {
    let mut s = Session::start(vec![], 1 << 20).await;
    s.wait_ready().await;
    let mut pid = s.user_pid();
    while pid.is_none() {
        assert!(s.recv().await.is_some());
        pid = s.user_pid();
    }
    let pid = pid.unwrap();
    drop(s.host);
    assert_eq!(
        timeout(T, s.child.wait()).await.unwrap().unwrap().code(),
        Some(4)
    );
    assert_pid_gone(pid).await;
}
