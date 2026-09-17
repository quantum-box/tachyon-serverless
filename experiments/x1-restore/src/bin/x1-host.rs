//! `x1-host` (PLT-4652, experimental): host side of one microVM in the X1
//! snapshot / restore experiment. Listens on the vsock UDS (`<uds>_<port>`),
//! drives the handshake and the `x1_*` control frames, sends invokes, and
//! prints one JSON object per line to stdout (`t_ms` since start,
//! `host_wall_ms`, `event`, fields). Never logs `HelloAck.env`.
//!
//! Modes:
//! - `source`: handshake (no secret in the env), report `checkpoint_wait`
//!   when the process waits in `continue`, then keep logging until killed.
//! - `clone`: the first frame must be `x1_reconnect` (a restored copy). A
//!   `hello` means the guest cold-booted: exit 3 so it is never counted as a
//!   restore. Answers `restored` with `--instance-id`, waits for `ready`,
//!   invokes, shuts down.
//! - `cold`: baseline cold boot through the same lifecycle (answers `cold`).
//! - `bridge`: the product bridge (no pump): handshake, ready, one invoke,
//!   then log until the deadline, including whether anything reconnects.
//! - `listen`: accept and log whatever arrives until the deadline.
//!
//! Exit codes: 0 done, 1 failure / deadline in a mode that expects progress,
//! 3 cold boot seen where a restore was expected.

use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tachyon_serverless_protocol::runtime_api::event_types;
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, decode_message, encode_message,
};
use tachyon_serverless_x1_restore::control::{GuestControl, HostControl, is_control};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Source,
    Clone,
    Cold,
    Bridge,
    Listen,
}

#[derive(Debug, Parser)]
#[command(name = "x1-host", about = "X1 experiment host for one microVM")]
struct Args {
    /// UDS path to listen on (Firecracker: `<vsock uds_path>_<port>`).
    #[arg(long)]
    listen: PathBuf,
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value = "env_x1restore")]
    env_id: String,
    /// Identity handed to a restored copy (clone mode).
    #[arg(long)]
    instance_id: Option<String>,
    #[arg(long, default_value_t = 1)]
    generation: u64,
    /// JSON payload of each invoke.
    #[arg(long, default_value = "{\"n\":97}")]
    payload: String,
    #[arg(long, default_value_t = 2)]
    invokes: u32,
    /// Overall deadline of this host process.
    #[arg(long, default_value_t = 120)]
    deadline_secs: u64,
    #[arg(long, default_value = "/function/app")]
    entrypoint: String,
    /// Extra `KEY=VALUE` for the user process env (bridge / cold modes only;
    /// never used for a snapshot source).
    #[arg(long = "env")]
    env: Vec<String>,
    /// Clone / cold modes: stay connected this long after the invokes before
    /// the shutdown (so several restored VMMs are observable at once).
    #[arg(long, default_value_t = 0)]
    linger_ms: u64,
    /// Clone mode: after the restore, connect to this vsock UDS (the VMM's
    /// `uds_path`, not `<uds>_<port>`) and `CONNECT <doorbell-port>` so the
    /// guest learns about the restore at once.
    #[arg(long)]
    doorbell_uds: Option<PathBuf>,
    #[arg(long, default_value_t = 5001)]
    doorbell_port: u32,
}

type Reader = FramedRead<ReadHalf<UnixStream>, FrameCodec>;
type Writer = FramedWrite<WriteHalf<UnixStream>, FrameCodec>;

#[derive(Clone, Copy)]
struct Events {
    started: Instant,
}

impl Events {
    fn emit(&self, event: &str, fields: Value) {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut obj = json!({
            "t_ms": self.started.elapsed().as_millis() as u64,
            "host_wall_ms": wall,
            "event": event,
        });
        if let (Some(o), Value::Object(f)) = (obj.as_object_mut(), fields) {
            o.extend(f);
        }
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{obj}");
        let _ = out.flush();
    }
}

struct Conn {
    reader: Reader,
    writer: Writer,
}

struct Host {
    args: Args,
    ev: Events,
    deadline: tokio::time::Instant,
    listener: UnixListener,
    epoch: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let ev = Events {
        started: Instant::now(),
    };
    if args.listen.exists() {
        let _ = std::fs::remove_file(&args.listen);
    }
    let listener = match UnixListener::bind(&args.listen) {
        Ok(l) => l,
        Err(e) => {
            ev.emit(
                "error",
                json!({"message": format!("bind {}: {e}", args.listen.display())}),
            );
            return std::process::ExitCode::from(1);
        }
    };
    ev.emit(
        "listening",
        json!({"path": args.listen.display().to_string(), "mode": format!("{:?}", args.mode).to_lowercase()}),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.deadline_secs);
    let mut host = Host {
        args,
        ev,
        deadline,
        listener,
        epoch: 1,
    };
    let result = match host.args.mode {
        Mode::Source => host.source().await,
        Mode::Clone => host.clone_mode().await,
        Mode::Cold => host.cold().await,
        Mode::Bridge => host.bridge().await,
        Mode::Listen => host.listen().await,
    };
    let _ = std::fs::remove_file(&host.args.listen);
    match result {
        Ok(code) => {
            host.ev.emit("exit", json!({"code": code}));
            std::process::ExitCode::from(code)
        }
        Err(e) => {
            host.ev.emit("error", json!({"message": format!("{e:#}")}));
            std::process::ExitCode::from(1)
        }
    }
}

enum Next {
    Frame(bool),
    Accepted(UnixStream),
}

fn conn(s: UnixStream) -> Conn {
    let (r, w) = tokio::io::split(s);
    Conn {
        reader: FramedRead::new(r, FrameCodec),
        writer: FramedWrite::new(w, FrameCodec),
    }
}

/// Connect to the guest's doorbell port through the VMM's vsock UDS
/// (`CONNECT <port>\n` -> `OK <host port>\n`), retrying until the restored
/// VMM has created the socket and the guest listener answers.
async fn ring_doorbell(path: PathBuf, port: u32, ev: Events, deadline: tokio::time::Instant) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let mut attempts = 0u32;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        if let Ok(mut s) = UnixStream::connect(&path).await
            && s.write_all(format!("CONNECT {port}\n").as_bytes())
                .await
                .is_ok()
        {
            let mut line = String::new();
            let mut reader = BufReader::new(&mut s);
            let read =
                tokio::time::timeout(Duration::from_millis(500), reader.read_line(&mut line)).await;
            if matches!(read, Ok(Ok(n)) if n > 0) && line.starts_with("OK") {
                ev.emit(
                    "doorbell_ok",
                    json!({"attempts": attempts, "reply": line.trim()}),
                );
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    ev.emit("doorbell_gave_up", json!({"attempts": attempts}));
}

/// What a frame from the guest was.
enum Frame {
    Protocol(GuestMessage),
    Control(GuestControl),
}

impl Host {
    async fn accept(&self) -> anyhow::Result<Conn> {
        let (s, _) = tokio::time::timeout_at(self.deadline, self.listener.accept())
            .await
            .map_err(|_| anyhow!("deadline waiting for a connection"))?
            .context("accept")?;
        self.ev.emit("accepted", json!({}));
        Ok(conn(s))
    }

    /// Next frame, logged. `Ok(None)` = connection closed.
    async fn next(&self, c: &mut Conn) -> anyhow::Result<Option<Frame>> {
        let item = tokio::time::timeout_at(self.deadline, c.reader.next())
            .await
            .map_err(|_| anyhow!("deadline waiting for a frame"))?;
        let bytes: Bytes = match item {
            None => {
                self.ev.emit("connection_closed", json!({}));
                return Ok(None);
            }
            Some(Err(e)) => {
                self.ev
                    .emit("connection_error", json!({"message": e.to_string()}));
                return Ok(None);
            }
            Some(Ok(b)) => b,
        };
        if is_control(&bytes) {
            let c: GuestControl = serde_json::from_slice(&bytes).context("control frame")?;
            self.ev.emit("guest_control", serde_json::to_value(&c)?);
            return Ok(Some(Frame::Control(c)));
        }
        let m: GuestMessage = decode_message(&bytes).context("guest frame")?;
        match &m {
            GuestMessage::Heartbeat { .. } => {}
            GuestMessage::Log { line, phase, .. } => self
                .ev
                .emit("guest_log", json!({"phase": phase, "line": line})),
            other => self.ev.emit("guest_frame", serde_json::to_value(other)?),
        }
        Ok(Some(Frame::Protocol(m)))
    }

    async fn send_host(&self, c: &mut Conn, m: &HostMessage) -> anyhow::Result<()> {
        c.writer.send(encode_message(m)?).await.context("send")
    }

    async fn send_control(&self, c: &mut Conn, m: &HostControl) -> anyhow::Result<()> {
        self.ev.emit("host_control", serde_json::to_value(m)?);
        c.writer
            .send(Bytes::from(serde_json::to_vec(m)?))
            .await
            .context("send control")
    }

    async fn handshake(&mut self, c: &mut Conn, env: Vec<(String, String)>) -> anyhow::Result<()> {
        match self.next(c).await? {
            Some(Frame::Protocol(GuestMessage::Hello { .. })) => {}
            Some(_) => bail!("expected hello"),
            None => bail!("closed before hello"),
        }
        let ack = HostMessage::HelloAck {
            environment_id: self.args.env_id.clone(),
            epoch: self.epoch,
            entrypoint: self.args.entrypoint.clone(),
            args: vec![],
            env,
            working_dir: "/function".into(),
            // The source waits at the checkpoint for as long as the snapshot takes.
            init_timeout_ms: 600_000,
            max_response_bytes: 1 << 20,
            max_log_line_bytes: 8192,
            snapshot_hold: false,
        };
        self.send_host(c, &ack).await?;
        self.ev.emit("hello_ack_sent", json!({"env_vars": match &ack { HostMessage::HelloAck { env, .. } => env.len(), _ => 0 }}));
        Ok(())
    }

    fn extra_env(&self) -> Vec<(String, String)> {
        self.args
            .env
            .iter()
            .filter_map(|kv| {
                kv.split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect()
    }

    async fn wait_waiting(&self, c: &mut Conn) -> anyhow::Result<()> {
        loop {
            match self.next(c).await? {
                Some(Frame::Control(GuestControl::Waiting { .. })) => return Ok(()),
                Some(Frame::Protocol(GuestMessage::InitError { .. })) => bail!("init error"),
                Some(_) => {}
                None => bail!("closed before the checkpoint wait"),
            }
        }
    }

    async fn wait_ready(&self, c: &mut Conn) -> anyhow::Result<()> {
        loop {
            match self.next(c).await? {
                Some(Frame::Protocol(GuestMessage::Ready { .. })) => return Ok(()),
                Some(Frame::Protocol(GuestMessage::InitError { .. })) => bail!("init error"),
                Some(_) => {}
                None => bail!("closed before ready"),
            }
        }
    }

    async fn invoke_all(&self, c: &mut Conn) -> anyhow::Result<()> {
        let payload: Value = serde_json::from_str(&self.args.payload).context("--payload")?;
        for i in 0..self.args.invokes {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
            let attempt_id = format!("att_x1_{i}");
            let invoke = HostMessage::Invoke {
                invocation_id: format!("inv_x1_{i}"),
                attempt_id: attempt_id.clone(),
                epoch: self.epoch,
                event_type: event_types::JSON.into(),
                deadline_ms: now + 30_000,
                remaining_ms: 30_000,
                trace_id: format!("x1-{i}"),
                payload: payload.clone(),
            };
            self.ev
                .emit("invoke_sent", json!({"attempt_id": attempt_id}));
            self.send_host(c, &invoke).await?;
            loop {
                match self.next(c).await? {
                    Some(Frame::Protocol(GuestMessage::Response { .. })) => break,
                    Some(Frame::Protocol(GuestMessage::Error { .. })) => bail!("invoke failed"),
                    Some(_) => {}
                    None => bail!("closed during invoke"),
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self, c: &mut Conn) -> anyhow::Result<()> {
        if self.args.linger_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.args.linger_ms)).await;
        }
        self.send_host(
            c,
            &HostMessage::Shutdown {
                reason: "x1 done".into(),
            },
        )
        .await?;
        while self.next(c).await?.is_some() {}
        Ok(())
    }

    async fn source(&mut self) -> anyhow::Result<u8> {
        let mut c = self.accept().await?;
        self.handshake(&mut c, vec![]).await?;
        self.wait_waiting(&mut c).await?;
        self.ev.emit("checkpoint_wait", json!({}));
        // Log until killed or the deadline. A resumed source reconnects after
        // the snapshot reset its vsock connection, and the VMM does not always
        // close the old host-side connection, so a new connection supersedes
        // the current one instead of waiting for it to end.
        loop {
            let next = tokio::select! {
                r = self.next(&mut c) => Next::Frame(r?.is_some()),
                a = self.listener.accept() => Next::Accepted(a.context("accept")?.0),
            };
            match next {
                Next::Frame(true) => {}
                Next::Frame(false) => c = self.accept().await?,
                Next::Accepted(s) => {
                    self.ev
                        .emit("accepted", json!({"superseded_previous": true}));
                    c = conn(s);
                }
            }
        }
    }

    async fn clone_mode(&mut self) -> anyhow::Result<u8> {
        let instance_id = self
            .args
            .instance_id
            .clone()
            .ok_or_else(|| anyhow!("--instance-id is required in clone mode"))?;
        if let Some(path) = self.args.doorbell_uds.clone() {
            tokio::spawn(ring_doorbell(
                path,
                self.args.doorbell_port,
                self.ev,
                self.deadline,
            ));
        }
        let mut c = self.accept().await?;
        match self.next(&mut c).await? {
            Some(Frame::Control(GuestControl::Reconnect { .. })) => {
                self.ev.emit("restore_identified", json!({}));
            }
            Some(Frame::Protocol(GuestMessage::Hello { .. })) => {
                self.ev.emit(
                    "cold_boot_detected",
                    json!({"message": "a hello on a restored VMM means the guest booted; not a restore"}),
                );
                return Ok(3);
            }
            Some(_) => bail!("unexpected first frame"),
            None => bail!("closed before x1_reconnect"),
        }
        self.send_control(
            &mut c,
            &HostControl::Continue {
                restored: true,
                instance_id: Some(instance_id),
                generation: self.args.generation,
            },
        )
        .await?;
        self.wait_ready(&mut c).await?;
        self.invoke_all(&mut c).await?;
        self.shutdown(&mut c).await?;
        Ok(0)
    }

    async fn cold(&mut self) -> anyhow::Result<u8> {
        let mut c = self.accept().await?;
        let env = self.extra_env();
        self.handshake(&mut c, env).await?;
        self.wait_waiting(&mut c).await?;
        self.send_control(
            &mut c,
            &HostControl::Continue {
                restored: false,
                instance_id: None,
                generation: 0,
            },
        )
        .await?;
        self.wait_ready(&mut c).await?;
        self.invoke_all(&mut c).await?;
        self.shutdown(&mut c).await?;
        Ok(0)
    }

    async fn bridge(&mut self) -> anyhow::Result<u8> {
        let mut c = self.accept().await?;
        let env = self.extra_env();
        self.handshake(&mut c, env).await?;
        self.wait_ready(&mut c).await?;
        self.invoke_all(&mut c).await?;
        self.ev.emit("bridge_idle", json!({}));
        self.listen_on(Some(c)).await
    }

    async fn listen(&mut self) -> anyhow::Result<u8> {
        self.listen_on(None).await
    }

    async fn listen_on(&self, mut conn: Option<Conn>) -> anyhow::Result<u8> {
        loop {
            let mut c = match conn.take() {
                Some(c) => c,
                None => match self.accept().await {
                    Ok(c) => c,
                    Err(_) => {
                        self.ev.emit("deadline", json!({}));
                        return Ok(0);
                    }
                },
            };
            loop {
                match self.next(&mut c).await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => {
                        self.ev.emit("deadline", json!({}));
                        return Ok(0);
                    }
                }
            }
        }
    }
}
