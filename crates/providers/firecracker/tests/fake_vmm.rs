//! Lifecycle tests against a *fake* Firecracker.
//!
//! No KVM is needed: the provider is pointed at a shell wrapper that re-executes
//! this very test binary with `TACHYON_FAKE_FC=1`. In that mode
//! [`fake_firecracker_entry`] behaves like Firecracker as seen from the host:
//!
//! - prints to stdout (the provider redirects it to `console.log`),
//! - serves the API on `--api-sock`, records every call to `<env>/api.jsonl`
//!   and answers 204 (or 400 in `fail-api` mode),
//! - after `InstanceStart` connects back to `<uds_path>_<port>` (the port is
//!   parsed from the recorded kernel `boot_args`, exactly like the guest would)
//!   and sends a `Hello` frame, then powers off (exit 0) on `Shutdown`.
//!
//! `mkfs.ext4` is replaced by a script that records its arguments.

use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;

use tachyon_serverless_domain::{
    Architecture, EgressProfile, EnvironmentId, ResourceProfile, RevisionId, TenantId,
};
use tachyon_serverless_protocol::{
    FrameCodec, GuestMessage, HostMessage, decode_message, encode_message,
};
use tachyon_serverless_provider_firecracker::provider::ARCHIVE_DIR;
use tachyon_serverless_provider_firecracker::vmm::{EnvPaths, pid_alive};
use tachyon_serverless_provider_firecracker::{
    FirecrackerConfig, FirecrackerProvider, artifact_location_for,
};
use tachyon_serverless_provider_port::{
    EnvironmentObservation, EnvironmentSpec, ExecutionProvider, ProviderError, TerminateReason,
};

// ---------------------------------------------------------------------------
// fake Firecracker (runs in a re-executed copy of this test binary)
// ---------------------------------------------------------------------------

mod fake {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};

    fn arg_value(args: &[String], name: &str) -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1).cloned())
    }

    fn read_request(stream: &mut UnixStream) -> (String, String, serde_json::Value) {
        let mut reader = BufReader::new(stream);
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap() == 0 {
                break;
            }
            if line == "\r\n" {
                break;
            }
            head.push_str(&line);
        }
        let mut lines = head.lines();
        let status = lines.next().unwrap_or_default();
        let mut parts = status.split(' ');
        let method = parts.next().unwrap_or_default().to_owned();
        let path = parts.next().unwrap_or_default().to_owned();
        let cl: usize = lines
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; cl];
        reader.read_exact(&mut body).unwrap();
        let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
        (method, path, json)
    }

    fn write_frame(s: &mut UnixStream, bytes: &[u8]) {
        s.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
        s.write_all(bytes).unwrap();
        s.flush().unwrap();
    }

    fn read_frame(s: &mut UnixStream) -> Option<Vec<u8>> {
        let mut len = [0u8; 4];
        s.read_exact(&mut len).ok()?;
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        s.read_exact(&mut body).ok()?;
        Some(body)
    }

    fn cmdline_value(boot_args: &str, key: &str) -> Option<String> {
        boot_args
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix(key).and_then(|v| v.strip_prefix('=')))
            .map(str::to_owned)
    }

    /// The guest side: connect to the host listener and speak the bridge protocol.
    fn guest(boot_args: String, uds_path: String) {
        let env_id = cmdline_value(&boot_args, "tachyon.env_id").expect("env id in cmdline");
        let port = cmdline_value(&boot_args, "tachyon.vsock_port").expect("port in cmdline");
        assert_eq!(
            cmdline_value(&boot_args, "tachyon.function_dev").as_deref(),
            Some("/dev/vdb")
        );
        assert_eq!(
            cmdline_value(&boot_args, "init").as_deref(),
            Some("/sbin/tachyon-init")
        );
        std::thread::sleep(std::time::Duration::from_millis(30));
        let mut s = UnixStream::connect(format!("{uds_path}_{port}")).expect("host listener");
        println!("fake console: guest connected to host port {port}");
        let hello = tachyon_serverless_protocol::GuestMessage::Hello {
            protocol_version: tachyon_serverless_protocol::PROTOCOL_VERSION,
            bridge_version: "fake-bridge".into(),
            environment_id: env_id,
            guest_boot_id: Some("00000000-fake-boot-id".into()),
            architecture: std::env::consts::ARCH.into(),
        };
        write_frame(
            &mut s,
            &tachyon_serverless_protocol::encode_message(&hello).unwrap(),
        );
        while let Some(frame) = read_frame(&mut s) {
            let msg: tachyon_serverless_protocol::HostMessage =
                tachyon_serverless_protocol::decode_message(&frame).unwrap();
            if let tachyon_serverless_protocol::HostMessage::Shutdown { reason } = msg {
                println!("fake console: shutdown ({reason}); reboot: power off");
                std::process::exit(0);
            }
        }
        println!("fake console: host closed the connection; reboot: power off");
        std::process::exit(0);
    }

    pub fn run(args: Vec<String>, mode: &str) -> ! {
        if args.iter().any(|a| a == "--version") {
            println!("Firecracker v9.9.9-fake");
            std::process::exit(0);
        }
        let api_sock = PathBuf::from(arg_value(&args, "--api-sock").expect("--api-sock"));
        let id = arg_value(&args, "--id").expect("--id");
        let log_path = PathBuf::from(arg_value(&args, "--log-path").expect("--log-path"));
        assert_eq!(arg_value(&args, "--level").as_deref(), Some("Warning"));
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "invalid instance id {id}"
        );
        assert!(log_path.exists(), "--log-path must exist before start");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(b"fake fc.log: logger initialised\n")
            .unwrap();
        println!("fake console: firecracker starting id={id}");
        // Safety net against orphans if a test fails half-way.
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(60));
            std::process::exit(9);
        });
        let dir = api_sock.parent().map(Path::to_path_buf).unwrap();
        let listener = UnixListener::bind(&api_sock).expect("bind api sock");
        if mode == "exit-early" {
            println!("fake console: kernel panic - not syncing (simulated)");
            std::process::exit(1);
        }
        let mut record = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("api.jsonl"))
            .unwrap();
        let mut boot_args = String::new();
        let mut uds_path = String::new();
        for conn in listener.incoming() {
            let mut conn = conn.unwrap();
            let (method, path, body) = read_request(&mut conn);
            let mut entry = serde_json::json!({"method": method, "path": path, "body": body});
            if path == "/actions" {
                let port = cmdline_value(&boot_args, "tachyon.vsock_port").unwrap_or_default();
                entry["listener_present_at_start"] =
                    serde_json::Value::Bool(Path::new(&format!("{uds_path}_{port}")).exists());
            }
            writeln!(record, "{entry}").unwrap();
            if mode == "fail-api" && path == "/boot-source" {
                let fault = r#"{"fault_message":"fake: The kernel file cannot be opened"}"#;
                write!(
                    conn,
                    "HTTP/1.1 400 Bad Request\r\nServer: Firecracker API\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    fault.len(),
                    fault
                )
                .unwrap();
                continue;
            }
            conn.write_all(
                b"HTTP/1.1 204 No Content\r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n",
            )
            .unwrap();
            match path.as_str() {
                "/boot-source" => {
                    boot_args = body["boot_args"].as_str().unwrap_or_default().to_owned();
                }
                "/vsock" => {
                    uds_path = body["uds_path"].as_str().unwrap_or_default().to_owned();
                }
                "/actions" if body["action_type"] == "InstanceStart" && mode != "no-connect" => {
                    let (b, u) = (boot_args.clone(), uds_path.clone());
                    std::thread::spawn(move || guest(b, u));
                }
                _ => {}
            }
        }
        std::process::exit(0);
    }
}

/// Entry point of the fake Firecracker. A no-op in a normal test run.
#[test]
fn fake_firecracker_entry() {
    let Some(_) = std::env::var_os("TACHYON_FAKE_FC") else {
        return;
    };
    let args: Vec<String> = std::env::var("TACHYON_FAKE_FC_ARGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let mode = std::env::var("TACHYON_FAKE_FC_MODE").unwrap_or_else(|_| "normal".into());
    fake::run(args, &mode);
}

// ---------------------------------------------------------------------------
// host-side fixtures
// ---------------------------------------------------------------------------

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    provider: FirecrackerProvider,
    artifact: PathBuf,
}

fn write_exec(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn fixture(mode: &str) -> Fixture {
    // Unix socket paths are limited to ~104 bytes; keep the tree short.
    let dir = if Path::new("/tmp").is_dir() {
        tempfile::Builder::new()
            .prefix("fc")
            .tempdir_in("/tmp")
            .unwrap()
    } else {
        tempfile::tempdir().unwrap()
    };
    let root = dir.path().to_path_buf();
    let exe = std::env::current_exe().unwrap();
    let fc = root.join("firecracker.sh");
    write_exec(
        &fc,
        &format!(
            "#!/bin/sh\nexec env TACHYON_FAKE_FC=1 TACHYON_FAKE_FC_MODE={mode} TACHYON_FAKE_FC_ARGS=\"$*\" '{}' fake_firecracker_entry --exact --nocapture --test-threads=1\n",
            exe.display()
        ),
    );
    let mkfs = root.join("mkfs.ext4");
    write_exec(
        &mkfs,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$(dirname \"$5\")/mkfs.args\"\nexit 0\n",
    );
    std::fs::write(root.join("vmlinux"), b"not really a kernel").unwrap();
    std::fs::write(root.join("rootfs.ext4"), b"not really a rootfs").unwrap();
    let artifact = root.join("app.bin");
    std::fs::write(&artifact, elf_for_host()).unwrap();
    let provider = FirecrackerProvider::new(FirecrackerConfig {
        firecracker_binary: fc,
        kernel: root.join("vmlinux"),
        rootfs: root.join("rootfs.ext4"),
        workdir: root.join("run"),
        vsock_port: 5000,
        mkfs_ext4: mkfs,
        kill_grace: Duration::from_secs(2),
        boot_args_extra: Some("loglevel=8".into()),
    });
    Fixture {
        _dir: dir,
        root,
        provider,
        artifact,
    }
}

/// A minimal static ELF64 for the host architecture (header only).
fn elf_for_host() -> Vec<u8> {
    let machine: u16 = match Architecture::host().unwrap() {
        Architecture::X86_64 => 0x3E,
        Architecture::Aarch64 => 0xB7,
    };
    let mut v = vec![0u8; 64 + 56];
    v[..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
    v[4] = 2;
    v[5] = 1;
    v[6] = 1;
    v[16..18].copy_from_slice(&2u16.to_le_bytes());
    v[18..20].copy_from_slice(&machine.to_le_bytes());
    v[32..40].copy_from_slice(&64u64.to_le_bytes());
    v[54..56].copy_from_slice(&56u16.to_le_bytes());
    v[56..58].copy_from_slice(&1u16.to_le_bytes());
    v[64..68].copy_from_slice(&1u32.to_le_bytes()); // PT_LOAD
    v
}

fn spec(f: &Fixture, connect_timeout: Duration) -> EnvironmentSpec {
    EnvironmentSpec {
        environment_id: EnvironmentId::generate(),
        tenant_id: TenantId::generate(),
        revision_id: RevisionId::generate(),
        artifact: artifact_location_for(&f.artifact).unwrap(),
        architecture: Architecture::host().unwrap(),
        resources: ResourceProfile {
            memory_mib: 512,
            cpu_millis: 1500,
            ephemeral_storage_mib: 128,
        },
        egress: EgressProfile::None,
        connect_timeout,
    }
}

fn read_api_log(env_dir: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(env_dir.join("api.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_lifecycle_with_fake_vmm() {
    let f = fixture("normal");
    let p = &f.provider;
    assert_eq!(p.firecracker_version(), Some("v9.9.9-fake"));

    let spec = spec(&f, Duration::from_secs(10));
    let env_id = spec.environment_id.clone();
    let paths = EnvPaths::new(&f.root.join("run"), env_id.as_str(), 5000);
    let artifact_len = std::fs::metadata(&f.artifact).unwrap().len();

    let handle = p.create_environment(spec).await.expect("create");
    assert_eq!(handle.environment_id, env_id);
    let pid = handle.evidence.host_pid.expect("host pid");
    assert!(pid_alive(pid));
    let d = &handle.evidence.details;
    assert_eq!(d["provider"], "firecracker");
    assert_eq!(d["firecracker_version"], "v9.9.9-fake");
    assert_eq!(d["vcpus"], 2);
    assert_eq!(d["mem_mib"], 512);
    assert_eq!(d["vsock_port"], 5000);
    assert_eq!(d["function_drive_bytes"], 9 * 1024 * 1024);
    assert_eq!(d["instance_id"], env_id.as_str().replace('_', "-"));
    assert!(d["kernel_sha256"].as_str().unwrap().len() == 64);
    assert!(d["rootfs_sha256"].as_str().unwrap().len() == 64);
    assert!(handle.connected_at >= handle.created_at);

    // Host-side artefacts.
    assert_eq!(
        std::fs::metadata(&paths.function_drive).unwrap().len(),
        9 * 1024 * 1024
    );
    let mode = std::fs::metadata(&paths.stage_app)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o755);
    assert_eq!(
        std::fs::metadata(&paths.stage_app).unwrap().len(),
        artifact_len
    );
    let mkfs_args = std::fs::read_to_string(paths.dir.join("mkfs.args")).unwrap();
    let mkfs_args: Vec<&str> = mkfs_args.lines().collect();
    assert_eq!(
        mkfs_args,
        vec![
            "-q",
            "-F",
            "-d",
            paths.stage.to_str().unwrap(),
            paths.function_drive.to_str().unwrap(),
            "9M"
        ]
    );
    assert_eq!(
        std::fs::read_to_string(&paths.pid_file).unwrap().trim(),
        pid.to_string()
    );

    // API sequence exactly as docs/protocol.md §C.
    let calls = read_api_log(&paths.dir);
    let seq: Vec<&str> = calls.iter().map(|c| c["path"].as_str().unwrap()).collect();
    assert_eq!(
        seq,
        vec![
            "/machine-config",
            "/boot-source",
            "/drives/rootfs",
            "/drives/function",
            "/vsock",
            "/actions"
        ]
    );
    assert!(calls.iter().all(|c| c["method"] == "PUT"));
    assert_eq!(
        calls[0]["body"],
        serde_json::json!({"vcpu_count": 2, "mem_size_mib": 512, "smt": false})
    );
    assert_eq!(
        calls[1]["body"]["kernel_image_path"],
        f.root.join("vmlinux").to_str().unwrap()
    );
    let boot_args = calls[1]["body"]["boot_args"].as_str().unwrap();
    assert!(
        boot_args.starts_with("console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init ")
    );
    assert!(boot_args.contains(&format!(
        "tachyon.env_id={env_id} tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb"
    )));
    assert!(boot_args.ends_with("loglevel=8"));
    assert_eq!(
        boot_args.contains("keep_bootcon"),
        Architecture::host() == Some(Architecture::Aarch64)
    );
    assert_eq!(
        calls[2]["body"],
        serde_json::json!({"drive_id": "rootfs", "path_on_host": f.root.join("rootfs.ext4"), "is_root_device": true, "is_read_only": true})
    );
    assert_eq!(
        calls[3]["body"],
        serde_json::json!({"drive_id": "function", "path_on_host": paths.function_drive, "is_root_device": false, "is_read_only": true})
    );
    assert_eq!(
        calls[4]["body"],
        serde_json::json!({"guest_cid": 3, "uds_path": paths.vsock_uds})
    );
    assert_eq!(
        calls[5]["body"],
        serde_json::json!({"action_type": "InstanceStart"})
    );
    assert_eq!(
        calls[5]["listener_present_at_start"], true,
        "host must listen before InstanceStart"
    );

    // The accepted stream carries the bridge Hello without any CONNECT handshake.
    let mut bridge = Framed::new(handle.stream, FrameCodec);
    let frame = tokio::time::timeout(Duration::from_secs(5), bridge.next())
        .await
        .expect("hello in time")
        .expect("stream open")
        .expect("frame");
    let hello: GuestMessage = decode_message(&frame).unwrap();
    match hello {
        GuestMessage::Hello {
            environment_id,
            guest_boot_id,
            ..
        } => {
            assert_eq!(environment_id, env_id.as_str());
            assert_eq!(guest_boot_id.as_deref(), Some("00000000-fake-boot-id"));
        }
        other => panic!("expected Hello, got {other:?}"),
    }

    assert_eq!(
        p.observe_environment(&env_id).await.unwrap(),
        EnvironmentObservation::Running {
            host_pid: Some(pid)
        }
    );
    assert_eq!(p.list_environments().await.unwrap(), vec![env_id.clone()]);

    // Shutdown -> the fake guest powers off -> Firecracker exits 0.
    bridge
        .send(
            encode_message(&HostMessage::Shutdown {
                reason: "test".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let eof = tokio::time::timeout(Duration::from_secs(5), bridge.next()).await;
    assert!(
        matches!(eof, Ok(None)),
        "expected EOF after poweroff, got {eof:?}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        p.observe_environment(&env_id).await.unwrap(),
        EnvironmentObservation::Exited {
            exit_code: Some(0),
            signal: None
        }
    );

    let report = p
        .terminate_environment(&env_id, TerminateReason::Completed)
        .await
        .unwrap();
    assert!(!report.was_running, "VM had already powered off");
    assert!(
        report
            .cleaned
            .iter()
            .any(|c| c == &format!("process-group:{pid}"))
    );
    for expect in [&paths.function_drive, &paths.stage, &paths.dir] {
        assert!(
            report
                .cleaned
                .iter()
                .any(|c| c == &expect.display().to_string()),
            "{} missing from cleaned: {:?}",
            expect.display(),
            report.cleaned
        );
    }
    assert!(!paths.dir.exists());
    assert!(!pid_alive(pid));
    let archive = f.root.join("run").join(ARCHIVE_DIR).join(env_id.as_str());
    let console = std::fs::read_to_string(archive.join("console.log")).unwrap();
    assert!(
        console.contains("fake console: firecracker starting"),
        "{console}"
    );
    assert!(console.contains("power off"), "{console}");
    assert!(
        std::fs::read_to_string(archive.join("fc.log"))
            .unwrap()
            .contains("logger initialised")
    );
    assert_eq!(
        p.observe_environment(&env_id).await.unwrap(),
        EnvironmentObservation::NotFound
    );
    assert!(p.list_environments().await.unwrap().is_empty());
    // Idempotent.
    let again = p
        .terminate_environment(&env_id, TerminateReason::Completed)
        .await
        .unwrap();
    assert!(!again.was_running);
    assert!(again.cleaned.is_empty());
}

#[tokio::test]
async fn terminate_kills_a_running_vm_and_reports_cleanup() {
    let f = fixture("normal");
    let p = &f.provider;
    let first = spec(&f, Duration::from_secs(10));
    let env_id = first.environment_id.clone();
    let paths = EnvPaths::new(&f.root.join("run"), env_id.as_str(), 5000);
    let handle = p.create_environment(first).await.expect("create");
    let pid = handle.evidence.host_pid.unwrap();
    // Duplicate ids are refused while the environment exists.
    let dup = EnvironmentSpec {
        environment_id: env_id.clone(),
        ..spec(&f, Duration::from_secs(1))
    };
    assert!(matches!(
        p.create_environment(dup).await,
        Err(ProviderError::InvalidSpec(_))
    ));

    // Keep the bridge stream open: the VM is still running when we time out.
    let report = p
        .terminate_environment(&env_id, TerminateReason::Timeout)
        .await
        .unwrap();
    assert!(report.was_running);
    assert!(!pid_alive(pid), "process group must be killed");
    assert!(!paths.dir.exists());
    assert!(!paths.vsock_listener.exists());
    assert!(!paths.api_sock.exists());
    assert_eq!(
        p.observe_environment(&env_id).await.unwrap(),
        EnvironmentObservation::NotFound
    );
    drop(handle);
}

#[tokio::test]
async fn boot_error_when_vmm_exits_early_includes_console_tail() {
    let f = fixture("exit-early");
    let spec = spec(&f, Duration::from_secs(5));
    let env_id = spec.environment_id.clone();
    let err = f.provider.create_environment(spec).await.unwrap_err();
    let ProviderError::Boot(msg) = &err else {
        panic!("expected Boot, got {err}");
    };
    assert!(msg.contains("firecracker exited before"), "{msg}");
    assert!(msg.contains("console.log (tail)"), "{msg}");
    assert!(
        msg.contains("kernel panic - not syncing (simulated)"),
        "{msg}"
    );
    assert!(msg.contains("fake fc.log"), "{msg}");
    assert!(!f.root.join("run").join(env_id.as_str()).exists());
    assert!(
        f.root
            .join("run")
            .join(ARCHIVE_DIR)
            .join(env_id.as_str())
            .join("console.log")
            .exists()
    );
    assert!(f.provider.list_environments().await.unwrap().is_empty());
}

#[tokio::test]
async fn boot_error_on_api_400_kills_vmm_and_cleans_up() {
    let f = fixture("fail-api");
    let spec = spec(&f, Duration::from_secs(5));
    let env_id = spec.environment_id.clone();
    let err = f.provider.create_environment(spec).await.unwrap_err();
    let ProviderError::Boot(msg) = &err else {
        panic!("expected Boot, got {err}");
    };
    assert!(msg.contains("PUT /boot-source -> 400"), "{msg}");
    assert!(msg.contains("kernel file cannot be opened"), "{msg}");
    assert!(!f.root.join("run").join(env_id.as_str()).exists());
    assert_eq!(
        f.provider.observe_environment(&env_id).await.unwrap(),
        EnvironmentObservation::NotFound
    );
}

#[tokio::test]
async fn boot_times_out_when_guest_never_connects() {
    let f = fixture("no-connect");
    let spec = spec(&f, Duration::from_millis(700));
    let env_id = spec.environment_id.clone();
    let started = std::time::Instant::now();
    let err = f.provider.create_environment(spec).await.unwrap_err();
    let ProviderError::Boot(msg) = &err else {
        panic!("expected Boot, got {err}");
    };
    assert!(msg.contains("timeout waiting for guest bridge"), "{msg}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!f.root.join("run").join(env_id.as_str()).exists());
    assert!(f.provider.list_environments().await.unwrap().is_empty());
}

#[tokio::test]
async fn validate_artifact_accepts_fixture_binary() {
    let f = fixture("normal");
    let loc = artifact_location_for(&f.artifact).unwrap();
    f.provider
        .validate_artifact(&loc, Architecture::host().unwrap())
        .await
        .unwrap();
    // Sanity: the fixture file really is what we wrote.
    let mut buf = Vec::new();
    std::fs::File::open(&f.artifact)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    assert_eq!(&buf[..4], b"\x7fELF");
}
