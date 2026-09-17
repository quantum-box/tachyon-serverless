//! Restore link (X1, PLT-4653, feature `experimental-restore`).
//!
//! The session ([`crate::session`]) is unchanged: it talks to an in-memory
//! duplex, and this link pumps frames between that duplex and the host
//! connection. Until the host asks for a snapshot hold the link is a
//! transparent pipe and a lost connection ends the session exactly as before
//! (exit 4). With `HelloAck.snapshot_hold` (protocol version 3):
//!
//! 1. The doorbell is armed (a guest listen socket survives a VMM snapshot,
//!    connected sockets do not; docs/adr/0015 §「Firecracker で分かったこと」1).
//! 2. When the user process asks `continue`, the link reports
//!    [`GuestMessage::CheckpointWaiting`] and holds the answer.
//! 3. When the connection is lost (the snapshot reset the vsock device, or
//!    the doorbell rang on a restored copy), the link reconnects and says
//!    [`GuestMessage::Reconnect`] first, carrying the unchanged boot id.
//! 4. On [`HostMessage::Restore`] it sets the guest wall clock to the host's,
//!    then answers `continue` with `restored` and the host-assigned identity.
//!    From then on the link is transparent again.
//!
//! Frames are never altered. A frame whose write failed is resent once on the
//! next connection (heartbeats are dropped). At the checkpoint only heartbeats
//! and logs flow, which is why snapshots are taken only there.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tachyon_serverless_protocol::runtime_api::lifecycle::{self, Continuation};
use tachyon_serverless_protocol::{
    CHECKPOINT_PHASE, FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message,
    encode_message,
};
use tokio::io::DuplexStream;
use tokio::sync::{mpsc, watch};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info, warn};

use crate::process::now_ms;
use crate::runtime_api::RestoreSource;
use crate::transport::BoxedHostStream;

/// A signal within this long of a reconnect is treated as the same restore.
const RESET_GRACE: Duration = Duration::from_secs(2);

/// Future of one connection attempt.
pub type ConnectFuture = Pin<Box<dyn Future<Output = std::io::Result<BoxedHostStream>> + Send>>;
/// Makes a new host connection (one attempt).
pub type Connect = Box<dyn FnMut() -> ConnectFuture + Send>;
/// Arms the doorbell once a hold is requested; each message on the sender is
/// "the restore happened, drop the connection now".
pub type ArmDoorbell = Box<dyn FnOnce(mpsc::UnboundedSender<String>) + Send>;
/// Sets the guest wall clock (ms since the Unix epoch).
pub type SetClock = Box<dyn Fn(u64) -> std::io::Result<()> + Send + Sync>;

struct Shared {
    hold: AtomicBool,
    answer: watch::Sender<Option<Continuation>>,
    waiting: mpsc::UnboundedSender<()>,
}

/// The [`RestoreSource`] the session uses: `cold` at once unless the host
/// asked for a hold.
pub struct LinkRestore(Arc<Shared>);

impl RestoreSource for LinkRestore {
    fn continuation(&self) -> Pin<Box<dyn Future<Output = Continuation> + Send + '_>> {
        if !self.0.hold.load(Ordering::SeqCst) {
            return Box::pin(std::future::ready(Continuation::Cold));
        }
        let _ = self.0.waiting.send(());
        let mut rx = self.0.answer.subscribe();
        Box::pin(async move {
            loop {
                if let Some(c) = rx.borrow_and_update().clone() {
                    return c;
                }
                if rx.changed().await.is_err() {
                    // The link is gone and so is the session.
                    return Continuation::Cold;
                }
            }
        })
    }
}

pub struct LinkConfig {
    pub environment_id: String,
    pub guest_boot_id: Option<String>,
    /// How long a held guest keeps trying to reach the host again.
    pub reconnect_budget: Duration,
    pub reconnect_interval: Duration,
    pub arm_doorbell: Option<ArmDoorbell>,
    /// `None` leaves the clock alone (tests, the process provider).
    pub set_clock: Option<SetClock>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LinkOutcome {
    /// The session closed its side.
    SessionClosed,
    /// The host connection ended while no hold was active: the session ends
    /// as it always did.
    HostLost(String),
    /// A held guest could not reach the host again within the budget.
    GaveUp(String),
}

/// The link's handles: the restore source for the session and the task body.
pub struct Link {
    shared: Arc<Shared>,
    waiting: mpsc::UnboundedReceiver<()>,
}

pub fn link() -> (LinkRestore, Link) {
    let (answer, _) = watch::channel(None);
    let (wtx, wrx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        hold: AtomicBool::new(false),
        answer,
        waiting: wtx,
    });
    (
        LinkRestore(shared.clone()),
        Link {
            shared,
            waiting: wrx,
        },
    )
}

#[derive(Deserialize)]
struct TypeOnly {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    snapshot_hold: bool,
}

fn peek(frame: &[u8]) -> Option<TypeOnly> {
    serde_json::from_slice(frame).ok()
}

fn encode(m: &GuestMessage) -> Bytes {
    encode_message(m).expect("link frames are small")
}

impl Link {
    /// Pump until the session ends.
    pub async fn run(
        mut self,
        session: DuplexStream,
        first: BoxedHostStream,
        mut connect: Connect,
        mut cfg: LinkConfig,
    ) -> LinkOutcome {
        let (srd, swr) = tokio::io::split(session);
        let mut from_session = FramedRead::new(srd, FrameCodec);
        let mut to_session = FramedWrite::new(swr, FrameCodec);
        let (bell_tx, mut bells) = mpsc::unbounded_channel::<String>();
        let mut stream = first;
        let mut reconnects: u64 = 0;
        let mut pending: Option<Bytes> = None;
        let mut last_lost = String::new();
        // The host has asked for a hold and not yet sent `Restore`.
        let mut held = false;

        loop {
            let conn_started = Instant::now();
            let (hrd, hwr) = tokio::io::split(stream);
            let mut from_host = FramedRead::new(hrd, FrameCodec);
            let mut to_host = FramedWrite::new(hwr, FrameCodec);

            let lost: String = 'conn: {
                if reconnects > 0 {
                    let hello = GuestMessage::Reconnect {
                        protocol_version: PROTOCOL_VERSION,
                        environment_id: cfg.environment_id.clone(),
                        guest_boot_id: cfg.guest_boot_id.clone(),
                        reconnects,
                        lost: last_lost.clone(),
                    };
                    if let Err(e) = to_host.send(encode(&hello)).await {
                        break 'conn format!("send reconnect: {e}");
                    }
                }
                if let Some(frame) = pending.take()
                    && let Err(e) = to_host.send(frame.clone()).await
                {
                    pending = Some(frame);
                    break 'conn format!("resend: {e}");
                }
                loop {
                    tokio::select! {
                        f = from_session.next() => match f {
                            None | Some(Err(_)) => return LinkOutcome::SessionClosed,
                            Some(Ok(frame)) => {
                                if let Err(e) = to_host.send(frame.clone()).await {
                                    if peek(&frame).is_none_or(|t| t.kind != "heartbeat") {
                                        pending = Some(frame);
                                    }
                                    break 'conn format!("write: {e}");
                                }
                            }
                        },
                        f = from_host.next() => match f {
                            None => break 'conn "host closed the connection".to_string(),
                            Some(Err(e)) => break 'conn format!("read: {e}"),
                            Some(Ok(frame)) => {
                                let kind = peek(&frame);
                                match kind.as_ref().map(|t| t.kind.as_str()) {
                                    Some("hello_ack") if kind.as_ref().is_some_and(|t| t.snapshot_hold) => {
                                        info!("restore link: snapshot hold requested by the host");
                                        held = true;
                                        self.shared.hold.store(true, Ordering::SeqCst);
                                        if let Some(arm) = cfg.arm_doorbell.take() {
                                            arm(bell_tx.clone());
                                        }
                                    }
                                    Some("restore") => {
                                        if !held || reconnects == 0 {
                                            warn!(held, reconnects, "restore link: unexpected restore frame ignored");
                                            continue;
                                        }
                                        match decode_message::<HostMessage>(&frame) {
                                            Ok(HostMessage::Restore { environment_id, instance_id, generation, epoch, host_now_ms }) => {
                                                let clock = match &cfg.set_clock {
                                                    Some(set) => set(host_now_ms).map_err(|e| e.to_string()),
                                                    None => Ok(()),
                                                };
                                                if let Err(e) = &clock {
                                                    // The after-restore hook would read a stale
                                                    // clock; the host sees it in the log.
                                                    error!("restore link: cannot set the guest clock: {e}");
                                                }
                                                info!(%environment_id, %instance_id, generation, epoch, clock_set = clock.is_ok(), "restore link: restored");
                                                held = false;
                                                self.shared.hold.store(false, Ordering::SeqCst);
                                                self.shared.answer.send_replace(Some(Continuation::Restored {
                                                    instance_id,
                                                    restored_at_ms: now_ms(),
                                                    generation,
                                                }));
                                            }
                                            _ => warn!("restore link: undecodable restore frame ignored"),
                                        }
                                        continue;
                                    }
                                    _ => {}
                                }
                                if to_session.send(frame).await.is_err() {
                                    return LinkOutcome::SessionClosed;
                                }
                            }
                        },
                        Some(reason) = bells.recv() => {
                            if !held {
                                continue;
                            }
                            // The write failure may have won the race already.
                            if reconnects > 0 && conn_started.elapsed() < RESET_GRACE {
                                info!(%reason, "restore link: signal ignored (connection is fresh)");
                                continue;
                            }
                            break 'conn reason;
                        }
                        Some(()) = self.waiting.recv() => {
                            let w = GuestMessage::CheckpointWaiting {
                                lifecycle_phase: CHECKPOINT_PHASE.to_string(),
                                lifecycle_version: lifecycle::VERSION,
                                after_restore_ran: false,
                            };
                            if let Err(e) = to_host.send(encode(&w)).await {
                                break 'conn format!("write checkpoint_waiting: {e}");
                            }
                        }
                    }
                }
            };

            if !held {
                return LinkOutcome::HostLost(lost);
            }
            warn!(%lost, reconnects, "restore link: host connection lost while held; reconnecting");
            last_lost = lost;
            let started = Instant::now();
            stream = loop {
                match connect().await {
                    Ok(s) => break s,
                    Err(e) if started.elapsed() >= cfg.reconnect_budget => {
                        return LinkOutcome::GaveUp(format!("reconnect: {e}"));
                    }
                    Err(_) => tokio::time::sleep(cfg.reconnect_interval).await,
                }
            };
            reconnects += 1;
            info!(
                reconnects,
                after_ms = started.elapsed().as_millis() as u64,
                "restore link: reconnected"
            );
        }
    }
}

/// Set `CLOCK_REALTIME` (Linux guests; PID 1 has `CAP_SYS_TIME`).
#[cfg(target_os = "linux")]
pub fn set_realtime_clock(ms: u64) -> std::io::Result<()> {
    // SAFETY: an all-zero timespec is valid; fields are set below with the
    // platform's own types (`time_t` is deprecated as a name on musl).
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    ts.tv_sec = (ms / 1000).try_into().unwrap_or(0);
    ts.tv_nsec = ((ms % 1000) * 1_000_000).try_into().unwrap_or(0);
    // SAFETY: `ts` is a valid timespec.
    let rc = unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Doorbell on a guest vsock listen port (Linux guests).
#[cfg(target_os = "linux")]
pub fn vsock_doorbell(port: u32) -> ArmDoorbell {
    Box::new(move |tx| {
        tokio::spawn(async move {
            use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};
            match VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, port)) {
                Ok(listener) => loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            info!(peer_cid = peer.cid(), "restore link: doorbell rang");
                            drop(stream);
                            if tx.send("vsock doorbell".to_string()).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("restore link: doorbell accept failed: {e}");
                            break;
                        }
                    }
                },
                Err(e) => warn!("restore link: doorbell bind failed: {e}"),
            }
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{UnixListener, UnixStream};

    type HostSide = (
        FramedRead<tokio::io::ReadHalf<UnixStream>, FrameCodec>,
        FramedWrite<tokio::io::WriteHalf<UnixStream>, FrameCodec>,
    );

    async fn accept(l: &UnixListener) -> HostSide {
        let (s, _) = l.accept().await.unwrap();
        let (r, w) = tokio::io::split(s);
        (
            FramedRead::new(r, FrameCodec),
            FramedWrite::new(w, FrameCodec),
        )
    }

    fn unix_connect(path: std::path::PathBuf) -> Connect {
        Box::new(move || {
            let p = path.clone();
            Box::pin(async move {
                let s = UnixStream::connect(p).await?;
                Ok(Box::new(s) as BoxedHostStream)
            })
        })
    }

    fn ack(hold: bool) -> Bytes {
        encode_message(&HostMessage::HelloAck {
            environment_id: "env_src".into(),
            epoch: 1,
            entrypoint: "/function/app".into(),
            args: vec![],
            env: vec![],
            working_dir: "/".into(),
            init_timeout_ms: 1000,
            max_response_bytes: 1024,
            max_log_line_bytes: 1024,
            snapshot_hold: hold,
        })
        .unwrap()
    }

    fn cfg(bells: Option<ArmDoorbell>, clock: Option<SetClock>) -> LinkConfig {
        LinkConfig {
            environment_id: "env_src".into(),
            guest_boot_id: Some("boot-1".into()),
            reconnect_budget: Duration::from_secs(5),
            reconnect_interval: Duration::from_millis(10),
            arm_doorbell: bells,
            set_clock: clock,
        }
    }

    /// Without a hold the link is a pipe: `continue` is cold at once and a
    /// lost connection ends the session (no reconnect).
    #[tokio::test]
    async fn without_a_hold_it_is_transparent_and_a_loss_ends_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("h.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = Box::new(UnixStream::connect(&sock).await.unwrap()) as BoxedHostStream;
        let (source, link) = link();
        let (session, link_side) = tokio::io::duplex(1 << 20);
        let task = tokio::spawn(link.run(
            link_side,
            first,
            unix_connect(sock.clone()),
            cfg(None, None),
        ));
        let (mut h_read, mut h_write) = accept(&listener).await;
        let (sr, sw) = tokio::io::split(session);
        let mut s_read = FramedRead::new(sr, FrameCodec);
        let mut s_write = FramedWrite::new(sw, FrameCodec);

        h_write.send(ack(false)).await.unwrap();
        assert_eq!(s_read.next().await.unwrap().unwrap(), ack(false));
        assert_eq!(source.continuation().await, Continuation::Cold);
        let ready = encode(&GuestMessage::Ready { init_ms: 1 });
        s_write.send(ready.clone()).await.unwrap();
        assert_eq!(h_read.next().await.unwrap().unwrap(), ready);

        drop((h_read, h_write));
        match task.await.unwrap() {
            LinkOutcome::HostLost(_) => {}
            other => panic!("expected HostLost, got {other:?}"),
        }
    }

    /// The full hold: checkpoint report, loss, reconnect with the same boot
    /// id, doorbell-driven reconnect, restore answers `continue` with the
    /// host's identity and sets the clock before that.
    #[tokio::test]
    async fn a_held_guest_reports_the_checkpoint_reconnects_and_is_restored() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("h.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = Box::new(UnixStream::connect(&sock).await.unwrap()) as BoxedHostStream;
        let (source, link) = link();
        let (session, link_side) = tokio::io::duplex(1 << 20);
        let armed = Arc::new(std::sync::Mutex::new(None::<mpsc::UnboundedSender<String>>));
        let armed2 = armed.clone();
        let clock_set = Arc::new(std::sync::Mutex::new(None::<u64>));
        let clock_set2 = clock_set.clone();
        let task = tokio::spawn(link.run(
            link_side,
            first,
            unix_connect(sock.clone()),
            cfg(
                Some(Box::new(move |tx| *armed2.lock().unwrap() = Some(tx))),
                Some(Box::new(move |ms| {
                    *clock_set2.lock().unwrap() = Some(ms);
                    Ok(())
                })),
            ),
        ));
        let (sr, sw) = tokio::io::split(session);
        let mut s_read = FramedRead::new(sr, FrameCodec);
        let _s_write = FramedWrite::new(sw, FrameCodec);

        // Source connection: hold requested, the process waits in continue.
        let (mut h_read, mut h_write) = accept(&listener).await;
        h_write.send(ack(true)).await.unwrap();
        assert_eq!(s_read.next().await.unwrap().unwrap(), ack(true));
        assert!(armed.lock().unwrap().is_some(), "doorbell armed on hold");
        let src = Arc::new(source);
        let src2 = src.clone();
        let cont = tokio::spawn(async move { src2.continuation().await });
        let f = h_read.next().await.unwrap().unwrap();
        assert_eq!(
            decode_message::<GuestMessage>(&f).unwrap(),
            GuestMessage::CheckpointWaiting {
                lifecycle_phase: "checkpoint".into(),
                lifecycle_version: lifecycle::VERSION,
                after_restore_ran: false,
            }
        );

        // Snapshot reset: the connection goes away.
        drop((h_read, h_write));
        let (mut h_read, h_write) = accept(&listener).await;
        match decode_message::<GuestMessage>(&h_read.next().await.unwrap().unwrap()).unwrap() {
            GuestMessage::Reconnect {
                protocol_version,
                environment_id,
                guest_boot_id,
                reconnects,
                ..
            } => {
                assert_eq!(protocol_version, PROTOCOL_VERSION);
                assert_eq!(environment_id, "env_src");
                assert_eq!(guest_boot_id.as_deref(), Some("boot-1"));
                assert_eq!(reconnects, 1);
            }
            other => panic!("expected reconnect, got {other:?}"),
        }
        assert!(!cont.is_finished());

        // A doorbell on the stale connection (after the grace) forces a
        // reconnect even though the old socket looks healthy.
        tokio::time::sleep(RESET_GRACE + Duration::from_millis(50)).await;
        armed
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send("vsock doorbell".into())
            .unwrap();
        let (mut h_read2, mut h_write2) = accept(&listener).await;
        drop((h_read, h_write));
        match decode_message::<GuestMessage>(&h_read2.next().await.unwrap().unwrap()).unwrap() {
            GuestMessage::Reconnect {
                reconnects, lost, ..
            } => {
                assert_eq!(reconnects, 2);
                assert_eq!(lost, "vsock doorbell");
            }
            other => panic!("expected reconnect, got {other:?}"),
        }

        h_write2
            .send(
                encode_message(&HostMessage::Restore {
                    environment_id: "env_clone".into(),
                    instance_id: "rst_a".into(),
                    generation: 1,
                    epoch: 1,
                    host_now_ms: 1_800_000_000_000,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        match cont.await.unwrap() {
            Continuation::Restored {
                instance_id,
                generation,
                ..
            } => {
                assert_eq!(instance_id, "rst_a");
                assert_eq!(generation, 1);
            }
            Continuation::Cold => panic!("expected restored"),
        }
        assert_eq!(*clock_set.lock().unwrap(), Some(1_800_000_000_000));

        // Transparent again: host frames reach the session, and a later loss
        // ends the session instead of reconnecting.
        let ping = encode_message(&HostMessage::Ping { nonce: 3 }).unwrap();
        h_write2.send(ping.clone()).await.unwrap();
        assert_eq!(s_read.next().await.unwrap().unwrap(), ping);
        drop((h_read2, h_write2));
        assert!(matches!(task.await.unwrap(), LinkOutcome::HostLost(_)));
    }

    /// A `Restore` on the original connection (no reconnect happened, so this
    /// guest was never snapshotted) is not honoured.
    #[tokio::test]
    async fn a_restore_without_a_reconnect_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("h.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = Box::new(UnixStream::connect(&sock).await.unwrap()) as BoxedHostStream;
        let (source, link) = link();
        let (session, link_side) = tokio::io::duplex(1 << 20);
        let _task = tokio::spawn(link.run(
            link_side,
            first,
            unix_connect(sock.clone()),
            cfg(None, None),
        ));
        let (sr, _sw) = tokio::io::split(session);
        let mut s_read = FramedRead::new(sr, FrameCodec);
        let (mut h_read, mut h_write) = accept(&listener).await;
        h_write.send(ack(true)).await.unwrap();
        s_read.next().await.unwrap().unwrap();
        let src = Arc::new(source);
        let src2 = src.clone();
        let cont = tokio::spawn(async move { src2.continuation().await });
        h_read.next().await.unwrap().unwrap();
        h_write
            .send(
                encode_message(&HostMessage::Restore {
                    environment_id: "env_x".into(),
                    instance_id: "forged".into(),
                    generation: 1,
                    epoch: 1,
                    host_now_ms: 1,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!cont.is_finished(), "continue must stay held");
    }

    #[tokio::test]
    async fn a_held_guest_gives_up_when_the_host_never_comes_back() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("h.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = Box::new(UnixStream::connect(&sock).await.unwrap()) as BoxedHostStream;
        let (_source, link) = link();
        let (_session, link_side) = tokio::io::duplex(1 << 20);
        let mut c = cfg(None, None);
        c.reconnect_budget = Duration::from_millis(100);
        let task = tokio::spawn(link.run(link_side, first, unix_connect(sock.clone()), c));
        let (h_read, mut h_write) = accept(&listener).await;
        h_write.send(ack(true)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop((h_read, h_write));
        drop(listener);
        std::fs::remove_file(&sock).unwrap();
        assert!(matches!(task.await.unwrap(), LinkOutcome::GaveUp(_)));
    }
}
