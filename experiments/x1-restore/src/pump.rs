//! Frame pump between the bridge session (an in-memory duplex) and the host
//! connection, which it re-establishes after a transport reset.
//!
//! Limits (experiment, documented in ADR-0015): a frame whose write failed is
//! resent once on the next connection (heartbeats are dropped), but a write
//! into a socket buffer whose peer is already gone can succeed and the frame
//! is then lost, as is a frame the host wrote that the guest had not read.
//! At the checkpoint wait point nothing but heartbeats flows, which is why
//! the experiment snapshots only there.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tachyon_serverless_protocol::FrameCodec;
use tachyon_serverless_protocol::runtime_api::lifecycle::Continuation;
use tachyon_serverless_runtime_bridge::runtime_api::RestoreSource;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio::sync::{mpsc, watch};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info, warn};

use crate::clock::{Clocks, urandom_hex};
use crate::control::{GuestControl, HostControl, frame_type, is_control};

/// A reset signal within this long of a reconnect is treated as the same
/// restore and ignored.
const RESET_GRACE: Duration = Duration::from_secs(2);

/// Lifecycle `continue` answered from host control frames.
pub struct X1Restore {
    answer: watch::Sender<Option<Continuation>>,
    waiting: mpsc::UnboundedSender<()>,
}

/// The pump's side of [`X1Restore`].
pub struct RestoreHandle {
    answer: watch::Sender<Option<Continuation>>,
    waiting: mpsc::UnboundedReceiver<()>,
}

/// A restore source and the handle the pump uses to answer it.
pub fn restore_pair() -> (Arc<X1Restore>, RestoreHandle) {
    let (answer, _) = watch::channel(None);
    let (wtx, wrx) = mpsc::unbounded_channel();
    (
        Arc::new(X1Restore {
            answer: answer.clone(),
            waiting: wtx,
        }),
        RestoreHandle {
            answer,
            waiting: wrx,
        },
    )
}

impl RestoreSource for X1Restore {
    fn continuation(&self) -> Pin<Box<dyn Future<Output = Continuation> + Send + '_>> {
        let _ = self.waiting.send(());
        let mut rx = self.answer.subscribe();
        Box::pin(async move {
            loop {
                if let Some(c) = rx.borrow_and_update().clone() {
                    return c;
                }
                if rx.changed().await.is_err() {
                    // The pump is gone, so is the session; the value is moot.
                    return Continuation::Cold;
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct PumpConfig {
    pub guest_boot_id: Option<String>,
    /// File written with the instance id when a restore is announced
    /// (the scratch drive in the guest), to show clone writes do not share.
    pub scratch_marker: Option<PathBuf>,
    /// How long to keep trying to reconnect before giving up.
    pub reconnect_budget: Duration,
    pub reconnect_interval: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PumpOutcome {
    /// The session closed its side (normal end).
    SessionClosed,
    /// Reconnecting did not succeed within the budget.
    GaveUp(String),
}

/// Run the pump until the session ends. `first` is an already connected host
/// stream; `connect` makes new ones after a loss. A message on `resets` (the
/// guest observed a VM generation change) drops the current connection at
/// once instead of waiting for the next write to fail.
pub async fn run_pump<S, F, Fut>(
    session: DuplexStream,
    first: S,
    mut connect: F,
    mut restore: RestoreHandle,
    mut resets: mpsc::UnboundedReceiver<String>,
    cfg: PumpConfig,
) -> PumpOutcome
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FnMut() -> Fut,
    Fut: Future<Output = std::io::Result<S>>,
{
    let (srd, swr) = tokio::io::split(session);
    let mut from_session = FramedRead::new(srd, FrameCodec);
    let mut to_session = FramedWrite::new(swr, FrameCodec);
    let mut stream = first;
    let mut connection: u64 = 0;
    let mut pending: Option<Bytes> = None;
    let mut last_lost = String::new();

    loop {
        let conn_started = Instant::now();
        let (hrd, hwr) = tokio::io::split(stream);
        let mut from_host = FramedRead::new(hrd, FrameCodec);
        let mut to_host = FramedWrite::new(hwr, FrameCodec);

        let lost: String = 'conn: {
            if connection > 0 {
                let hello = GuestControl::Reconnect {
                    guest_boot_id: cfg.guest_boot_id.clone(),
                    connection,
                    lost: last_lost.clone(),
                    clocks: Clocks::now(),
                    urandom_hex: urandom_hex(),
                };
                if let Err(e) = to_host.send(encode(&hello)).await {
                    break 'conn format!("send x1_reconnect: {e}");
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
                        None | Some(Err(_)) => return PumpOutcome::SessionClosed,
                        Some(Ok(frame)) => {
                            if let Err(e) = to_host.send(frame.clone()).await {
                                if frame_type(&frame).as_deref() != Some("heartbeat") {
                                    pending = Some(frame);
                                }
                                break 'conn format!("write: {e}");
                            }
                        }
                    },
                    f = from_host.next() => match f {
                        None => break 'conn "host closed the connection".to_string(),
                        Some(Err(e)) => break 'conn format!("read: {e}"),
                        Some(Ok(frame)) if is_control(&frame) => {
                            if let Some(reply) = handle_control(&frame, &restore, &cfg)
                                && let Err(e) = to_host.send(encode(&reply)).await
                            {
                                break 'conn format!("write control: {e}");
                            }
                        }
                        Some(Ok(frame)) => {
                            if to_session.send(frame).await.is_err() {
                                return PumpOutcome::SessionClosed;
                            }
                        }
                    },
                    Some(reason) = resets.recv() => {
                        // A restore signal that arrives just after this copy
                        // already reconnected (the write failure won the race)
                        // must not tear down the fresh connection.
                        if connection > 0 && conn_started.elapsed() < RESET_GRACE {
                            info!(%reason, "x1: reset signal ignored (connection is fresh)");
                            continue;
                        }
                        break 'conn reason;
                    }
                    Some(()) = restore.waiting.recv() => {
                        let w = GuestControl::Waiting { clocks: Clocks::now() };
                        if let Err(e) = to_host.send(encode(&w)).await {
                            break 'conn format!("write x1_waiting: {e}");
                        }
                    }
                }
            }
        };

        warn!(%lost, connection, "x1: host connection lost; reconnecting");
        last_lost = lost;
        let started = Instant::now();
        stream = loop {
            match connect().await {
                Ok(s) => break s,
                Err(e) if started.elapsed() >= cfg.reconnect_budget => {
                    return PumpOutcome::GaveUp(format!("reconnect: {e}"));
                }
                Err(_) => tokio::time::sleep(cfg.reconnect_interval).await,
            }
        };
        connection += 1;
        info!(
            connection,
            after_ms = started.elapsed().as_millis() as u64,
            "x1: reconnected"
        );
    }
}

fn encode<T: serde::Serialize>(v: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(v).expect("control frames serialize"))
}

fn handle_control(frame: &[u8], restore: &RestoreHandle, cfg: &PumpConfig) -> Option<GuestControl> {
    let Ok(HostControl::Continue {
        restored,
        instance_id,
        generation,
    }) = serde_json::from_slice::<HostControl>(frame)
    else {
        warn!("x1: unknown control frame ignored");
        return None;
    };
    if !restored {
        info!("x1: continue answered cold");
        restore.answer.send_replace(Some(Continuation::Cold));
        return None;
    }
    let instance_id = instance_id.unwrap_or_default();
    let reply = cfg.scratch_marker.as_ref().map(|path| {
        let body = format!("{instance_id}\n");
        let result = (|| -> std::io::Result<()> {
            use std::io::Write;
            let mut f = std::fs::File::create(path)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()
        })();
        GuestControl::ScratchWritten {
            path: path.display().to_string(),
            bytes: body.len() as u64,
            error: result.err().map(|e| e.to_string()),
        }
    });
    info!(%instance_id, generation, "x1: continue answered restored");
    restore.answer.send_replace(Some(Continuation::Restored {
        instance_id,
        restored_at_ms: Clocks::now().wall_ms,
        generation,
    }));
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_protocol::{GuestMessage, decode_message, encode_message};
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

    /// A reset connection is re-established, the guest identifies itself, a
    /// frame written while disconnected still arrives, and `continue` resolves
    /// only on the host's `restored` answer (with the marker written).
    #[tokio::test]
    async fn survives_a_reset_and_answers_continue_from_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("v.sock_5000");
        let marker = dir.path().join("x1-instance");
        let listener = UnixListener::bind(&sock).unwrap();
        let (source, handle) = restore_pair();
        let (session, pump_side) = tokio::io::duplex(1 << 20);
        let first = UnixStream::connect(&sock).await.unwrap();
        let path = sock.clone();
        let cfg = PumpConfig {
            guest_boot_id: Some("boot-1".into()),
            scratch_marker: Some(marker.clone()),
            reconnect_budget: Duration::from_secs(5),
            reconnect_interval: Duration::from_millis(10),
        };
        let (_reset_tx, resets) = mpsc::unbounded_channel();
        let pump = tokio::spawn(run_pump(
            pump_side,
            first,
            move || UnixStream::connect(path.clone()),
            handle,
            resets,
            cfg,
        ));
        let (sr, sw) = tokio::io::split(session);
        let mut s_read = FramedRead::new(sr, FrameCodec);
        let mut s_write = FramedWrite::new(sw, FrameCodec);

        // connection 0: the process starts waiting.
        let (mut h_read, h_write) = accept(&listener).await;
        let cont = tokio::spawn(async move { source.continuation().await });
        let f = h_read.next().await.unwrap().unwrap();
        assert_eq!(frame_type(&f).as_deref(), Some("x1_waiting"));

        // reset (snapshot): drop the host side.
        drop((h_read, h_write));

        // connection 1 (restored copy)
        let (mut h_read, mut h_write) = accept(&listener).await;
        let ready = encode_message(&GuestMessage::Ready { init_ms: 7 }).unwrap();
        s_write.send(ready).await.unwrap();
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let f = h_read.next().await.unwrap().unwrap();
            seen.push(f);
        }
        let reconnect: GuestControl = serde_json::from_slice(&seen[0]).unwrap();
        match reconnect {
            GuestControl::Reconnect {
                guest_boot_id,
                connection,
                ..
            } => {
                assert_eq!(guest_boot_id.as_deref(), Some("boot-1"));
                assert_eq!(connection, 1);
            }
            other => panic!("expected reconnect, got {other:?}"),
        }
        assert_eq!(
            decode_message::<GuestMessage>(&seen[1]).unwrap(),
            GuestMessage::Ready { init_ms: 7 }
        );
        assert!(!cont.is_finished());

        let answer = HostControl::Continue {
            restored: true,
            instance_id: Some("clone-a".into()),
            generation: 1,
        };
        h_write.send(encode(&answer)).await.unwrap();
        let scratch = h_read.next().await.unwrap().unwrap();
        assert_eq!(frame_type(&scratch).as_deref(), Some("x1_scratch_written"));
        match cont.await.unwrap() {
            Continuation::Restored {
                instance_id,
                generation,
                ..
            } => {
                assert_eq!(instance_id, "clone-a");
                assert_eq!(generation, 1);
            }
            Continuation::Cold => panic!("expected restored"),
        }
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "clone-a\n");

        // protocol frames from the host reach the session untouched.
        let shutdown = encode_message(&tachyon_serverless_protocol::HostMessage::Shutdown {
            reason: "done".into(),
        })
        .unwrap();
        h_write.send(shutdown.clone()).await.unwrap();
        assert_eq!(s_read.next().await.unwrap().unwrap(), shutdown);

        drop((s_read, s_write));
        assert_eq!(pump.await.unwrap(), PumpOutcome::SessionClosed);
    }

    /// A generation-change signal reconnects even though the old connection
    /// still looks healthy, and the reason reaches the host.
    #[tokio::test]
    async fn a_reset_signal_forces_a_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("v.sock_5000");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = UnixStream::connect(&sock).await.unwrap();
        let (_source, handle) = restore_pair();
        let (_session, pump_side) = tokio::io::duplex(1 << 16);
        let (reset_tx, resets) = mpsc::unbounded_channel();
        let path = sock.clone();
        let _pump = tokio::spawn(run_pump(
            pump_side,
            first,
            move || UnixStream::connect(path.clone()),
            handle,
            resets,
            PumpConfig {
                guest_boot_id: Some("boot-2".into()),
                scratch_marker: None,
                reconnect_budget: Duration::from_secs(5),
                reconnect_interval: Duration::from_millis(10),
            },
        ));
        let _old = accept(&listener).await;
        reset_tx.send("vmgenid uevent".into()).unwrap();
        let (mut h_read, _h_write) = accept(&listener).await;
        let f = h_read.next().await.unwrap().unwrap();
        match serde_json::from_slice::<GuestControl>(&f).unwrap() {
            GuestControl::Reconnect { lost, .. } => assert_eq!(lost, "vmgenid uevent"),
            other => panic!("expected reconnect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn gives_up_when_the_host_never_comes_back() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("v.sock_5000");
        let listener = UnixListener::bind(&sock).unwrap();
        let first = UnixStream::connect(&sock).await.unwrap();
        let (_source, handle) = restore_pair();
        let (_session, pump_side) = tokio::io::duplex(1 << 16);
        let path = sock.clone();
        let (_reset_tx, resets) = mpsc::unbounded_channel();
        let pump = tokio::spawn(run_pump(
            pump_side,
            first,
            move || UnixStream::connect(path.clone()),
            handle,
            resets,
            PumpConfig {
                guest_boot_id: None,
                scratch_marker: None,
                reconnect_budget: Duration::from_millis(100),
                reconnect_interval: Duration::from_millis(10),
            },
        ));
        let (s, _) = listener.accept().await.unwrap();
        drop(s);
        drop(listener);
        std::fs::remove_file(&sock).unwrap();
        assert!(matches!(pump.await.unwrap(), PumpOutcome::GaveUp(_)));
    }
}
