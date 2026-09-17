//! Experiment-only control frames (`x1_*`). They travel in the same
//! length-prefixed frames as the protocol but are consumed by the pump and
//! never reach the bridge session, so the session sees only protocol frames.

use serde::{Deserialize, Serialize};

use crate::clock::Clocks;

/// Every control frame's `type` starts with this.
pub const CONTROL_PREFIX: &str = "x1_";

/// Guest → host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestControl {
    /// The user process is waiting in `GET lifecycle/continue`: the checkpoint
    /// wait point. Snapshots are taken after this.
    #[serde(rename = "x1_waiting")]
    Waiting { clocks: Clocks },
    /// The pump re-established the host connection after it was lost.
    #[serde(rename = "x1_reconnect")]
    Reconnect {
        /// Unchanged by a restore; a cold boot would have a new one.
        guest_boot_id: Option<String>,
        /// 1 for the first reconnect of this pump.
        connection: u64,
        /// Reason the previous connection ended, as the pump saw it.
        lost: String,
        clocks: Clocks,
        urandom_hex: String,
    },
    /// The per-instance marker was written to the scratch drive.
    #[serde(rename = "x1_scratch_written")]
    ScratchWritten {
        path: String,
        bytes: u64,
        error: Option<String>,
    },
}

/// Host → guest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HostControl {
    /// Answer the lifecycle `continue`.
    #[serde(rename = "x1_continue")]
    Continue {
        restored: bool,
        /// Required when `restored`; delivered after the restore, never
        /// captured in the snapshot.
        instance_id: Option<String>,
        generation: u64,
    },
}

#[derive(Deserialize)]
struct TypeOnly {
    #[serde(rename = "type")]
    kind: String,
}

/// `type` of a JSON frame, if it has one.
pub fn frame_type(frame: &[u8]) -> Option<String> {
    serde_json::from_slice::<TypeOnly>(frame)
        .ok()
        .map(|t| t.kind)
}

/// Whether a frame is an experiment control frame.
pub fn is_control(frame: &[u8]) -> bool {
    frame_type(frame).is_some_and(|t| t.starts_with(CONTROL_PREFIX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_protocol::{GuestMessage, HostMessage, decode_message};

    #[test]
    fn control_frames_are_tagged_and_disjoint_from_the_protocol() {
        let c = HostControl::Continue {
            restored: true,
            instance_id: Some("clone-a".into()),
            generation: 1,
        };
        let v = serde_json::to_vec(&c).unwrap();
        assert!(is_control(&v));
        assert_eq!(frame_type(&v).as_deref(), Some("x1_continue"));
        assert!(decode_message::<HostMessage>(&v).is_err());
        assert_eq!(serde_json::from_slice::<HostControl>(&v).unwrap(), c);

        let g = GuestControl::Waiting {
            clocks: Clocks::now(),
        };
        let v = serde_json::to_vec(&g).unwrap();
        assert!(is_control(&v));
        assert!(decode_message::<GuestMessage>(&v).is_err());

        let hb = serde_json::to_vec(&GuestMessage::Heartbeat { ts_ms: 1 }).unwrap();
        assert!(!is_control(&hb));
        assert_eq!(frame_type(&hb).as_deref(), Some("heartbeat"));
        assert!(!is_control(b"not json"));
    }
}
