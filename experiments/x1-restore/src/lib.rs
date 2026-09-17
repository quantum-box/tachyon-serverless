//! X1 (PLT-4652, experimental): harness for measuring whether a Firecracker /
//! Cloud Hypervisor snapshot of a microVM running the runtime bridge and
//! `examples/restore-aware` can be restored and cloned.
//!
//! Nothing here is used by the gateway or a provider. The product bridge
//! (`tachyon-serverless-runtime-bridge`) always answers the lifecycle
//! `continue` with `cold` and exits when its vsock connection breaks, which
//! is what a VMM does to every open vsock connection on snapshot / restore
//! (Firecracker `docs/snapshotting/snapshot-support.md` "Vsock device
//! reset"). To reach a real checkpoint wait point and to observe a restored
//! copy, the experiment runs the **unmodified** bridge session behind a frame
//! pump ([`pump`]) that:
//!
//! - reconnects to the host after a transport reset and identifies itself
//!   with an `x1_reconnect` frame (same `guest_boot_id` as the source, which
//!   is what distinguishes a restore from a cold boot);
//! - tells the host when the process is waiting in `continue`
//!   (`x1_waiting`), and answers `continue` only when the host says `cold` or
//!   `restored` (`x1_continue`).
//!
//! A restored guest learns about the vsock reset only when it next touches
//! the device (first run: the bridge heartbeat, up to 5 s later). The guest
//! init therefore also listens for the kernel's VM generation uevent
//! (`NEW_VMGENID=1`, [`uevent`]) and reconnects as soon as it arrives.
//!
//! The `x1_*` frames are an out-of-band experiment, not protocol: PLT-4653
//! decides the real restore notification (a new frame with a
//! `PROTOCOL_VERSION` bump, or a `HelloAck` capability field).

pub mod clock;
pub mod control;
pub mod pump;
pub mod uevent;
