//! `tsls-load`: bounded, local-only load scenarios for the gateway (PLT-4637,
//! docs/metrics.md §load scenarios).
//!
//! - [`limits`]: declared limits, compiled-in ceilings, target allowlist;
//! - [`plan`]: phases and seeded jitter;
//! - [`run`]: sending load, sampling `/metrics` and `/v1/capacity`;
//! - [`detect`]: overshoot, starvation, idle CPU and boot identity detectors;
//! - [`report`]: summary JSON, SVG and ASCII timelines;
//! - [`prom`]: a small Prometheus text reader.

pub mod detect;
pub mod limits;
pub mod plan;
pub mod prom;
pub mod report;
pub mod run;
