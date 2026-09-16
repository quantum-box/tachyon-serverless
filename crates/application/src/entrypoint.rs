//! Entrypoint policy: where the guest finds the executable and which working
//! directory it runs in, keyed on the provider kind (docs/protocol.md §C).
//!
//! - Firecracker: the function drive is mounted at `/function` inside the
//!   guest, so the entrypoint is `/function/app` and the working directory is
//!   the guest tmpfs `/tmp`.
//! - Process / fake (no isolation): the entrypoint is the artifact's host
//!   path and the working directory is the per-environment directory under
//!   the provider workdir (`<workdir>/<environment_id>`).

use std::path::{Path, PathBuf};

use tachyon_serverless_domain::{EnvironmentId, ProviderKind};

/// Guest-side entrypoint and working directory for one environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entrypoint {
    pub entrypoint: String,
    pub working_dir: String,
    pub args: Vec<String>,
}

/// Firecracker guest path of the function executable.
pub const FIRECRACKER_ENTRYPOINT: &str = "/function/app";
/// Firecracker guest working directory.
pub const FIRECRACKER_WORKING_DIR: &str = "/tmp";

#[derive(Debug, Clone)]
pub struct EntrypointPolicy {
    /// Provider workdir on the host (unisolated providers only).
    provider_workdir: PathBuf,
}

impl EntrypointPolicy {
    pub fn new(provider_workdir: impl Into<PathBuf>) -> Self {
        Self {
            provider_workdir: provider_workdir.into(),
        }
    }

    pub fn provider_workdir(&self) -> &Path {
        &self.provider_workdir
    }

    /// Resolve the entrypoint for `kind`.
    pub fn resolve(
        &self,
        kind: &ProviderKind,
        artifact_host_path: &Path,
        environment_id: &EnvironmentId,
    ) -> Entrypoint {
        match kind {
            ProviderKind::Firecracker => Entrypoint {
                entrypoint: FIRECRACKER_ENTRYPOINT.to_string(),
                working_dir: FIRECRACKER_WORKING_DIR.to_string(),
                args: Vec::new(),
            },
            ProviderKind::Process | ProviderKind::Fake | ProviderKind::Other(_) => Entrypoint {
                entrypoint: artifact_host_path.to_string_lossy().into_owned(),
                working_dir: self
                    .provider_workdir
                    .join(environment_id.as_str())
                    .to_string_lossy()
                    .into_owned(),
                args: Vec::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firecracker_uses_guest_paths() {
        let p = EntrypointPolicy::new("/var/run/tachyon");
        let e = p.resolve(
            &ProviderKind::Firecracker,
            Path::new("/data/artifacts/abc"),
            &EnvironmentId::generate(),
        );
        assert_eq!(e.entrypoint, "/function/app");
        assert_eq!(e.working_dir, "/tmp");
    }

    #[test]
    fn process_uses_host_paths() {
        let p = EntrypointPolicy::new("./data/process");
        let id = EnvironmentId::generate();
        let e = p.resolve(
            &ProviderKind::Process,
            Path::new("/data/artifacts/abc"),
            &id,
        );
        assert_eq!(e.entrypoint, "/data/artifacts/abc");
        assert_eq!(e.working_dir, format!("./data/process/{}", id.as_str()));
    }
}
