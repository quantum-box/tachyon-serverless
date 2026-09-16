//! The bootstrap log is part of the contract (PLT-4633 acceptance 4): an
//! operator must be able to read whether this gateway reuses environments,
//! whether that configuration was ever measured, and why — without calling the
//! API. `GET /v1/provider` carries the same facts (see `tests/pipeline.rs`).
//!
//! This lives in its own test binary because it needs the **global** tracing
//! subscriber. A thread-local one (`tracing::subscriber::set_default`) leaves
//! callsite interest depending on what other tests in the same process touched
//! first, which is exactly the kind of flake a log assertion must not have.

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use tachyon_serverless_application::{Application, BootstrapOptions, GatewayConfig};
use tachyon_serverless_domain::{Architecture, EnvironmentId, ProviderKind};
use tachyon_serverless_provider_port::{
    ArtifactLocation, Capabilities, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
    ExecutionProvider, IsolationLevel, PreflightReport, ProviderError, Support, TerminateReason,
    TerminateReport,
};

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A provider that reports both idle capabilities as `Unverified`: the code
/// exists but nobody measured it, which is exactly what the shipped Firecracker
/// provider reports (docs/adr/0001 §5).
///
/// Bootstrapping only ever asks for `kind` and `capabilities`; everything else
/// would be a lifecycle call, and this test never runs an invocation.
struct UnverifiedIdleProvider;

#[async_trait]
impl ExecutionProvider for UnverifiedIdleProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Firecracker
    }
    fn capabilities(&self) -> Capabilities {
        let unsupported = |what: &str| Support::unsupported(what.to_string());
        Capabilities {
            isolation: IsolationLevel::MicroVm,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            enforce_resource_limits: Support::unverified("not measured"),
            egress_none: Support::Supported,
            egress_restricted: unsupported("no network device"),
            egress_public_web: unsupported("no network device"),
            host_metering: Support::unverified("not measured"),
            idle_quiesce: Support::unverified("implemented but not measured on KVM"),
            idle_resume: Support::unverified("implemented but not measured on KVM"),
            snapshot_create: unsupported("not implemented"),
            snapshot_clone: unsupported("not implemented"),
            dev_only: false,
        }
    }
    async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
        unimplemented!("bootstrap does not preflight")
    }
    async fn validate_artifact(
        &self,
        _: &ArtifactLocation,
        _: Architecture,
    ) -> Result<(), ProviderError> {
        unimplemented!("nothing is deployed here")
    }
    async fn create_environment(
        &self,
        _: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError> {
        unimplemented!("nothing is invoked here")
    }
    async fn terminate_environment(
        &self,
        _: &EnvironmentId,
        _: TerminateReason,
    ) -> Result<TerminateReport, ProviderError> {
        unimplemented!("nothing is invoked here")
    }
    async fn observe_environment(
        &self,
        _: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError> {
        unimplemented!("nothing is invoked here")
    }
    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
        unimplemented!("the startup reconcile is not run here")
    }
}

fn config(dir: &Path, pool: &str) -> GatewayConfig {
    GatewayConfig::from_toml(&format!(
        r#"
listen = "127.0.0.1:0"
profile = "dev"
data_dir = "{data}"

[provider]
kind = "process"

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "{data}/process"

{pool}
"#,
        data = dir.display()
    ))
    .expect("valid configuration")
}

fn bootstrap(dir: &Path, pool: &str) -> Arc<Application> {
    Application::bootstrap_with(
        config(dir, pool),
        Arc::new(UnverifiedIdleProvider),
        BootstrapOptions {
            persist_state: false,
            ..BootstrapOptions::default()
        },
    )
    .expect("bootstrap")
}

#[test]
fn the_bootstrap_log_says_whether_environments_are_reused_and_why() {
    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(captured.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("this binary sets the global subscriber exactly once");

    // 1. The default: an unverified idle capability keeps reuse off even with
    //    `[pool] enabled = true`, and the log names the gate that decided it.
    let dir = tempfile::tempdir().unwrap();
    let app = bootstrap(dir.path(), "[pool]\nenabled = true\n");
    assert!(!app.pool.policy().reuse_enabled());
    let log = captured.text();
    assert!(log.contains("application bootstrapped"), "{log}");
    assert!(log.contains("environment_reuse=false"), "{log}");
    assert!(log.contains("reuse_verified=false"), "{log}");
    assert!(log.contains("idle_quiesce="), "{log}");
    assert!(
        log.contains("allow_unverified_idle is not set"),
        "the log must say what would change the decision: {log}"
    );
    assert!(
        !log.contains("UNVERIFIED idle capability"),
        "there is nothing to warn about while reuse is off: {log}"
    );

    // 2. With the measurement switch reuse really runs — and the log says, in
    //    its own warning, that this is a measurement configuration and not a
    //    verified warm one.
    let dir = tempfile::tempdir().unwrap();
    let app = bootstrap(
        dir.path(),
        "[pool]\nenabled = true\nallow_unverified_idle = true\n",
    );
    assert!(app.pool.policy().reuse_enabled());
    assert!(
        !app.pool.policy().idle_verified(),
        "running is not the same as measured"
    );
    let log = captured.text();
    assert!(log.contains("environment_reuse=true"), "{log}");
    assert!(
        log.contains("reuse_verified=false"),
        "an unverified configuration is never logged as verified: {log}"
    );
    assert!(log.contains("UNVERIFIED idle capability"), "{log}");
    assert!(log.contains("allow_unverified_idle"), "{log}");
    assert!(
        log.contains("must not be reported as a verified warm setup"),
        "{log}"
    );
}
