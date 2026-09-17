//! `functions deploy`: upload artifact -> create revision -> poll -> show alias.

use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Method;
use tachyon_serverless_api_types::{
    AliasResponse, ArtifactRequest, ArtifactUploadResponse, CreateRevisionRequest,
    EgressAllowRequest, ExecutionRequest, ResourcesRequest, RevisionResponse, SecretBindingRequest,
};

use crate::args::{DeployArgs, parse_egress_allow, parse_key_value};
use crate::client::ApiClient;
use crate::commands::functions::{print_alias, print_revision, revision_status};
use crate::error::{CliError, ExitCode};
use crate::output::Printer;
use crate::resolve::resolve_function_id;

pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct DeployOutcome {
    pub function_id: String,
    pub artifact: ArtifactUploadResponse,
    pub revision: RevisionResponse,
    pub alias: Option<AliasResponse>,
}

/// Build the revision request from CLI arguments and the uploaded digest.
pub fn build_revision_request(
    args: &DeployArgs,
    digest: &str,
) -> Result<CreateRevisionRequest, CliError> {
    let mut resources = ResourcesRequest::default();
    if let Some(m) = args.memory_mib {
        resources.memory_mib = m;
    }
    if let Some(c) = args.cpu_millis {
        resources.cpu_millis = c;
    }
    if let Some(e) = args.ephemeral_storage_mib {
        resources.ephemeral_storage_mib = e;
    }
    let mut execution = ExecutionRequest::default();
    if let Some(t) = args.timeout_seconds {
        execution.timeout_seconds = t;
    }
    if let Some(t) = args.init_timeout_seconds {
        execution.initialization_timeout_seconds = t;
    }
    if let Some(c) = args.max_concurrency {
        execution.max_concurrency = c;
    }
    if let Some(n) = args.min_ready {
        execution.min_ready = n;
    }
    execution.idle_ttl_seconds = args.idle_ttl_seconds;
    execution.scale_down_cooldown_seconds = args.scale_down_cooldown_seconds;
    let env_vars = args
        .env
        .iter()
        .map(|e| parse_key_value(e, "--env"))
        .collect::<Result<Vec<_>, _>>()?;
    let secrets = args
        .secret
        .iter()
        .map(|s| {
            parse_key_value(s, "--secret").map(|(env_name, binding_ref)| SecretBindingRequest {
                env_name,
                binding_ref,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let egress_allow = args
        .egress_allow
        .iter()
        .map(|raw| {
            parse_egress_allow(raw).map(|(protocol, cidr, ports)| EgressAllowRequest {
                cidr,
                protocol,
                ports,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: digest.to_string(),
        },
        architecture: args.arch.resolve()?.to_string(),
        resources,
        execution,
        egress: args.egress.clone(),
        egress_allow,
        env_vars,
        secrets,
        description: args.description.clone(),
        publish_to_prod: !args.no_publish,
        required_region: args.region.clone(),
        restore: (args.restore_policy.is_some() || args.synthetic_init_sample).then(|| {
            tachyon_serverless_api_types::RestoreRequest {
                policy: args
                    .restore_policy
                    .clone()
                    .unwrap_or_else(|| "disabled".into()),
                synthetic_init_sample: args.synthetic_init_sample,
            }
        }),
    })
}

/// Upload raw executable bytes.
pub async fn upload_artifact(
    client: &ApiClient,
    bytes: Vec<u8>,
) -> Result<ArtifactUploadResponse, CliError> {
    client
        .send(
            Method::POST,
            "/v1/artifacts",
            true,
            Some("application/octet-stream"),
            &[],
            Some(Bytes::from(bytes)),
        )
        .await?
        .ok()?
        .json()
}

/// Poll a revision until it reaches `ready` or `failed`.
pub async fn wait_for_revision(
    client: &ApiClient,
    function_id: &str,
    revision_id: &str,
    timeout: Duration,
    p: &mut Printer<'_>,
) -> Result<(RevisionResponse, String), CliError> {
    let start = Instant::now();
    let mut last_status = String::new();
    loop {
        let resp = client
            .get(&format!(
                "/v1/functions/{function_id}/revisions/{revision_id}"
            ))
            .await?
            .ok()?;
        let raw = resp.body_text();
        let r: RevisionResponse = resp.json()?;
        if r.status != last_status {
            p.note(format!("revision {} is {}", r.id, revision_status(&r)))?;
            last_status = r.status.clone();
        }
        match r.status.as_str() {
            "ready" => return Ok((r, raw)),
            "failed" => {
                let reason = r
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "unknown reason".into());
                return Err(CliError::failed(
                    ExitCode::Api,
                    format!("revision {} failed: {reason}", r.id),
                    Some(raw),
                ));
            }
            _ => {}
        }
        if start.elapsed() >= timeout {
            return Err(CliError::Timeout(format!(
                "revision {} still `{}` after {}s (--wait-timeout)",
                r.id,
                r.status,
                timeout.as_secs()
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Fetch the alias, retrying briefly until it points at `revision_id`
/// (the gateway may publish asynchronously right after the revision is ready).
async fn fetch_alias_pointing_at(
    client: &ApiClient,
    function_id: &str,
    alias: &str,
    revision_id: &str,
) -> Result<AliasResponse, CliError> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let resp = client
            .get(&format!("/v1/functions/{function_id}/aliases/{alias}"))
            .await?;
        if resp.is_success() {
            let a: AliasResponse = resp.json()?;
            if a.revision_id == revision_id || Instant::now() >= deadline {
                return Ok(a);
            }
        } else if resp.status != 404 || Instant::now() >= deadline {
            return Err(resp.into_error());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Full deploy flow. Progress goes to stderr; the final revision JSON to stdout
/// under `--json`.
pub async fn deploy(
    client: &ApiClient,
    args: &DeployArgs,
    p: &mut Printer<'_>,
) -> Result<DeployOutcome, CliError> {
    let function_id = resolve_function_id(client, &args.function).await?;
    let bytes = std::fs::read(&args.binary).map_err(|e| {
        CliError::usage(format!(
            "cannot read --binary {}: {e}",
            args.binary.display()
        ))
    })?;
    let request = build_revision_request(args, "sha256:pending")?;
    p.note(format!(
        "uploading {} ({} bytes, arch {})",
        args.binary.display(),
        bytes.len(),
        request.architecture
    ))?;
    let artifact = upload_artifact(client, bytes).await?;
    p.note(format!("artifact {}", artifact.digest))?;
    let request = CreateRevisionRequest {
        artifact: ArtifactRequest::Binary {
            digest: artifact.digest.clone(),
        },
        ..request
    };
    let resp = client
        .post_json(&format!("/v1/functions/{function_id}/revisions"), &request)
        .await?
        .ok()?;
    let mut raw = resp.body_text();
    let mut revision: RevisionResponse = resp.json()?;
    p.note(format!(
        "revision {} (#{}) created: {}",
        revision.id, revision.number, revision.status
    ))?;

    let mut alias = None;
    if args.should_wait() {
        let (r, body) = wait_for_revision(
            client,
            &function_id,
            &revision.id,
            Duration::from_secs(args.wait_timeout),
            p,
        )
        .await?;
        revision = r;
        raw = body;
        if !args.no_publish {
            let a = fetch_alias_pointing_at(client, &function_id, "prod", &revision.id).await?;
            if a.revision_id != revision.id {
                p.note(format!(
                    "warning: alias prod still points at {} (generation {})",
                    a.revision_id, a.generation
                ))?;
            }
            alias = Some(a);
        }
    }

    if p.json {
        p.raw(&raw)?;
    } else {
        print_revision(&revision, p)?;
        if let Some(a) = &alias {
            p.line("alias:")?;
            print_alias(a, p)?;
        }
    }
    Ok(DeployOutcome {
        function_id,
        artifact,
        revision,
        alias,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::ArchArg;

    fn args() -> DeployArgs {
        DeployArgs {
            function: "hello".into(),
            binary: "/tmp/hello".into(),
            arch: ArchArg::Aarch64,
            memory_mib: Some(512),
            cpu_millis: None,
            ephemeral_storage_mib: None,
            timeout_seconds: Some(2),
            init_timeout_seconds: None,
            max_concurrency: Some(1),
            min_ready: None,
            idle_ttl_seconds: None,
            scale_down_cooldown_seconds: None,
            env: vec!["GREETING=v1".into()],
            secret: vec!["DEMO_SECRET=demo-secret".into()],
            description: "d".into(),
            egress: None,
            egress_allow: Vec::new(),
            region: None,
            restore_policy: None,
            synthetic_init_sample: false,
            no_publish: true,
            wait: true,
            no_wait: false,
            wait_timeout: 120,
        }
    }

    #[test]
    fn builds_request_from_args() {
        let r = build_revision_request(&args(), "sha256:ab").unwrap();
        assert_eq!(
            r.artifact,
            ArtifactRequest::Binary {
                digest: "sha256:ab".into()
            }
        );
        assert_eq!(r.architecture, "aarch64");
        assert_eq!(r.resources.memory_mib, 512);
        assert_eq!(r.resources.cpu_millis, 500);
        assert_eq!(r.resources.ephemeral_storage_mib, 256);
        assert_eq!(r.execution.timeout_seconds, 2);
        assert_eq!(r.execution.initialization_timeout_seconds, 30);
        assert_eq!(r.execution.max_concurrency, 1);
        assert_eq!(r.env_vars, vec![("GREETING".to_string(), "v1".to_string())]);
        assert_eq!(r.secrets[0].env_name, "DEMO_SECRET");
        assert_eq!(r.secrets[0].binding_ref, "demo-secret");
        assert!(!r.publish_to_prod);
        assert_eq!(r.egress, None);
    }

    #[test]
    fn egress_flags_become_the_profile_and_allowlist() {
        let mut a = args();
        a.egress = Some("restricted".into());
        a.egress_allow = vec!["1.1.1.1/32:443,80".into(), "udp:1.1.1.1:53".into()];
        let r = build_revision_request(&a, "sha256:ab").unwrap();
        assert_eq!(r.egress.as_deref(), Some("restricted"));
        assert_eq!(
            r.egress_allow,
            vec![
                EgressAllowRequest {
                    cidr: "1.1.1.1/32".into(),
                    protocol: None,
                    ports: vec![443, 80],
                },
                EgressAllowRequest {
                    cidr: "1.1.1.1".into(),
                    protocol: Some("udp".into()),
                    ports: vec![53],
                },
            ]
        );
        for bad in [
            "1.1.1.1/32",
            "icmp:1.1.1.1:1",
            "1.1.1.1:http",
            ":443",
            "1.1.1.1:",
        ] {
            let mut a = args();
            a.egress_allow = vec![bad.into()];
            assert!(
                matches!(
                    build_revision_request(&a, "sha256:ab"),
                    Err(CliError::Usage(_))
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn rejects_bad_env() {
        let mut a = args();
        a.env = vec!["NOVALUE".into()];
        assert!(matches!(
            build_revision_request(&a, "sha256:ab"),
            Err(CliError::Usage(_))
        ));
    }
}
