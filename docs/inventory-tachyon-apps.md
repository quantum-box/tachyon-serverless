# tachyon-apps 棚卸しと再利用マップ（PLT-4613）

- 対象: `quantum-box/tachyon-apps`（sibling checkout を read-only で参照。ビルド・実行はしていない）
- 参照 commit: `ae727f1f7e05d824d9752b2ec1f1b109c9c433df`
- 参照日: 2026-09-15
- 本リポジトリ（tachyon-serverless）は tachyon-apps に **compile-time 依存を持たない**。ルート `Cargo.toml` の `[workspace.dependencies]` に path / git 依存は無く、今後も追加しない。再利用は「設計・型名・不変条件の参照」または「将来 adapter を書くときの相手先の把握」に限る。
- 本書は「何があるか」を記録する。「動くか」は記録しない。tachyon-apps に Kata / Cloud Hypervisor の設定と manifest が存在することは、本リポジトリの `ExecutionProvider` が要求する能力（§5）が実機で成立する証拠ではない。tachyon-apps 自身の ADR-0025 も、Kata guest 内で seccomp が有効かは未確認であり、public-web egress policy は production 未導入と記録している。

## 1. 分類の定義

| 分類 | 意味 |
|---|---|
| reference | 設計・命名・不変条件を参考にする。コードは持ち込まない。 |
| adapter candidate | 本リポジトリの port（§2）の実装として、将来 tachyon-apps 側の機能に接続する候補。P1 では接続しない。 |
| 不要 | 本プロトタイプの範囲に無関係。読んだ上で除外した記録。 |

## 2. 本リポジトリの port と tachyon-apps の対応

| 本リポジトリの port | 役割 | tachyon-apps 側の対応物 | 分類 |
|---|---|---|---|
| `ExecutionProvider`（`crates/provider-port/src/execution.rs`） | 環境の create / observe / terminate、`Capabilities`、`preflight` | `packages/runner-controller`（Kubernetes Job adapter）、`packages/compute/domain/src/provider/container_runtime.rs`、`provider/lambda_runtime.rs` | adapter candidate（将来の Kata/k8s adapter の相手先）／reference |
| `ArtifactStore`（`artifact.rs`） | digest 指定の実行ファイル保存。content-addressed | `packages/compute/domain/src/supplemental_artifact.rs`、`provider/supplemental_artifact_object_reader.rs`、`build.rs`（Build 成果物） | reference |
| `SecretProvider`（`secret.rs`） | `binding_ref` → `SecretValue`。値は HelloAck の env にだけ載る | `packages/secrets`（`SecretsRepository`、`SecretPath`、`SecretValue`） | adapter candidate |
| `IdentityProvider`（`identity.rs`） | Bearer → `Principal{tenant_id, roles}` | `packages/auth`（`Executor`、`ServiceAccount`、`WorkloadIdentity`、`api_key.rs`、`access_token.rs`）、`platform/foundation/value_object`（`TenantId`） | adapter candidate |
| `UsageSink`（`usage.rs`） | host 観測の `UsageEvent` 受け取り（`event_id` で重複排除） | `packages/compute/domain/src/cloud_app_billing.rs`、`cloud_app_build_charge.rs`、`packages/audit` | reference（課金は非対象） |
| repositories（`crates/application`。P1 は in-memory + `state.json`） | Function / Revision / Alias / Invocation / Environment の永続化 | `packages/compute/domain/src/repository.rs`、`job_run.rs::JobRunRepository`、`packages/compute/migrations/` | reference（TiDB 永続化は P1 非対象） |

## 3. 領域別の棚卸し

各行の「確認したパス」は commit `ae727f1f7` で実際に開いたファイルである。関数名・型名は同 commit 時点のもの。

### 3.1 `packages/compute/domain` — ID 規約と CloudApp / Build / Deployment / RunnerPool / JobRun

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/compute/domain/src/lib.rs`（prefix 一覧表）、`value_objects.rs`（`string_value_object!` / `secret_string_value_object!`）、`cloud_app.rs`、`build.rs`、`deployment.rs`、`runner.rs`、`job_run.rs`、`job_run_cancellation.rs`、`provider_failure.rs`、`environment.rs` |
| ID 規約 | `<prefix>_<26 文字 lowercase ULID>`。prefix は永続化される namespace token で、既存 prefix は決して rename しない。既存 prefix: `jr_` JobRunId、`rp_` RunnerPoolId、`rn_` RunnerId、`app_` CloudAppId、`bld_` BuildId、`dep_` DeploymentId、`env_` EnvironmentVariableId、`cbr_` CloudAppBillingRecordId ほか。`TenantId`（`tn_`）は `platform/foundation/value_object` が定義する |
| CloudApp | `CloudAppId`、`DeploymentTarget{CloudRun, CloudflarePages, Lambda, CloudflareWorkers}`、`CloudAppStatus`。アプリ＝長期稼働サービスであり、本リポジトリの `Function`（関数 + 不変 Revision + alias）とは粒度が異なる |
| Build / Deployment | `BuildStatus`、`BuildTransition`、`DeploymentStatus`、`DeploymentTargetMetadata`。「ビルド → デプロイ」の 2 段。本リポジトリは build を持たず、`POST /v1/artifacts` で完成バイナリを受け取って Revision に固定する |
| RunnerPool / Runner | `TrustLevel{OwnedBaremetal, Partner, ManagedCloud, Untrusted}`（weight 順序付き）、`CostClass`、`ResourceSource`（`#[non_exhaustive]`。doc comment に将来案として Firecracker が挙がるだけで variant は無い） |
| JobRun | `JobRunStatus{Pending, Assigned, Running, Succeeded, Failed, Canceled}`、`ContainerWorkloadSpec{image, command, args, env, volumes, resources: Value, security_context, priority_class_name, timeout_seconds, runtime: Value, ...}`、`JobRun{tenant_id, pool_id, runner_id, workload, result, error_message, claimed_at, started_at, finished_at}`、`JobRunRepository::reap_timed_out_job_runs` → `ReapTimedOutOutcome`、`JobRunCancellation::cancel_by_id` → `JobRunCancellationOutcome{Cancelled, AlreadyTerminal, NotFound}` |
| 失敗分類 | `ProviderFailureCategory{InfrastructureFailure, BuildFailure, UserConfigurationError, Timeout, Cancelled, Unknown}` + `is_retryable()`。本リポジトリの `ErrorClass`（`crates/domain/src/invocation.rs`）はこれより細かく、`OutcomeUnknown` を「自動再実行しない terminal」として別立てにしている |
| 分類 | **reference** |
| 対応する port / 型 | `crates/domain/src/ids.rs`（同じ `<prefix>_<ulid>` 規約。`TenantId` は `tn_` を再利用し、本リポジトリは tenant を発行しない）、`ExecutionEnvironment` / `InvocationAttempt` ≈ JobRun、`ErrorClass` ≈ `ProviderFailureCategory` |
| 注意 | **prefix 衝突**: tachyon-apps の `env_` は `EnvironmentVariableId`、本リポジトリの `env_` は `ExecutionEnvironment`（`crates/domain/src/ids.rs`）。両者は別ストアなので P1 では問題にならないが、同じ DB / ログ基盤に流す前に必ず解消する（本リポジトリ側はまだ永続データが無い）。決定は PLT-4618 の担当に委ねる |

### 3.2 `packages/compute` scheduler

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/compute/domain/src/scheduler.rs`（`SchedulerInput{tenant_id, required_capabilities, preferred_cost_class, trust_level_min}`、`SchedulerOutput`、`SchedulingDecision{candidate_pool_count, pool_scores, tiebreaker_applied}`、`PoolScore`） |
| 内容 | RunnerPool を trust / cost / capability / load でスコアリングし、決定内容を JSON で `job_runs.scheduling_decision` に残す |
| 分類 | **reference**（コードは不要） |
| 対応 | 本リポジトリの P1 は単一 host + semaphore（`docs/architecture.md` §3 手順 5）で pool 選択は無い。「決定の根拠を record に残す」パターンは `AttemptResponse.boot_evidence`（`crates/api-types`）と同じ考え方 |

### 3.3 `packages/runner-controller` — Kubernetes Job adapter と `runtime_class_name`

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/runner-controller/src/kubernetes.rs`（`KubernetesClient::create_job(.., runtime_class_name, ..)`、`kubernetes_job_manifest`、`effective_runtime_class_name`、`wait_for_job`、`read_pod_observation`、`delete_job`、`read_job_logs`、`jobrun_egress_*_network_policy`）、`src/network_policy_gate.rs`、`src/scheduling.rs`、`src/lib.rs`（`ControllerConfig{runtime_class_name, ..}`、workload contract version 1..=5）、`src/tachyon.rs`（`TachyonClient`）、`src/terminal_outbox.rs` |
| Job manifest の要点 | `backoffLimit: 0`、`ttlSecondsAfterFinished: 300`、`restartPolicy: Never`、`runtimeClassName` は workload の `runtime.runtime_class_name` が空でなければそれ、空なら controller 設定（テストの既定値は `kata`）。`activeDeadlineSeconds = workload.timeout_seconds`。Pod / container に `RuntimeDefault` seccomp を宣言、ServiceAccount token を automount しない |
| host 側 timeout | `activeDeadlineSeconds`（Kubernetes Job controller が秒粒度で enforce）と `wait_for_job` の 2 秒 poll。結果は container termination message ≤ 4 KiB（`CLOUD_APP_BUILD_TERMINAL_RESULT_BYTES`）で回収 |
| network | default-deny egress NetworkPolicy + DNS / kube-apiserver / public-https の allowlist。ADR-0030 の enforcement gate（sentinel の allow port 18080 が応答し deny port 18081 が拒否されるまで workload を起動しない） |
| 容量 | `scheduling.rs::PodFootprint::with_overhead` が RuntimeClass `overhead.podFixed` を加算して空きスロットを算出 |
| 終端通知 | `TachyonClient::complete/fail` は 409 を冪等成功として扱う。terminal callback 前に ConfigMap outbox へ永続化 |
| 分類 | **adapter candidate**（将来 `providers/kata` を書く場合の相手先）／**reference**（冪等な終端、host 側 deadline、token 非配布） |
| 対応する port | `ExecutionProvider`（create ≈ `create_job`、observe ≈ `read_pod_observation`、terminate ≈ `delete_job`、`list_environments` ≈ `list_runner_jobs_for_adoption`） |
| P1 で使わない理由 | (1) 本リポジトリの protocol は guest bridge との双方向 stream（vsock / unix socket）を前提とし、Job の termination message（4 KiB）では成立しない。(2) deadline が秒粒度かつ controller 経由で、`docs/architecture.md` §3 の host 直接 terminate と粒度が合わない。(3) Kubernetes・CNI・RuntimeClass・namespace quota への依存が、独立リポジトリで縦断を通す目的と矛盾する |

### 3.4 `packages/agent` coding_job runners（`containerized_codex`、`container_spec`）

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/agent/src/coding_job/runners/containerized_codex.rs`（`ContainerizedCodexRunner`、`docker_run_args`、`ensure_docker_supported`、`container_timeout_secs`）、`runners/container_spec.rs`（`ContainerWorkloadSpec{image, command, env, mounts, network: ContainerNetworkPolicy, working_dir, resources: ContainerResourceLimits{memory, cpus, pids_limit}, user}`、`CodexAppServerContainerSpecBuilder`）、`runner.rs`（`CodingRunner` trait）、`redaction.rs` |
| 内容 | `docker run --rm --network=none`（既定）`--memory` `--cpus` `--pids-limit` でコンテナを起動し、`tachyond.managed=true` ラベルで cleanup 対象を識別する。Docker が無い host では `ensure_docker_supported` で明示的に失敗する |
| 分類 | **reference** |
| 対応 | 本リポジトリの process provider（`docs/adr/0002-process-provider-dev-only.md`）と同じ「VM なし runner」の位置づけ。ただし process provider は Docker すら使わず、network none・memory 上限のいずれも持たない。`Capabilities.egress_none` を「設定した」ではなく「検証した」で判定する根拠として、network none を既定にする設計を参照する |

### 3.5 `cluster/hetzner-k3s/kata` — Kata + Cloud Hypervisor bootstrap

| 項目 | 内容 |
|---|---|
| 確認したパス | `cluster/hetzner-k3s/kata/README.md`、`helm/kata-deploy-values.yaml`、`manifests/runtimeclass-kata.yaml`、`manifests/kata-smoke-job.yaml`、`manifests/runner-baseline.yaml`、`manifests/runner-log-reader-rbac.yaml`、`scripts/install-kata-k3s-ubuntu.sh`、`deploy-runbook.md`（内部ネットワークの手順。本書では内容を引用しない） |
| 内容 | 推奨は upstream `kata-deploy` Helm chart（`3.29.0`、shim は `clh` のみ有効、`defaultShim.amd64: clh`）。RuntimeClass `kata` → handler `kata-clh`、`overhead.podFixed: cpu 250m / memory 256Mi`、`nodeSelector: tachyon.dev/runtime-kata=true`。break-glass の shell script は `/dev/kvm` の存在を確認し、`KATA_HYPERVISOR=clh` で containerd runtime `kata-clh`（`io.containerd.kata.v2`）を書く。smoke job は busybox で `uname -a` を出力（`activeDeadlineSeconds: 120`）。namespace `tachyon-runners` に ResourceQuota（pods 20 など）、LimitRange、`automountServiceAccountToken: false` の ServiceAccount |
| 分類 | **reference**（将来の Kata/CH adapter の前提把握）。P1 では **不要**（cluster に触れない） |
| 対応 | `ExecutionProvider` の将来 adapter。本リポジトリの Firecracker provider は cluster を使わず、KVM host 1 台で完結する（`docs/adr/0001-execution-provider-firecracker-first.md`） |
| 注意 | 設定と manifest の存在は能力の証拠ではない。smoke job は「Kata で Pod が起動した」ことしか示さず、§5 の各能力（deadline の実測、egress の遮断、ephemeral storage の上限、pause / snapshot）を示さない。ADR-0025 自身も seccomp の実効性を未確認と記録している |

### 3.6 `packages/secrets`

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/secrets/src/lib.rs`、`src/interface_adapter/repository.rs`（`SecretsRepository::{get_by_path, get_by_key, save, save_with_options, delete, delete_scoped, exists, list, list_bounded}`）、`src/domain/secret_path.rs`（`{tenant_id}/providers/{provider_type}` と global）、`src/domain/secret_value.rs`（JSON 値、`Debug` は `SecretValue(***)`）、`src/interface_adapter/gateway/{local_file, aws_secrets_manager, ssm_parameter_store, vault, cached, fallback}.rs`、`docs/src/architecture/decisions/ADR-0024-secrets-backend-ssm-parameter-store.md`（ファイル名のみ確認） |
| 分類 | **adapter candidate**（`SecretProvider`） |
| 対応 | `binding_ref` ↔ `SecretPath`（tenant scoped）。差分: tachyon-apps の `SecretValue` は JSON（複数 field）、本リポジトリの `SecretValue`（`crates/provider-port/src/secret.rs`）は env 値 1 本の文字列。adapter は `binding_ref` に path と field の両方を符号化する必要がある。P1 は `config/gateway.{dev,firecracker}.toml` の `[[secrets.bindings]]` による静的解決（`docs/architecture.md` §4） |
| 共通の不変条件 | `Debug` 出力を redact する、値をログに出さない。本リポジトリはさらに「値は `HelloAck.env` にだけ載せ、API 応答・`state.json` に書かない」（`docs/architecture.md` §5-2） |

### 3.7 `packages/auth`（`TenantId` を含む）

| 項目 | 内容 |
|---|---|
| 確認したパス | `platform/foundation/value_object/src/lib.rs`（`def_id!(TenantId, "tn_")`、`PlatformId` / `OperatorId` = `TenantId`）、`platform/foundation/util/src/macros/id.rs`（`def_id!`: prefix 検査と lowercase 化）、`packages/auth/domain/src/executor.rs`（`Executor{SystemUser, User, ServiceAccount, WorkloadIdentity{service_account, allowed_actions}, None}`）、`workload_identity.rs`（`wid_`）、`service_account.rs`、`api_key.rs`、`access_token.rs`、`policy.rs`、`policy_statement.rs`、`trn.rs`（`trn:<service>:<resource-type>:<resource-id>`）、`execution_mode.rs`（`ExecutionMode{Production, Sandbox}`、`SandboxRestriction`）、`multi_tenancy.rs` |
| 分類 | **adapter candidate**（`IdentityProvider`）／**reference**（`TenantId` 形式） |
| 対応 | 本リポジトリの `TenantId` は同じ `tn_` namespace を再利用し tenant を発行しない（`crates/domain/src/ids.rs`、test `existing_tenant_ids_parse`）。`Role{Deploy, Invoke, Operator}`（`crates/provider-port/src/identity.rs`）は placeholder で、将来は IAM の action / policy statement へ写像する。P1 は `config/gateway.{dev,firecracker}.toml` の静的 token（`[[identity.tokens]]`） |
| 差分 | tachyon-apps の `def_id!` は prefix と小文字化のみ、本リポジトリは prefix + 26 文字 Crockford lowercase を検査する。既存の実 ID（例 `tn_01hjjn348rn3t49zz6hvmfq67p`）は両方で valid |

### 3.8 `packages/audit`

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/audit/src/event.rs`（`AuditEvent{id, tenant_id, actor: AuditActor{kind, id, display, ip, user_agent}, action, resource: AuditResource{resource_type, resource_id, name}, result: AuditResult{Allow, Deny, Success, Failure}, reason, request_id, correlation_id, permission_decision, diff, metadata, occurred_at, created_at}`、`redact_json_value`）、`src/repository.rs`（`insert`、`insert_idempotent`、`list`）、`src/logger.rs`、`src/retention.rs` |
| 分類 | **reference**（P1 に audit log は無い）。将来は `UsageSink` と並ぶ sink 候補 |
| 対応 | `UsageEvent.event_id` による重複排除（`crates/domain/src/usage.rs`）は `insert_idempotent` と同じ要求。`redact_json_value` の対象キー（password / secret / token / authorization / cookie ...）は、本リポジトリで `BootEvidence.details` や log 行に secret を載せない規則の参考 |

### 3.9 docs ADR-0025（Kata 上の coding job）と関連 ADR

| 項目 | 内容 |
|---|---|
| 確認したパス | `docs/src/architecture/decisions/ADR-0025-kata-codex-coding-job-sessions.md`（全文）、`ADR-0030-jobrun-network-policy-enforcement-gate.md`（冒頭）、`ADR-0032-rootless-oci-build-on-non-privileged-kata.md`（冒頭）。`ADR-0031-cloud-app-builder-sandbox-contract.md`、`ADR-0052-kata-coding-session-websocket-control-plane.md`、`ADR-0053-codex-app-server-bridge-consolidation.md`、`docs/src/adr/0004-runner-registry.md`（`ResourceSource::Firecracker` は将来案の記述のみ）はファイル名と該当行のみ確認 |
| ADR-0025 の要点 | 1 turn = 1 Kata JobRun（破棄前提、再開は新 microVM）。RuntimeClass `kata` → `kata-clh`。ServiceAccount token を渡さない。UID/GID 1000、全 capability drop、`RuntimeDefault` seccomp を宣言するが **guest 内で有効かは未確認**（rollout gate 扱い）。credential は trusted init にだけ mount し main workload の env に載せない。NetworkPolicy enforcement gate 通過後に workload を起動。microVM 内は stdio（loopback WebSocket を廃止）。public-web egress policy は isolated 環境で probe 済みだが production 未導入 |
| 分類 | **reference** |
| 対応 | 本リポジトリの決め事と一致する点: destroy-after-invoke、secret を HelloAck だけに載せる、host 側 deadline、preflight と明示的 `Unsupported`（`Support::Unsupported{reason}`）。ADR-0031/0032 の「既知の不成立構成は実行前に理由付きで拒否する」は `ExecutionProvider::validate_artifact` / `preflight` の設計根拠 |

### 3.10 `docs/plt-478-edge-functions-design.md`

| 項目 | 内容 |
|---|---|
| 確認したパス | `docs/plt-478-edge-functions-design.md`（全 293 行） |
| 内容 | Cloudflare Workers を `DeploymentTarget` に追加する設計案。Workers の制約表（CPU time / memory 128 MB / script size / subrequests）、案比較（plan-aware validation、CodeBuild + buildspec、CloudWatch logs）、`RuntimeLogProvider` を `stream()` と `query()` に分ける方針 |
| 分類 | **reference**（関数型ワークロードの上限表と log provider の形）。コードは **不要**（Cloudflare は本リポジトリの provider ではない） |
| 対応 | `crates/domain/src/limits.rs::Limits` の項目立て、`GET /v1/invocations/{id}/logs`（`crates/api-types`）の `LogsResponse.dropped` |

### 3.11 `packages/compute/domain` cloud_app_billing

| 項目 | 内容 |
|---|---|
| 確認したパス | `packages/compute/domain/src/cloud_app_billing.rs`（`BillingPeriod`（`YYYY-MM`）、`CloudAppBuildUsage`、`CloudAppBillingPolicy{free_tier_minutes, unit_price_milli_yen, metering_version}`、`CloudAppBillingRecord`、`CloudAppBillingRecordId` `cbr_`）、`cloud_app_build_charge.rs`、`cost_allocation.rs`、`codebuild_cost.rs`（ファイル名のみ） |
| 分類 | **reference**（`metering_version` を record に持つ点）。課金そのものは **不要** |
| 対応 | `UsageEvent.meter_version` と `EvidenceQuality`（`crates/domain/src/usage.rs`）。`UsageSummaryResponse.not_billable` は常に true（`crates/api-types`）。P1 は使用量の事実だけを host 観測で記録し、料金には変換しない |

### 3.12 読んだ上で除外したもの（不要）

| 領域 | 理由 |
|---|---|
| `packages/compute/domain/src/provider/{build_provider, pages_provider, workers_provider, d1_provider, kv_provider, blob_provider, dns_provider, custom_hostname_provider, database_provider, log_drain_provider, gar_credential_issuer, ...}.rs` | Cloud Run / Lambda / Cloudflare / CodeBuild / DNS 等の外部 SaaS 連携。本リポジトリの provider は KVM host 上の microVM のみ |
| `packages/compute/domain/src/{cron_job, custom_domain, deploy_hook, app_transfer, template_catalog, framework_detection, wrangler_config, ...}.rs` | Cloud Apps 製品機能。P1 非対象（cron / Console UI は `docs/architecture.md` §6） |
| `packages/{iac, payment, pricing, onboarding, notification, ...}` | プロトタイプの範囲外 |
| `cluster/{gke-autopilot, n1-aws, n1-aws-bootstrap, intranet, developer-app, ...}` | 既存 cluster / ネットワーク運用。独立リポジトリで縦断を通す目的に無関係。内部ホスト名は本リポジトリに持ち込まない |
| `packages/runner-controller/src/terminal_outbox.rs`、session PVC GC | Kubernetes 固有の耐久化。P1 の gateway は単一プロセスで、Invocation ledger を `state.json` に持つ |

## 4. 再利用しないと決めた点（PLT-4616 への入力）

1. **Kubernetes 経由の実行**（runner-controller / Kata RuntimeClass）は P1 の provider にしない。理由は §3.3。Kata / Cloud Hypervisor は `ExecutionProvider` の後続 adapter として扱う。
2. **JobRun の結果回収経路**（termination message、kubernetes logs）は使わない。本リポジトリは bridge との stream（`crates/protocol`）で result / log を回収する。
3. **secrets / auth / audit の実装**は持ち込まない。port の adapter として後で接続できるよう、port 側の型（`SecretDeliveryContext`、`Principal`、`UsageEvent`）を tachyon-apps の型に写像可能な粒度に留める。
4. **課金**は持ち込まない。`UsageEvent` は host 観測の事実に限る。

## 5. 能力表（create/terminate、deadline、network、ephemeral storage、pause/resume、snapshot/restore）

判定語の定義:

- `verified`: 本リポジトリのスクリプト / テストにより実機（Linux/KVM）で確認し、証跡が `docs/evidence/` にある。§6 の baseline profile を満たさない記録で確認した場合は、そのことをセルに書く。
- `unsupported`: その構成では機能を露出しない、または P1 の `Capabilities` で `Unsupported{reason}` を返すと決めている。
- `unverified`: upstream に機能や設定は存在するが、本リポジトリでは測定していない。

**2026-09-16 時点の実機記録は Firecracker の 2 件だけで、どちらも §6 の baseline profile を満たさない。**

- `docs/evidence/kvm-20260915T080221Z/`（`scripts/kvm/smoke.sh`。gateway を通さず hello と timeout demo を 1 回ずつ）
- `docs/evidence/20260915T125610Z-firecracker/`（`TSLS_PROVIDER=firecracker scripts/e2e/demo.sh`。27/27 PASS）
- 条件: Apple M4 上の Lima VM（`vmType: vz`、nested virtualization）、Linux 7.0.0-28-generic aarch64、Firecracker v1.17.0、guest kernel は Firecracker CI の `vmlinux-6.1.155`（`CI_VERSION=v1.15 GUEST_KERNEL_SERIES=6.1`、`docs/kvm.md` §5）、1 vCPU / 256 MiB、1 host で 1 回ずつ
- nested virtualization のオーバーヘッドを含み、x86_64 でも N ≥ 20 でもない。時間の値は代表値として使わない（`docs/kvm.md` §5.5）

したがって Firecracker 列の `verified` は「上の条件で動作を確認した」の意味に留まり、性能の値を含まない。Cloud Hypervisor と Kata は本リポジトリで実行していない。tachyon-apps の Kata 設定は測定の代わりにならない。

| 能力 | Firecracker（prototype provider） | Cloud Hypervisor | Kata Containers（k3s、`kata-clh`） |
|---|---|---|---|
| create / terminate | verified（上記の条件。baseline profile 外）。`docs/evidence/kvm-20260915T080221Z/hello.json`: 起動して `outcome=response`、`terminate.was_running=true`、terminate 後の observe が `not_found`、`leftovers` なし（process・env dir・socket が残らない）。E2E step 19・27（`scripts/e2e/orphan-check.sh`）が `orphan-check: clean (firecracker)`。2 回目の terminate（冪等性）は未測定 | unverified（REST API / `ch-remote`。未実行） | unverified（`kata-smoke-job.yaml` は存在するが本リポジトリでは実行していない。Job 削除 → shim 終了の所要時間も未測定） |
| deadline（host 強制） | verified（上記の条件。baseline profile 外）。VMM 機能ではなく provider が VMM を kill する。`docs/evidence/kvm-20260915T080221Z/timeout.json`: `outcome=timeout`、`cancel_sent=true`、`terminate.was_running=true`、`leftovers` なし。E2E step 18（`timeout_seconds = 2`、504 `Host.Timeout`）。kill → cleanup 完了までの時間の単独測定（ADR-0001 M5 の条件）は無い | unverified（同上） | unverified（`activeDeadlineSeconds` は秒粒度。controller 経由の遅延を測っていない） |
| network（egress none / restricted） | unverified（tap を作らなければ guest に NIC が無い。「本当に到達できない」ことを guest から測る。上記の実機記録は NIC なしで動いたことだけを示し、到達不能は測っていない） | unverified（同上） | unverified（default-deny NetworkPolicy + ADR-0030 gate は存在。ADR-0025 は public-web policy を production 未導入と記録） |
| ephemeral storage 上限 | unverified（P1 は `/tmp` tmpfs。サイズ上限の指定と超過時の挙動を測る） | unverified | unverified（`ephemeral-storage` request/limit を宣言。ADR-0025 は `local-path` driver が claim ごとの bytes を強制しないと記録） |
| pause / resume | unverified（upstream は `PATCH /vm {state: Paused/Resumed}` を提供）。P1 の `Capabilities.idle_quiesce/idle_resume` は `Unsupported` | unverified（upstream は `vm.pause` / `vm.resume` を提供） | unsupported（Kubernetes Pod API に pause は無く、RuntimeClass 経由で露出しない） |
| snapshot / restore | unverified（upstream は `PUT /snapshot/create` / `PUT /snapshot/load` を提供。vsock / network / エントロピーに関する制約が upstream doc に記載）。P1 の `Capabilities.snapshot_create/snapshot_clone` は `Unsupported` | unverified（upstream は snapshot / restore を提供） | unsupported（RuntimeClass 経由で露出しない） |

P1 の `Capabilities`（`crates/provider-port/src/execution.rs`）では、Firecracker provider であっても `idle_quiesce` / `idle_resume` / `snapshot_create` / `snapshot_clone` / `egress_restricted` / `egress_public_web` を `Unsupported` として返す（`docs/architecture.md` §1, §6）。上表の `unverified` は「後続で測定可能」の意味であり、P1 で提供する意味ではない。Firecracker provider の実装は `create_terminate` / `observe` / `enforce_deadline` を `Supported` で返す。根拠は本表の verified 記録（aarch64 の nested virtualization）で、x86_64 と bare metal は未測定。`egress_none` は NIC を構成しないが guest からの到達不能を測っていない（ADR-0001 M8）ため `Unverified`、`host_metering` / `enforce_resource_limits` も `Unverified` で返す。このうち上表で `verified` にしたのは create / terminate と deadline だけで、どちらも baseline profile 外の記録である。

## 6. Baseline KVM 測定プロファイル

PLT-4615（検証環境）と PLT-4621（Firecracker provider）はこのプロファイルで測定する。生データは、`scripts/kvm/smoke.sh` が `docs/evidence/kvm-<UTC>/` に、`scripts/e2e/demo.sh` が `docs/evidence/<UTC>-<provider>/` に書く。プロファイルを変えた測定は別 evidence として区別し、変えた条件（host arch、nested virtualization の有無、kernel / rootfs の digest）を読み取れるようにする。

| 項目 | 値 | 根拠 |
|---|---|---|
| host arch | x86_64 を第一。aarch64 は第二（kernel cmdline に `keep_bootcon` を追加） | `docs/protocol.md` §C |
| host 要件 | Linux、`/dev/kvm` が存在し gateway 実行ユーザーが read/write 可能。nested virtualization の場合はその旨を evidence に記録 | `ExecutionProvider::preflight` |
| vCPU | 1 | `ResourceProfile::default().cpu_millis = 500` → `vcpus() = 1`（`crates/domain/src/revision.rs`） |
| memory | 256 MiB | `ResourceProfile::default().memory_mib` |
| kernel | Firecracker CI kernel v1.17 系列の `vmlinux`（upstream getting-started の取得手順に従う）。ファイルの SHA-256 を evidence に記録 | PLT-4615 |
| rootfs | minimal ext4、read-only。`/sbin/tachyon-init` = runtime-bridge の static musl バイナリ。空ディレクトリ `/proc /sys /dev /tmp /function`。SHA-256 を evidence に記録 | `docs/protocol.md` §C |
| function drive | 環境ごとに `mkfs.ext4 -d` で生成、read-only、`/app` = artifact | `docs/protocol.md` §C |
| network | なし（tap を作らない、egress none） | `docs/architecture.md` §6 |
| vsock | guest CID 3 → host port 5000（`<uds_path>_5000`） | `crates/protocol::DEFAULT_VSOCK_PORT` |
| 測定点 | `AttemptTimings`（host 計測）: `environment_boot_ms`（create 開始 → bridge 接続）、`runtime_init_ms`（接続 → Ready）、`handler_ms`、`total_ms`。加えて terminate 開始 → `TerminateReport` 返却までの ms、terminate 後の orphan（process / socket / drive / workdir）件数 = 0 | `crates/domain/src/invocation.rs::AttemptTimings`、`crates/provider-port::TerminateReport` |
| 試行 | cold start のみ（P1 は warm 無し）。N ≥ 20、中央値と p95 を記録。hello / http-axum / cpu-burn（timeout kill）の 3 サンプル | PLT-4630 |
| evidence に含めるもの | `uname -a`、`firecracker --version`、kernel / rootfs の digest、`GET /v1/provider` の出力（`capabilities` と `preflight`）、各 attempt の `InvocationResponse` JSON | `crates/api-types::ProviderInfo` |

本表は測定条件であって結果ではない。本表どおりの測定（x86_64、N ≥ 20、中央値・p95、3 サンプル）はまだ行っていない。条件の異なる記録として `docs/evidence/kvm-20260915T080221Z/`（smoke 2 回）と `docs/evidence/20260915T125610Z-firecracker/`（E2E 1 回、11 attempt）がある。本表との違いは host arch（aarch64）、nested virtualization（Apple M4 上の Lima vz）、kernel（Firecracker CI の `CI_VERSION=v1.15` の `vmlinux-6.1.155`。本表の「v1.17 系列」ではない）、試行回数。vCPU 1 / memory 256 MiB は本表と同じ（`hello.json` の `evidence.details`）。時間の値は `docs/kvm.md` §5.5 の参考値で、代表値として使わない。

## 7. 未解決事項

| # | 内容 | 担当 |
|---|---|---|
| 1 | `env_` prefix の衝突（§3.1 注意） | PLT-4618 |
| 2 | `SecretProvider` adapter で `binding_ref` に path + field をどう符号化するか（§3.6） | PLT-4623 以降 |
| 3 | Kata / Cloud Hypervisor adapter を書く場合の bridge transport（vsock が Kubernetes 経由で使えるか） | PLT-4616 の「残る測定」 |
| 4 | 解消済み: `ArtifactStore` port は tenant を持たないままだが、`POST /v1/artifacts` が `(tenant_id, digest)` の所有を `state.json`（`artifact_owners`）に記録し、revision は自 tenant が upload した digest しか参照できない（他 tenant だけが upload した digest は存在しない digest と同じ扱い）。実装は `crates/application/src/services/artifact.rs`（`ArtifactService::upload`、`owned_artifact`）。`docs/threat-model.md` §14-1 | PLT-4620 |
