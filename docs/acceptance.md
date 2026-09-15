# 受入チェックリスト（PLT-4613〜PLT-4630）

- 対象: Linear プロジェクト「Tachyon Serverless — 動作プロトタイプ」P0〜P1
- 基準: `docs/architecture.md`、`docs/protocol.md`、`docs/threat-model.md`、`docs/adr/`
- 状態の記録日: 2026-09-15（commit `66a277b` の契約 crate と本トラックの docs を基準にした）

## 状態の定義

| 状態 | 意味 |
|---|---|
| 実装済み | 本リポジトリに証跡（テスト名・ファイル・スクリプト）があり、指定コマンドで再現できる |
| 未検証 | コードまたは文書はあるが、実機 / 自動テストで確認していない。証跡パスは「期待する場所」 |
| 別トラックで実装中 | 本チェックリスト作成時点で他トラックが担当し、成果物がこの worktree からは見えない。統合時に状態を更新する |
| 未着手 | 誰も着手していない、または P1 で非対象と決めた |

証跡の置き場所の規約:

- 単体テスト: `cargo test -p <crate> <test_name>` で再現できる `<crate>::<module>::<test_name>`
- 統合 / E2E: `scripts/` 以下のスクリプト名、または `tests/` 以下のテスト名
- 実機測定: `docs/evidence/<plt-id>/` 以下（生 JSON、コマンド出力、digest）。`docs/inventory-tachyon-apps.md` §6 の profile を明記する
- 文書: `docs/` 以下のパスと節番号

更新ルール: 各トラックの PR で該当行の状態と証跡パスを更新する。状態を「実装済み」にするときは証跡パスが実在することを PR で示す。

---

## PLT-4613 既存 Kata・compute・runner の実機棚卸しと再利用マップ

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | tachyon-apps の該当領域を reference / adapter candidate / 不要 に分類し、確認したファイルパスと commit を記録している | 実装済み | `docs/inventory-tachyon-apps.md` §3（commit `ae727f1f7`） |
| 2 | 本リポジトリの各 port（ExecutionProvider / ArtifactStore / SecretProvider / IdentityProvider / UsageSink / repositories）への対応を示している | 実装済み | `docs/inventory-tachyon-apps.md` §2 |
| 3 | 本リポジトリが tachyon-apps に compile-time 依存しないことを明記し、ワークスペースにも依存が無い | 実装済み | `docs/inventory-tachyon-apps.md` 冒頭、ルート `Cargo.toml`（path / git 依存なし）、`crates/domain/src/boundary.rs::domain_has_no_framework_dependencies` |
| 4 | 「既存の Kata / CH 設定は runtime 能力の証拠ではない」と明記している | 実装済み | `docs/inventory-tachyon-apps.md` 冒頭、§3.5 |
| 5 | 能力表（create/terminate、deadline、network、ephemeral storage、pause/resume、snapshot/restore × Firecracker / CH / Kata）があり、未測定項目は `unverified` | 実装済み（値はすべて unverified） | `docs/inventory-tachyon-apps.md` §5 |
| 6 | baseline KVM 測定プロファイル（host arch、vCPU、memory、kernel、rootfs）を定義している | 実装済み | `docs/inventory-tachyon-apps.md` §6 |
| 7 | 実機での測定 | 未着手（P0 では測定しない） | `docs/evidence/plt-4621/` に置く予定 |

## PLT-4614 プロトタイプの API・実行契約・脅威モデルを固定

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 信頼境界・資産・前提・原則（guest 自己申告を根拠にしない）が文書化されている | 実装済み | `docs/threat-model.md` §3〜§6 |
| 2 | 2 tenant + operator のアクセス制御マトリクスと期待 HTTP 結果（他 tenant は 404） | 実装済み（文書） / 未検証（テスト） | `docs/threat-model.md` §7。期待するテスト: `apps/gateway` の `tenant_isolation_returns_404_for_foreign_resources`（PLT-4619） |
| 3 | 失敗分類 `ErrorClass` と HTTP `ErrorCode` の対応が固定されている | 実装済み | `crates/domain/src/invocation.rs::ErrorClass`、`crates/api-types::tests::error_codes_map_to_status` |
| 4 | deadline モデル（queue / init / execution / client）が定義されている | 実装済み（文書・domain 型） / 別トラックで実装中（enforcement） | `docs/threat-model.md` §8、`crates/domain::Deadlines`、`crates/domain::invocation::tests::rejects_elapsed_client_deadline_and_bad_key`。enforcement は PLT-4627 |
| 5 | `OutcomeUnknown` の意味（Running からのみ、自動再実行しない）が固定されている | 実装済み | `docs/threat-model.md` §9、`crates/domain::invocation::tests::outcome_unknown_only_after_running` |
| 6 | Idempotency-Key の意味（scope、一致 / 不一致、保持）が固定されている | 実装済み（文書、key 長の検証） / 別トラックで実装中（gateway） | `docs/threat-model.md` §10、`rejects_elapsed_client_deadline_and_bad_key`。gateway は PLT-4626 |
| 7 | payload と資源の上限が `Limits` と一致している | 実装済み | `docs/threat-model.md` §11、`crates/domain/src/limits.rs` |
| 8 | process provider が守らないもの・非目標が明記されている | 実装済み | `docs/threat-model.md` §13, §15、`docs/adr/0002-process-provider-dev-only.md` |
| 9 | host ↔ bridge ↔ SDK の protocol が固定されている | 実装済み | `docs/protocol.md`、`crates/protocol::wire::tests::{frames_roundtrip_over_duplex, oversized_frame_rejected, unknown_fields_are_ignored_but_unknown_types_fail}`、`crates/protocol::runtime_api::tests::http_event_roundtrip` |

## PLT-4615 専用 KVM 検証環境と bootstrap・teardown

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | KVM host の要件（`/dev/kvm`、arch、nested virt の記録）が文書化されている | 実装済み（profile） / 別トラックで実装中（`docs/kvm.md`） | `docs/inventory-tachyon-apps.md` §6、`docs/kvm.md` |
| 2 | bootstrap スクリプトで `firecracker` バイナリ・kernel（CI kernel v1.17 系列）・rootfs を取得し、digest を検証する | 別トラックで実装中 | `scripts/kvm/bootstrap.sh`（期待）、`docs/evidence/plt-4615/digests.txt` |
| 3 | teardown スクリプトで環境・ソケット・drive・workdir を全削除し、orphan 0 を確認する | 別トラックで実装中 | `scripts/kvm/teardown.sh`（期待） |
| 4 | `ExecutionProvider::preflight` が host 要件を検査し、`GET /readyz` に反映する | 実装済み（port 定義） / 別トラックで実装中（実装） | `crates/provider-port/src/execution.rs::PreflightReport`、PLT-4621 |
| 5 | 本番 cluster・remote に触れない | 実装済み（規則） | `docs/inventory-tachyon-apps.md` §3.12、`docs/adr/0001` §「決定」 |

## PLT-4616 Firecracker / CH / Kata 比較と ExecutionProvider 方針

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 比較表（Kubernetes 統合、起動時間、設定面、host 強制 timeout、network、filesystem、pause/resume、snapshot/restore、cleanup、既存 cluster 依存）がある | 実装済み | `docs/adr/0001-execution-provider-firecracker-first.md` §「比較」 |
| 2 | 起動時間は upstream の主張として引用し、測定値と区別している | 実装済み | 同 §「比較」注記 |
| 3 | 決定: Firecracker 第一、trait の背後、Kata / CH は後続 adapter、CH は fallback | 実装済み | 同 §「決定」 |
| 4 | 残る測定の一覧と合格の目安がある | 実装済み（一覧） / 未検証（測定） | 同 §「残る測定」M1〜M13、`docs/evidence/plt-4621/` |
| 5 | fake / process provider の結果が決定を左右しない規則 | 実装済み | 同 §「受入規則」、`docs/adr/0002` |
| 6 | process provider の存在理由・guard・証明範囲 | 実装済み | `docs/adr/0002-process-provider-dev-only.md` |

## PLT-4617 Rust workspace・provider 境界・軽量 CI

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | workspace が Rust 1.95 / edition 2024 でビルドできる | 実装済み | `cargo build --workspace`、`rust-toolchain.toml`、`Cargo.toml` |
| 2 | domain が framework / hypervisor に依存しない（boundary test） | 実装済み | `crates/domain/src/boundary.rs::domain_has_no_framework_dependencies` |
| 3 | `ExecutionProvider` ほか port trait が定義され、`Capabilities` に `Supported / Unsupported / Unverified` がある | 実装済み | `crates/provider-port/src/{execution, artifact, secret, identity, usage}.rs` |
| 4 | fake provider で pipeline を KVM なしでテストできる | 別トラックで実装中 | `crates/providers/fake`、`crates/application` のテスト |
| 5 | CI（fmt / clippy `-D warnings` / test）が PR で走る | 別トラックで実装中 | `.github/workflows/ci.yml`（この commit には存在しない） |
| 6 | 依存の追加はルート `[workspace.dependencies]` 経由のみ | 実装済み（規約） / 未検証（自動検査） | `Cargo.toml`。期待するテスト: `workspace_dependencies_are_centralized` |

## PLT-4618 Function・Invocation・実行環境のモデルと DB migration

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | ID が `<prefix>_<26 文字 lowercase ULID>` で、prefix 不一致・大文字・短い ULID を拒否する | 実装済み | `crates/domain::ids::tests::{generated_ids_have_prefix_and_lowercase_ulid, wrong_prefix_is_rejected, uppercase_or_short_ulid_is_rejected, existing_tenant_ids_parse, serde_roundtrip_validates}` |
| 2 | Function / Revision / Alias の状態遷移と不変性（`spec_digest`、generation CAS） | 実装済み | `crates/domain::revision::tests::{valid_spec_passes_and_digest_is_stable, invalid_specs_are_rejected, revision_lifecycle_and_terminal_rejection, failure_allowed_from_any_non_terminal_state}`、`crates/domain::alias::tests::cas_update_and_rollback_pointer` |
| 3 | Invocation / Attempt の状態遷移（terminal 後の更新拒否、`OutcomeUnknown` は Running からのみ） | 実装済み | `crates/domain::invocation::tests::{happy_path, cannot_succeed_before_running, outcome_unknown_only_after_running, queue_timeout_fails_from_queued, attempt_terminal_once}` |
| 4 | ExecutionEnvironment / Lease（状態遷移、`(attempt_id, epoch)` fencing） | 実装済み | `crates/domain::environment::tests::{lifecycle, failure_from_any_state_and_lost, lease_fencing}` |
| 5 | repositories（in-memory）と `state.json` 永続化 | 別トラックで実装中 | `crates/application`（PLT-4619 / 4626） |
| 6 | DB migration（TiDB） | 未着手（P1 非対象。`docs/architecture.md` §6） | — |
| 7 | `env_` prefix の tachyon-apps との衝突を解消する方針を決める | 未着手 | `docs/inventory-tachyon-apps.md` §3.1, §7-1 |

## PLT-4619 関数管理 API・tenant 認可・execution role

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 管理 API の DTO と path が定義されている | 実装済み | `crates/api-types/src/lib.rs`（path 一覧はモジュール doc）、`tests::create_revision_request_defaults` |
| 2 | Bearer token → `Principal{tenant, roles}`、`X-Tachyon-Tenant-Id` 不一致は 403 | 実装済み（port） / 別トラックで実装中（gateway） | `crates/provider-port/src/identity.rs::{Principal, Role, IdentityProvider}`、`docs/threat-model.md` §7 |
| 3 | 他 tenant の資源は 404（403 ではない） | 別トラックで実装中 / 未検証 | 期待するテスト: `apps/gateway` `tenant_isolation_returns_404_for_foreign_resources` |
| 4 | role 不足は 403、operator は読み取り専用（`output` 省略、logs 403） | 別トラックで実装中 | `docs/threat-model.md` §7 |
| 5 | `POST /v1/functions` / `GET` / `DELETE`、revision の作成 / 一覧、alias の CAS 更新が動く | 別トラックで実装中 | `apps/gateway` |
| 6 | OpenAPI（`/openapi.json`）が DTO から生成される | 別トラックで実装中 | `docs/api.md` |

## PLT-4620 Rust / OCI artifact 登録・immutable publish・rollback

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `ArtifactStore` port（put / get / exists、digest 冪等） | 実装済み（port） / 別トラックで実装中（実装） | `crates/provider-port/src/artifact.rs` |
| 2 | digest が `sha256:<64 hex>` で計算・検証される | 実装済み | `crates/domain::ids::tests::digests` |
| 3 | Revision は不変で、`spec_digest` の不一致を検出する | 実装済み | `revision_lifecycle_and_terminal_rejection`（`verify_integrity`） |
| 4 | alias の CAS 更新と `previous_revision_id` による rollback | 実装済み（domain） / 別トラックで実装中（API / CLI） | `cas_update_and_rollback_pointer`、`PUT /v1/functions/{id}/aliases/{alias}` |
| 5 | OCI 参照は受理するが実行不能として理由付き `Failed` になる | 実装済み（型） / 別トラックで実装中（validation） | `crates/domain::ArtifactRef::OciImage`、`crates/api-types::ArtifactRequest::OciImage` |
| 6 | artifact の tenant 所有を記録し、他 tenant の digest 参照を拒否する | 未着手 | `docs/threat-model.md` §14-1 |
| 7 | `POST /v1/artifacts` の上限（256 MiB）超過が 413 | 別トラックで実装中 | `crates/domain::Limits::max_artifact_bytes` |

## PLT-4621 Firecracker ExecutionProvider

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `preflight` が `/dev/kvm`、バイナリ、kernel / rootfs の存在と digest を検査する | 別トラックで実装中 | `crates/providers/firecracker`、`docs/evidence/plt-4621/preflight.json` |
| 2 | API 順序（machine-config → boot-source → drives → vsock → InstanceStart）と `<uds>_5000` の事前 listen | 別トラックで実装中 | `docs/protocol.md` §C |
| 3 | function drive を `mkfs.ext4 -d` で生成し read-only で渡す | 別トラックで実装中 | 同上 |
| 4 | `EnvironmentHandle` が bridge 接続済み stream と `BootEvidence{guest_boot_id, host_pid, details}` を返す | 別トラックで実装中 / 未検証 | `docs/evidence/plt-4621/hello-attempt.json` |
| 5 | `terminate_environment` が冪等で、`cleaned` に API socket / vsock uds / drive / workdir を列挙する | 別トラックで実装中 / 未検証 | ADR-0001 M6 |
| 6 | baseline profile で boot / init / handler の時間を測定している | 未検証 | ADR-0001 M2〜M4、`docs/evidence/plt-4621/timings.json` |
| 7 | aarch64 で動作する | 未検証 | ADR-0001 M12 |

## PLT-4622 tenant 隔離・egress 起動ゲート・資源上限

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `Capabilities` で egress / resource / isolation を明示し、未測定は `Unverified` | 実装済み（型） / 別トラックで実装中（値） | `crates/provider-port::{Capabilities, Support, IsolationLevel}` |
| 2 | revision の resource / timeout / env の範囲検査 | 実装済み | `crates/domain::revision::tests::invalid_specs_are_rejected` |
| 3 | `TACHYON_` prefix の env と重複を拒否する | 実装済み | 同上 |
| 4 | P1 は tap を作らず egress none。`Restricted` / `PublicWeb` を要求する revision は拒否 | 別トラックで実装中 / 未検証 | ADR-0001 M8 |
| 5 | vCPU / memory が `machine-config` に反映され、超過 alloc が `Crash` に分類される | 未検証 | ADR-0001 M9 |
| 6 | 1 environment = 1 tenant × 1 revision × 1 attempt、破棄後に再利用しない | 実装済み（domain 規約） / 別トラックで実装中（pipeline） | `crates/domain::ExecutionPolicy`（`concurrency_per_environment = 1`、`min_ready = 0`）、`docs/threat-model.md` §5-6 |
| 7 | jailer / 専用ユーザーの検討 | 未着手 | `docs/threat-model.md` §14-2 |

## PLT-4623 実行 identity・Secret binding・準備完了ゲート

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `SecretProvider` が `SecretDeliveryContext{tenant, revision, environment, epoch}` 付きでしか解決しない | 実装済み（port） / 別トラックで実装中（実装） | `crates/provider-port/src/secret.rs` |
| 2 | `SecretValue` の `Debug` が redact される | 実装済み（コード） / 未検証（テスト無し） | `crates/provider-port/src/secret.rs`。期待するテスト: `secret_value_debug_is_redacted` |
| 3 | secret 値が `HelloAck.env` にだけ載り、ログ・API・`state.json` に出ない | 別トラックで実装中 / 未検証 | 期待するテスト: `hello_ack_is_never_logged`、`state_json_contains_no_secret_values` |
| 4 | `Ready` を `init_deadline` まで待ち、超過は `InitError` / 502 | 実装済み（domain） / 別トラックで実装中（bridge session） | `crates/domain::Deadlines.init_deadline`、`docs/threat-model.md` §8 |
| 5 | 静的 token / binding を `config/gateway.toml` から読む | 別トラックで実装中 | `docs/architecture.md` §4 |
| 6 | tachyon-apps `packages/secrets` への adapter | 未着手（P1 非対象） | `docs/inventory-tachyon-apps.md` §3.6 |

## PLT-4624 guest Runtime Bridge と protocol

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | frame codec（`u32 BE` 長 + JSON、8 MiB 上限、未知 type はエラー、未知 field は無視） | 実装済み | `crates/protocol::wire::tests::{frames_roundtrip_over_duplex, oversized_frame_rejected, unknown_fields_are_ignored_but_unknown_types_fail}` |
| 2 | Runtime API の path / header / event type / DTO | 実装済み | `crates/protocol/src/runtime_api.rs`、`tests::http_event_roundtrip` |
| 3 | bridge バイナリ（`--transport unix|vsock`、`--init` モード、exit code 0/2/3/4） | 別トラックで実装中 | `crates/runtime-bridge` |
| 4 | 1 in-flight、`Ready` は 1 回、in-flight 中の終了は `Error{crash}` | 別トラックで実装中 / 未検証 | 期待するテスト: `bridge_rejects_second_in_flight`、`bridge_reports_crash_with_exit_code` |
| 5 | `Cancel{grace_ms}` で SIGTERM → grace → SIGKILL | 別トラックで実装中 / 未検証 | 期待するテスト: `bridge_cancel_kills_after_grace` |
| 6 | static musl ビルドで `/sbin/tachyon-init` として動く | 未検証 | `docs/evidence/plt-4621/` |

## PLT-4625 Rust handler SDK・axum HTTP アダプター・ローカル実行

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `run(handler)` が `tachyon.invoke.v1` を処理し、`Err` → `Handler.Error`、panic → `Runtime.Panic` | 別トラックで実装中 | `crates/sdk` |
| 2 | `serve_http(router)` が `tachyon.http.v1` を `http::Request` に変換し `tower::Service` として呼ぶ（TCP を開かない） | 別トラックで実装中 | `crates/sdk` |
| 3 | 非 http event を `serve_http` に渡すと `Runtime.UnsupportedEvent` | 別トラックで実装中 | 期待するテスト: `serve_http_rejects_json_event` |
| 4 | `TACHYON_UNISOLATED=1` を SDK が読める | 別トラックで実装中 | `crates/protocol::env::UNISOLATED` |
| 5 | ローカル実行（`tsls dev`）で process provider を使って往復できる | 別トラックで実装中 | `docs/cli.md` |

## PLT-4626 同期 Invoke Gateway

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `POST /v1/functions/{id}:invoke` と `ANY /v1/functions/{id}/http/{*path}` | 別トラックで実装中 | `apps/gateway` |
| 2 | alias → revision の解決と受付時の固定、`revision_not_ready` / `function_deleted` は 409 | 実装済み（domain / api-types） / 別トラックで実装中（gateway） | `crates/api-types::ErrorCode`、`docs/architecture.md` §3 |
| 3 | payload 上限 413、容量超過 429、queue 超過 504 | 別トラックで実装中 | `docs/threat-model.md` §8, §11 |
| 4 | Idempotency-Key（一致は既存を返し再実行しない、不一致は 409） | 別トラックで実装中 | `docs/threat-model.md` §10。期待するテスト: `idempotency_key_returns_existing_invocation`、`idempotency_key_conflict_on_different_input` |
| 5 | `InvocationResponse` に attempts / timings / boot_evidence / deadlines が入る | 実装済み（DTO） / 別トラックで実装中（値） | `crates/api-types::{InvocationResponse, AttemptResponse}` |
| 6 | client 切断後も deadline まで追跡して記録する | 別トラックで実装中 / 未検証 | 期待するテスト: `client_disconnect_does_not_cancel` |

## PLT-4627 ホスト強制 timeout・cancel・結果不明・後始末

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | Lease の `(attempt_id, epoch)` 不一致・解放後の結果を拒否する | 実装済み | `crates/domain::environment::tests::lease_fencing` |
| 2 | `execution_deadline` 到達で `Cancel` → grace 1 s → `terminate` → `Failed{Timeout}` / 504 | 別トラックで実装中 / 未検証 | ADR-0001 M5、期待するテスト: `watchdog_terminates_on_execution_deadline` |
| 3 | `POST :cancel` → `Cancelled` / 499、cancel 自体は 202 | 別トラックで実装中 | `crates/api-types::ErrorCode::Cancelled` |
| 4 | 接続断で `OutcomeUnknown`、自動再実行しない、環境 terminate | 実装済み（domain） / 別トラックで実装中（pipeline） | `outcome_unknown_only_after_running`、ADR-0001 M11 |
| 5 | `terminate_environment` が冪等で `cleaned` を列挙、orphan 0 | 実装済み（port） / 未検証 | `crates/provider-port::TerminateReport`、ADR-0001 M6 / M7 |
| 6 | gateway 再起動時に Running を `OutcomeUnknown` に reconcile し、`list_environments` の孤児を terminate する | 別トラックで実装中 | 期待するテスト: `reconcile_marks_running_as_outcome_unknown` |
| 7 | timeout / 強制終了した環境を再利用しない | 実装済み（P1 は再利用なし） | `docs/architecture.md` §1 |

## PLT-4628 Invocation 単位のログ・履歴・トレース

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | log 行を 16 KiB で char boundary 切り、`truncated` を立てる | 実装済み | `crates/domain::log::tests::bounded_line_respects_char_boundaries` |
| 2 | invocation ごとに 2000 行 / 1 MiB で打ち切り、`LogsResponse.dropped = true` | 実装済み（型） / 別トラックで実装中（保持） | `crates/domain::Limits`、`crates/api-types::LogsResponse` |
| 3 | `phase`（boot / init / handler / shutdown）と `stream`（stdout / stderr / platform）を保持する | 実装済み（型） / 別トラックで実装中 | `crates/domain::{LogPhase, LogStream}`、`crates/protocol::{LogPhase, LogStream}` |
| 4 | `GET /v1/invocations/{id}/logs`、`GET /v1/functions/{id}/invocations` | 別トラックで実装中 | `apps/gateway` |
| 5 | `trace_id` が invoke → Invoke frame → Runtime API header → 応答 header まで伝播する | 別トラックで実装中 / 未検証 | 期待するテスト: `trace_id_propagates_to_guest` |
| 6 | secret 値がログに出ない | 未検証 | PLT-4623 #3 と同じ |

## PLT-4629 CLI functions deploy / invoke / logs / rollback / dev

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `tsls deploy`（artifact upload → revision → alias） | 別トラックで実装中 | `apps/cli`、`docs/cli.md` |
| 2 | `tsls invoke`（結果、`invocation_id`、provider が dev_only なら警告） | 別トラックで実装中 | `docs/adr/0002` §「決定」4 |
| 3 | `tsls logs` | 別トラックで実装中 | — |
| 4 | `tsls rollback`（`previous_revision_id` へ CAS 更新） | 別トラックで実装中 | — |
| 5 | `tsls dev`（process provider でローカル往復） | 別トラックで実装中 | — |
| 6 | CLI は HTTP 経由でのみ application に触れる | 実装済み（規約） | `docs/architecture.md` §2 |

## PLT-4630 Rust サンプル 3 種と E2E デモ

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `examples/hello`（JSON invoke） | 別トラックで実装中（skeleton あり） | `examples/hello/src/main.rs` |
| 2 | `examples/http-axum`（`serve_http`） | 別トラックで実装中（skeleton あり） | `examples/http-axum/src/main.rs` |
| 3 | `examples/cpu-burn`（timeout kill の実演） | 別トラックで実装中（skeleton あり） | `examples/cpu-burn/src/main.rs` |
| 4 | E2E スクリプト: deploy → invoke → logs → rollback → 環境破棄を process provider と Firecracker provider の両方で通す | 未着手 | `scripts/e2e.sh`（期待） |
| 5 | Firecracker provider での実行証跡（`BootEvidence.guest_boot_id`、`AttemptTimings`、`GET /v1/provider`）を残す | 未検証 | `docs/evidence/plt-4630/` |
| 6 | process provider の結果を microVM の証跡として使わない | 実装済み（規則） | `docs/adr/0002`、`docs/adr/0001` §「受入規則」 |
