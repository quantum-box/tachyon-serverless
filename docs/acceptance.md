# 受入チェックリスト（PLT-4613〜PLT-4630）

- 対象: Linear プロジェクト「Tachyon Serverless — 動作プロトタイプ」P0〜P1
- 基準: `docs/architecture.md`、`docs/protocol.md`、`docs/threat-model.md`、`docs/adr/`
- 状態の記録日: 2026-09-16（統合ブランチ `feat/serverless-prototype-p1` の commit `8555e34` 以降（2026-09-16 のレビュー指摘の修正を統合した後。E2E はこの統合後の commit `95af2ba` で再実行）のコード・テスト・`docs/evidence/` を読んで更新）

## 状態の定義

| 状態 | 意味 |
|---|---|
| 実装済み | コードと、それを検査する自動テスト（または文書・規則）が本リポジトリにあり、下記のコマンドで再現できる。KVM 実機での確認は含まない |
| 実装済み・KVM実測あり | 上に加えて、Firecracker microVM（Linux/KVM）で動かした記録が `docs/evidence/` にある |
| 未検証 | コードまたは文書はあるが、条件を満たす自動テストまたは実機の記録が無い。理由を併記する |
| 未着手 | 実装が無い、または P1 で非対象と決めた |

- 1 行に複数の状態があるときは「実装済み（範囲） / 未検証（範囲）」のように範囲を括弧で書く。
- テスト名は `<ファイル>::<テスト関数>`（複数は `::{a, b}`）。`cargo test -p <crate> <テスト関数>` で再現する。

## 証跡

### 再現コマンド

| 種類 | コマンド | 備考 |
|---|---|---|
| 単体・統合テスト | `cargo test --workspace` | `tests/` 以下は統合テスト（fake provider、偽 Firecracker、mock gateway） |
| lint | `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings` | `.github/workflows/ci.yml` と同じ |
| E2E（process provider） | `scripts/e2e/demo.sh` | 27 step。ヘルパのテストは `scripts/e2e/selftest.sh` |
| KVM smoke | `scripts/kvm/bootstrap.sh` → `scripts/kvm/smoke.sh` | 手順と確認済みの環境は `docs/kvm.md` §5 |
| E2E（Firecracker provider） | `TSLS_PROVIDER=firecracker scripts/e2e/demo.sh` | 同上 |

### 実機の記録（`docs/evidence/`）

| ディレクトリ | 内容 | 環境 | 結果 |
|---|---|---|---|
| `docs/evidence/20260915T073238Z-process/` | `scripts/e2e/demo.sh` の `summary.json`、`provider.json`、`invocations.json`、`gateway.log`、`steps/*.log` | macOS（Darwin 25.6.0 arm64）、process provider（隔離なし） | 27/27 PASS |
| `docs/evidence/kvm-20260915T080221Z/` | `scripts/kvm/smoke.sh`（`fc-smoke`、gateway なし）の `summary.txt`、`hello.json`、`timeout.json`、guest console、Firecracker log | Apple M4 上の Lima VM（vz + nested virtualization）、Linux 7.0.0-28-generic aarch64、Firecracker v1.17.0、guest kernel 6.1.155 | hello は `outcome=response`、timeout demo は `outcome=timeout`。どちらも `leftovers` なし |
| `docs/evidence/20260915T125610Z-firecracker/` | `TSLS_PROVIDER=firecracker scripts/e2e/demo.sh` の同じファイル群 | 上と同じ VM、`config/gateway.firecracker.toml`（`profile = "production"`） | 27/27 PASS |
| `docs/evidence/20260915T171415Z-process/` | `scripts/e2e/demo.sh`（レビュー指摘修正の統合後、secret 値の検査ステップを含む 28 ステップ） | macOS、process provider（隔離なし） | 28/28 PASS |
| `docs/evidence/20260915T171631Z-firecracker/` | `TSLS_PROVIDER=firecracker scripts/e2e/demo.sh`（同上、commit `95af2ba`） | 上と同じ VM | 28/28 PASS |

本文の「E2E step NN」は各 E2E ディレクトリの `steps/NN-*.log`（例: step 23 = `steps/23-cross-tenant_get_invoke_-__404.log`）。特に断らない限り process と firecracker の両方で PASS している。

KVM の記録に共通する制約:

- aarch64 の **nested virtualization** 上の記録であり、`docs/inventory-tachyon-apps.md` §6 の baseline profile（x86_64 第一、N ≥ 20、中央値・p95）を満たしていない。時間の値は参考値で、性能や SLA の約束ではない。
- 1 host・1 人の開発者による 1 回ずつの実行。別 host・別開発者の追試はしていない（§「横断」）。
- process provider の結果は microVM の証跡として使わない（ADR-0002、ADR-0001 §「受入規則」）。process の attempt の `boot_evidence` には `guest_boot_id` が無い。

## レビュー指摘（2026-09-16）

2026-09-16 のコードレビューで確認された 14 件の指摘は、すべて修正・統合済み（merge commit `963d5b6`、`a167682`、`5f59ddc`）。各 issue の行の状態を書き換え、修正を検査するテストを証跡に記載した。修正の対象外で残っている範囲（KVM 上の測定、client 切断のテストなど）は、各行に「未検証」「未着手」として理由とともに残している。

---

## PLT-4613 既存 Kata・compute・runner の実機棚卸しと再利用マップ

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | tachyon-apps の該当領域を reference / adapter candidate / 不要 に分類し、確認したファイルパスと commit を記録している | 実装済み | `docs/inventory-tachyon-apps.md` §3（commit `ae727f1f7`） |
| 2 | 本リポジトリの各 port（ExecutionProvider / ArtifactStore / SecretProvider / IdentityProvider / UsageSink / repositories）への対応を示している | 実装済み | `docs/inventory-tachyon-apps.md` §2 |
| 3 | 本リポジトリが tachyon-apps に compile-time 依存しないことを明記し、ワークスペースにも依存が無い | 実装済み | `docs/inventory-tachyon-apps.md` 冒頭、ルート `Cargo.toml`（path / git 依存なし）、`crates/domain/src/boundary.rs::domain_has_no_framework_dependencies` |
| 4 | 「既存の Kata / CH 設定は runtime 能力の証拠ではない」と明記している | 実装済み | `docs/inventory-tachyon-apps.md` 冒頭、§3.5 |
| 5 | 能力表（create/terminate、deadline、network、ephemeral storage、pause/resume、snapshot/restore × Firecracker / CH / Kata）があり、未測定項目は `unverified` | 実装済み（Firecracker の create / terminate と deadline は baseline profile 外の記録で verified、それ以外は unverified / unsupported） | `docs/inventory-tachyon-apps.md` §5。verified の根拠は `docs/evidence/kvm-20260915T080221Z/`（`hello.json`、`timeout.json`）と `docs/evidence/20260915T125610Z-firecracker/`（E2E step 18・19・27）で、aarch64 の nested virtualization 上の記録（§「ADR-0001 残る測定の状況」） |
| 6 | baseline KVM 測定プロファイル（host arch、vCPU、memory、kernel、rootfs）を定義している | 実装済み | `docs/inventory-tachyon-apps.md` §6 |
| 7 | 実機での測定 | 実装済み・KVM実測あり（Firecracker、aarch64 nested virtualization） / 未着手（Cloud Hypervisor・Kata） | `docs/evidence/kvm-20260915T080221Z/`、`docs/evidence/20260915T125610Z-firecracker/`、§「ADR-0001 残る測定の状況」 |

## PLT-4614 プロトタイプの API・実行契約・脅威モデルを固定

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 信頼境界・資産・前提・原則（guest 自己申告を根拠にしない）が文書化されている | 実装済み | `docs/threat-model.md` §3〜§6 |
| 2 | 2 tenant + operator のアクセス制御マトリクスと期待 HTTP 結果（他 tenant は 404） | 実装済み（2 tenant、operator 列 = 自 tenant の読み取り専用。他 tenant 404、invocation 系 403） | `docs/threat-model.md` §7、`crates/application/src/authz.rs::foreign_tenant_is_not_found`、`apps/gateway/tests/gateway_integration.rs::operator_role_is_own_tenant_read_only`、`crates/application/tests/pipeline.rs::cross_tenant_resources_are_not_found`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（他 tenant の function / invocation / logs / invoke が 404）、E2E step 23 |
| 3 | 失敗分類 `ErrorClass` と HTTP `ErrorCode` の対応が固定されている | 実装済み | `crates/domain/src/invocation.rs`（`ErrorClass`）、`crates/api-types/src/lib.rs::error_codes_map_to_status`、`crates/application/src/error.rs::codes_and_statuses` |
| 4 | deadline モデル（queue / init / execution / client）が定義されている | 実装済み（文書・domain 型、queue / init / execution / client の enforcement） | `docs/threat-model.md` §8、`crates/domain/src/invocation.rs::rejects_elapsed_client_deadline_and_bad_key`、`crates/application/tests/pipeline.rs::{capacity_exceeded_and_queue_timeout, user_error_panic_init_error_and_never_ready, hang_times_out_cancels_and_terminates_with_timeout}`、client deadline は `crates/application/tests/pipeline.rs::{client_deadline_bounds_the_queue_wait, client_deadline_during_initialization_never_starts_the_handler, client_deadline_is_checked_again_right_before_dispatch, client_deadline_clamps_the_execution_deadline}`（到達は `Timeout` / `Host.ClientDeadline` / 504。handler 送信の直前にも再確認し、期限後は dispatch しない） |
| 5 | `OutcomeUnknown` の意味（Running からのみ、自動再実行しない）が固定されている | 実装済み | `docs/threat-model.md` §9、`crates/domain/src/invocation.rs::outcome_unknown_only_after_running`、`crates/application/tests/pipeline.rs::disconnect_after_invoke_is_outcome_unknown` |
| 6 | Idempotency-Key の意味（scope、一致 / 不一致、保持）が固定されている | 実装済み（文書、key 長の検証、gateway の replay / conflict、受付前に拒否された key を消費しない） | `docs/threat-model.md` §10、`crates/application/tests/pipeline.rs::{idempotency_replay_and_conflict, capacity_rejection_does_not_consume_the_idempotency_key, invalid_idempotency_key_and_trace_id_are_rejected_without_side_effects, concurrent_requests_with_the_same_key_run_once, dangling_idempotency_key_is_healed_on_restart}`、`crates/application/src/repository.rs::{idempotency_key_is_bound_with_its_invocation, dangling_idempotency_entries_are_dropped_on_restart}`（key は Invocation の記録と同じ更新で結び付ける。429 / 400 の後の再送は新規として受け付ける）、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（同じ key の再送で再実行しない） |
| 7 | payload と資源の上限が `Limits` と一致している | 実装済み | `docs/threat-model.md` §11、`crates/domain/src/limits.rs` |
| 8 | process provider が守らないもの・非目標が明記されている | 実装済み | `docs/threat-model.md` §13, §15、`docs/adr/0002-process-provider-dev-only.md` |
| 9 | host ↔ bridge ↔ SDK の protocol が固定されている | 実装済み | `docs/protocol.md`、`crates/protocol/src/wire.rs::{frames_roundtrip_over_duplex, oversized_frame_rejected, unknown_fields_are_ignored_but_unknown_types_fail}`、`crates/protocol/src/runtime_api.rs::http_event_roundtrip`、`crates/runtime-bridge/tests/roundtrip.rs::echo_roundtrip_then_shutdown` |

## PLT-4615 専用 KVM 検証環境と bootstrap・teardown

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | KVM host の要件（`/dev/kvm`、arch、nested virt の記録）が文書化されている | 実装済み・KVM実測あり | `docs/kvm.md` §2・§5、`scripts/kvm/preflight.sh`、`docs/inventory-tachyon-apps.md` §6。記録: `docs/evidence/kvm-20260915T080221Z/hello.json` の `preflight`（os / arch / kvm ほか 9 項目が ok）。nested virtualization であることは `docs/kvm.md` §5 に記録（`PreflightReport` の項目には無い） |
| 2 | bootstrap スクリプトで `firecracker` バイナリ・kernel（CI kernel v1.17 系列）・rootfs を取得し、digest を検証する | 実装済み・KVM実測あり（`CI_VERSION=v1.15 GUEST_KERNEL_SERIES=6.1`） / 未検証（既定の日付 prefix 自動解決、§6 の「v1.17 系列」kernel） | `scripts/kvm/bootstrap.sh`（Firecracker v1.17.0 の tgz を公開 `.sha256.txt` で検証。kernel は upstream の digest が無いため取得時の sha256 を `.kvm/manifest.json` に記録し、再実行時に照合）、`scripts/kvm/build-rootfs.sh`、`docs/evidence/kvm-20260915T080221Z/summary.txt`（kernel / rootfs の sha256）、`docs/evidence/kvm-20260915T080221Z/hello-console.txt`（`Linux version 6.1.155+`） |
| 3 | teardown スクリプトで環境・ソケット・drive・workdir を全削除し、orphan 0 を確認する | 実装済み（スクリプト） / 未検証（KVM 上で `scripts/kvm/teardown.sh` を実行した記録が evidence に無い） | `scripts/kvm/teardown.sh`。orphan 0 そのものは fc-smoke の `leftovers`（`docs/evidence/kvm-20260915T080221Z/hello.json`、`timeout.json`）と E2E step 19・27（`scripts/e2e/orphan-check.sh`）で確認 |
| 4 | `ExecutionProvider::preflight` が host 要件を検査し、`GET /readyz` に反映する | 実装済み・KVM実測あり | `crates/provider-port/src/execution.rs`（`PreflightReport`）、`crates/providers/firecracker/src/preflight.rs::{preflight_reports_structured_failures_on_any_host, digest_cache_hits_until_file_changes, concurrent_digests_are_deduplicated_and_survive_cancellation}`、`crates/providers/firecracker/src/provider.rs::preflight_never_fails_and_kind_is_firecracker`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（`/readyz`）、`docs/evidence/20260915T125610Z-firecracker/steps/02-gateway_healthz_readyz.log` |
| 5 | 本番 cluster・remote に触れない | 実装済み（規則） | `docs/inventory-tachyon-apps.md` §3.12、`docs/adr/0001-execution-provider-firecracker-first.md` §「決定」、`.github/workflows/kvm-integration.yml`（手動起動のみ、secret なし） |

## PLT-4616 Firecracker / CH / Kata 比較と ExecutionProvider 方針

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 比較表（Kubernetes 統合、起動時間、設定面、host 強制 timeout、network、filesystem、pause/resume、snapshot/restore、cleanup、既存 cluster 依存）がある | 実装済み | `docs/adr/0001-execution-provider-firecracker-first.md` §「比較」 |
| 2 | 起動時間は upstream の主張として引用し、測定値と区別している | 実装済み | 同 §「比較」注記 |
| 3 | 決定: Firecracker 第一、trait の背後、Kata / CH は後続 adapter、CH は fallback | 実装済み | 同 §「決定」 |
| 4 | 残る測定の一覧と合格の目安がある | 実装済み（一覧） / 実装済み・KVM実測あり（M1・M5・M7・M12、M2〜M4 は参考値） / 未検証（M6・M8〜M11・M13、M2〜M4 の N ≥ 20） | §「ADR-0001 残る測定の状況」。ADR-0001 §「受入規則」の確定条件（M1〜M8 が通る）は M6・M8 が未検証のため、まだ満たしていない |
| 5 | fake / process provider の結果が決定を左右しない規則 | 実装済み | 同 §「受入規則」、`docs/adr/0002-process-provider-dev-only.md` |
| 6 | process provider の存在理由・guard・証明範囲 | 実装済み | `docs/adr/0002-process-provider-dev-only.md`、`crates/application/src/config.rs::production_rejects_dev_only_provider`、`crates/application/tests/pipeline.rs::production_profile_rejects_dev_only_provider` |

## PLT-4617 Rust workspace・provider 境界・軽量 CI

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | workspace が Rust 1.95 / edition 2024 でビルドできる | 実装済み | `cargo build --workspace`、`rust-toolchain.toml`、`Cargo.toml` |
| 2 | domain が framework / hypervisor に依存しない（boundary test） | 実装済み | `crates/domain/src/boundary.rs::domain_has_no_framework_dependencies` |
| 3 | `ExecutionProvider` ほか port trait が定義され、`Capabilities` に `Supported / Unsupported / Unverified` がある | 実装済み | `crates/provider-port/src/execution.rs`、`artifact.rs`、`secret.rs`、`identity.rs`、`usage.rs` |
| 4 | fake provider で pipeline を KVM なしでテストできる | 実装済み | `crates/providers/fake/src/lib.rs::{respond_ok_speaks_protocol_and_records_lifecycle, init_error_and_duplicate_id, disconnect_after_invoke_closes_stream, capabilities_are_dev_only}`、`crates/application/tests/pipeline.rs`（23 テスト）、`apps/gateway/tests/gateway_integration.rs` |
| 5 | CI（fmt / clippy `-D warnings` / test）が PR で走る | 実装済み（workflow 定義） / 未検証（この更新では GitHub Actions 上の実行結果を確認していない） | `.github/workflows/ci.yml`（fmt / clippy / test / build、x86_64 musl の guest ビルドと static 確認、`bash -n`・shellcheck・`scripts/e2e/selftest.sh`）。`.github/workflows/kvm-integration.yml` は手動起動のみで、self-hosted KVM runner は未用意 |
| 6 | 依存の追加はルート `[workspace.dependencies]` 経由のみ | 実装済み（規約） / 未検証（自動検査なし） | `Cargo.toml`。検査するテストは存在しない |

## PLT-4618 Function・Invocation・実行環境のモデルと DB migration

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | ID が `<prefix>_<26 文字 lowercase ULID>` で、prefix 不一致・大文字・短い ULID を拒否する | 実装済み | `crates/domain/src/ids.rs::{generated_ids_have_prefix_and_lowercase_ulid, wrong_prefix_is_rejected, uppercase_or_short_ulid_is_rejected, existing_tenant_ids_parse, serde_roundtrip_validates}` |
| 2 | Function / Revision / Alias の状態遷移と不変性（`spec_digest`、generation CAS） | 実装済み | `crates/domain/src/revision.rs::{valid_spec_passes_and_digest_is_stable, invalid_specs_are_rejected, revision_lifecycle_and_terminal_rejection, failure_allowed_from_any_non_terminal_state}`、`crates/domain/src/alias.rs::cas_update_and_rollback_pointer` |
| 3 | Invocation / Attempt の状態遷移（terminal 後の更新拒否、`OutcomeUnknown` は Running からのみ） | 実装済み | `crates/domain/src/invocation.rs::{happy_path, cannot_succeed_before_running, outcome_unknown_only_after_running, queue_timeout_fails_from_queued, attempt_terminal_once}` |
| 4 | ExecutionEnvironment / Lease（状態遷移、`(attempt_id, epoch)` fencing） | 実装済み | `crates/domain/src/environment.rs::{lifecycle, failure_from_any_state_and_lost, lease_fencing}` |
| 5 | repositories（in-memory）と `state.json` 永続化 | 実装済み | `crates/application/src/repository.rs::{function_name_unique_per_tenant, idempotency_key_is_bound_with_its_invocation, dangling_idempotency_entries_are_dropped_on_restart, artifact_ownership_is_per_tenant_and_persisted, state_without_artifact_owners_still_loads, logs_are_bounded_per_invocation, persistence_roundtrip_and_restart_reconcile, corrupt_state_file_is_refused_with_a_hint}` |
| 6 | DB migration（TiDB） | 方針決定済み（`docs/adr/0003-execution-state-persistence.md`。P2 が要求する複数プロセス間の原子性と移行手順を決定） / 未着手（実装。P1 の永続化は in-memory + `state.json` のまま） | `docs/adr/0003-execution-state-persistence.md`、`docs/architecture.md` §5-12 |
| 7 | `env_` prefix の tachyon-apps との衝突を解消する方針を決める | 方針決定済み（`docs/adr/0003-execution-state-persistence.md`） | `docs/inventory-tachyon-apps.md` §3.1, §7-1 |

## PLT-4619 関数管理 API・tenant 認可・execution role

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 管理 API の DTO と path が定義されている | 実装済み | `crates/api-types/src/lib.rs::create_revision_request_defaults`、`docs/api.md` |
| 2 | Bearer token → `Principal{tenant, roles}`、`X-Tachyon-Tenant-Id` 不一致は 403 | 実装済み | `crates/provider-port/src/identity.rs`、`apps/gateway/src/middleware.rs`（`authenticate`）、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（token 無し / 不正は 401、`x-tachyon-tenant-id` 不一致は 403、一致は 200） |
| 3 | 他 tenant の資源は 404（403 ではない） | 実装済み・KVM実測あり | `crates/application/src/authz.rs::foreign_tenant_is_not_found`、`crates/application/tests/pipeline.rs::cross_tenant_resources_are_not_found`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`、`apps/cli/tests/mock_gateway.rs::cross_tenant_resource_is_404_exit_2`、E2E step 23 |
| 4 | role 不足は 403、operator は読み取り専用（`output` 省略、logs 403） | 実装済み（role 不足 403、operator は自 tenant の読み取り専用（他 tenant 404、invocation 系 403）） | `crates/application/src/authz.rs::roles`、`apps/gateway/tests/gateway_integration.rs::operator_role_is_own_tenant_read_only`、`docs/threat-model.md` §7。受入条件の「`output` を省いた invocation 閲覧」は実装せず、operator には invocation / logs / usage を 403 で返す（§7 の注。実装するまで付与しない） |
| 5 | `POST /v1/functions` / `GET` / `DELETE`、revision の作成 / 一覧、alias の CAS 更新が動く | 実装済み・KVM実測あり | `apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（作成・一覧・削除後の invoke が 409 `function_deleted`、revision 作成、alias CAS 409）、`crates/application/tests/pipeline.rs::alias_cas_conflict_and_rollback`、`apps/cli/tests/mock_gateway.rs::{create_and_list_functions, resolves_function_by_name_and_by_id}`、E2E step 05〜08・22 |
| 6 | OpenAPI（`/openapi.json`）が DTO から生成される | 実装済み | `apps/gateway/src/openapi.rs`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（OpenAPI 3.x、主要 path、`ApiErrorBody`）、`docs/api.md` |

## PLT-4620 Rust / OCI artifact 登録・immutable publish・rollback

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `ArtifactStore` port（put / get / exists、digest 冪等） | 実装済み | `crates/provider-port/src/artifact.rs`、`crates/application/src/local_ports.rs::artifact_store_put_get_exists_and_verifies` |
| 2 | digest が `sha256:<64 hex>` で計算・検証される | 実装済み | `crates/domain/src/ids.rs::digests` |
| 3 | Revision は不変で、`spec_digest` の不一致を検出する | 実装済み | `crates/domain/src/revision.rs::revision_lifecycle_and_terminal_rejection`（`verify_integrity`） |
| 4 | alias の CAS 更新と `previous_revision_id` による rollback | 実装済み・KVM実測あり | `crates/domain/src/alias.rs::cas_update_and_rollback_pointer`、`crates/application/tests/pipeline.rs::alias_cas_conflict_and_rollback`、`apps/cli/tests/mock_gateway.rs::{rollback_uses_previous_revision_and_cas_generation, rollback_to_explicit_revision_and_conflict}`、E2E step 22 |
| 5 | OCI 参照は受理するが実行不能として理由付き `Failed` になる | 実装済み（コード） / 未検証（テストなし） | `crates/application/src/services/revision.rs`（`registry/repo@sha256:<hex>` の pinned 参照だけ受理し、validate で「not executable by prototype providers」の理由付き `Failed`）、`crates/application/src/services/invoke.rs`（invoke 時も `init_error`） |
| 6 | artifact の tenant 所有を記録し、他 tenant の digest 参照を拒否する | 実装済み | `crates/application/src/services/artifact.rs`（`ArtifactService::upload` が `(tenant_id, digest)` の所有を記録し、`owned_artifact` は自 tenant が upload していない digest を存在しない digest と同じに扱う）、`crates/application/src/repository.rs::{artifact_ownership_is_per_tenant_and_persisted, state_without_artifact_owners_still_loads}`、`crates/application/tests/pipeline.rs::revisions_cannot_reference_another_tenants_artifact`、`apps/gateway/tests/gateway_integration.rs::foreign_artifact_digest_is_indistinguishable_from_a_missing_one`、`docs/threat-model.md` §14-1。修正前の版で作られた Ready revision は再検証しない |
| 7 | `POST /v1/artifacts` の上限（256 MiB）超過が 413 | 実装済み（コード） / 未検証（artifact upload の 413 を検査するテストなし） | `apps/gateway/src/lib.rs`（`/v1/artifacts` の `DefaultBodyLimit`）、`apps/gateway/src/handlers.rs`（`read_body` が上限超過を `payload_too_large` にする）、`crates/domain/src/limits.rs`。invoke payload の 413 は `apps/gateway/tests/gateway_integration.rs::invoke_failures_map_to_status_codes` |

## PLT-4621 Firecracker ExecutionProvider

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `preflight` が `/dev/kvm`、バイナリ、kernel / rootfs の存在と digest を検査する | 実装済み・KVM実測あり | PLT-4615 #4 のテスト、`docs/evidence/kvm-20260915T080221Z/hello.json` の `preflight`、`docs/evidence/20260915T125610Z-firecracker/provider.json` |
| 2 | API 順序（machine-config → boot-source → drives → vsock → InstanceStart）と `<uds>_5000` の事前 listen | 実装済み・KVM実測あり | `crates/providers/firecracker/tests/fake_vmm.rs::full_lifecycle_with_fake_vmm`（PUT の順序と、InstanceStart より前の listen を検査）、`docs/evidence/kvm-20260915T080221Z/hello-console.txt`（kernel cmdline、`Run /sbin/tachyon-init as init process`） |
| 3 | function drive を `mkfs.ext4 -d` で生成し read-only で渡す | 実装済み・KVM実測あり | `crates/providers/firecracker/src/drive.rs::{mkfs_arguments_follow_protocol, sizing_rounds_up_to_mib_with_8mib_slack, sparse_image_has_requested_length}`、`hello.json` の `evidence.details.function_drive_bytes` |
| 4 | `EnvironmentHandle` が bridge 接続済み stream と `BootEvidence{guest_boot_id, host_pid, details}` を返す | 実装済み・KVM実測あり | `docs/evidence/kvm-20260915T080221Z/hello.json`・`timeout.json` の `evidence`、`docs/evidence/20260915T125610Z-firecracker/invocations.json`（11 attempt すべてで `guest_boot_id` が異なる）、E2E step 21 |
| 5 | `terminate_environment` が冪等で、`cleaned` に API socket / vsock uds / drive / workdir を列挙する | 実装済み・KVM実測あり（1 回目の terminate） / 未検証（KVM 上の 2 回目の terminate = M6） | `crates/providers/firecracker/src/provider.rs::{terminate_missing_env_is_idempotent_noop, terminate_cleans_a_stale_env_dir_and_archives_logs}`、`crates/providers/firecracker/tests/fake_vmm.rs::terminate_kills_a_running_vm_and_reports_cleanup`、`hello.json` の `terminate.cleaned`・`observation_after_terminate`・`leftovers` |
| 6 | baseline profile で boot / init / handler の時間を測定している | 実装済み・KVM実測あり（参考値。baseline profile ではない） / 未検証（x86_64、N ≥ 20、中央値・p95） | §「ADR-0001 残る測定の状況」M2〜M4、`docs/kvm.md` §5.5 |
| 7 | aarch64 で動作する | 実装済み・KVM実測あり（nested virtualization） | 上記の evidence はすべて aarch64。x86_64 は §「横断」 |

## PLT-4622 tenant 隔離・egress 起動ゲート・資源上限

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `Capabilities` で egress / resource / isolation を明示し、未測定は `Unverified` | 実装済み・KVM実測あり | `crates/provider-port/src/execution.rs`、`crates/providers/firecracker/src/provider.rs::capabilities_are_explicit`、`crates/providers/process/src/lib.rs::capabilities_are_dev_only_and_explicit`、`crates/providers/fake/src/lib.rs::capabilities_are_dev_only`、各 E2E の `provider.json` と step 04 |
| 2 | revision の resource / timeout / env の範囲検査 | 実装済み | `crates/domain/src/revision.rs::invalid_specs_are_rejected` |
| 3 | `TACHYON_` prefix の env と重複を拒否する | 実装済み | 同上 |
| 4 | P1 は tap を作らず egress none。`Restricted` / `PublicWeb` を要求する revision は拒否 | 実装済み・KVM実測あり（M8: 3 宛先すべて `NetworkUnreachable`、DNS 解決せず、interface は loopback のみ。`docs/evidence/isolation-20260916T020934Z/`） | `crates/providers/firecracker/src/provider.rs::create_rejects_non_none_egress`、capability `egress_restricted` / `egress_public_web` = unsupported。注: revision の作成・validate は `restricted` / `public-web` を受理して `Ready` にし、拒否は invoke 時の環境作成で起きる（受入条件の「revision を拒否」とは異なる）。process provider は host の network を共有する |
| 5 | vCPU / memory が `machine-config` に反映され、超過 alloc が `Crash` に分類される | 実装済み・KVM実測あり（M9: guest の vCPU 1 = 要求 1、MemTotal 232 MiB / 要求 256 MiB、128 MiB 環境で 512 MiB 確保は 80 MiB 到達後に `crash` / `Runtime.Crash`。`docs/evidence/isolation-20260916T020934Z/`） | `hello.json` の `evidence.details.{vcpus, mem_mib}`（1 / 256）、capability `enforce_resource_limits` = unverified。cgroup 等による host 側の資源強制は無い |
| 6 | 1 environment = 1 tenant × 1 revision × 1 attempt、破棄後に再利用しない | 実装済み・KVM実測あり | `crates/domain/src/revision.rs`（`ExecutionPolicy`）、`crates/application/tests/pipeline.rs::happy_path_records_timings_evidence_secrets_and_cleanup`、`docs/evidence/20260915T125610Z-firecracker/gateway.log`（invoke ごとに別の env id で `firecracker spawned` → `environment terminated`） |
| 7 | jailer / 専用ユーザーの検討 | 未着手 | `docs/threat-model.md` §14-2 |

## PLT-4623 実行 identity・Secret binding・準備完了ゲート

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `SecretProvider` が `SecretDeliveryContext{tenant, revision, environment, epoch}` 付きでしか解決しない | 実装済み | `crates/provider-port/src/secret.rs`、`crates/application/src/local_ports.rs::identity_and_secrets_are_tenant_scoped`、`crates/application/src/error.rs::foreign_and_missing_secret_bindings_are_indistinguishable`、`crates/application/src/services/invoke.rs::secret_binding_failures_do_not_reveal_other_tenants`（他 tenant の binding と存在しない binding は同じ `init_error` / `Host.SecretBindingUnavailable` になり、区別できない） |
| 2 | `SecretValue` の `Debug` が redact される | 実装済み（`SecretValue` の `Debug` 実装、`HostMessage::HelloAck`・`HelloAckParams`・設定の redact） / 実装済み（`SecretValue` 自体の `Debug` も `crates/provider-port/src/secret.rs::secret_value_debug_is_redacted` で検査） | `crates/provider-port/src/secret.rs`（`Debug` は `SecretValue(<redacted>)`）、`crates/protocol/src/wire.rs::hello_ack_debug_redacts_env`（`HelloAck.env` は `<N vars, redacted>`）、`crates/application/src/bridge_session.rs::hello_ack_params_debug_redacts_env`、`crates/application/src/config.rs::parses_dev_config_and_redacts_secrets` |
| 3 | secret 値が `HelloAck.env` にだけ載り、ログ・API・`state.json` に出ない | 実装済み（`HelloAck.env` に載る。`state.json`、host のログ、invocation のログ、bridge のログに出ない。E2E では gateway.log・evidence・`state.json` を demo.sh が検査） / 未検証（成功した invocation や revision の API 応答に値が出ないことを検査するテストなし） | `crates/application/tests/pipeline.rs::{happy_path_records_timings_evidence_secrets_and_cleanup, secret_values_never_reach_host_logs}`（host の tracing 出力と invocation ログ）、`crates/runtime-bridge/tests/roundtrip.rs::hello_ack_env_never_reaches_bridge_logs`（bridge の stderr と host へ送る frame）、E2E step 09（`secret_present: true`）。binding 解決失敗時の error body に他 tenant の値が出ないことは `crates/application/tests/pipeline.rs::unavailable_secret_binding_is_an_init_error_without_booting` |
| 4 | `Ready` を `init_deadline` まで待ち、超過は `InitError` / 502 | 実装済み・KVM実測あり（secret binding 解決失敗の `init_error` はテストのみ） | `crates/application/src/bridge_session.rs::init_error_timeout_and_disconnect`、`crates/application/tests/pipeline.rs::{user_error_panic_init_error_and_never_ready, unavailable_secret_binding_is_an_init_error_without_booting}`（binding を解決できなければ環境を起動せず `init_error` / 502）、`apps/gateway/tests/gateway_integration.rs::invoke_failures_map_to_status_codes`（`init_error` 502）、E2E step 12 |
| 5 | 静的 token / binding を gateway の設定ファイル（当初の記載は config/gateway.toml）から読む | 実装済み（ファイルは `config/gateway.dev.toml` / `config/gateway.firecracker.toml`） | `crates/application/src/config.rs::parses_dev_config_and_redacts_secrets`、`apps/gateway/tests/config_files.rs::{dev_config_loads, firecracker_config_loads}` |
| 6 | tachyon-apps `packages/secrets` への adapter | 未着手（P1 非対象） | `docs/inventory-tachyon-apps.md` §3.6 |

## PLT-4624 guest Runtime Bridge と protocol

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | frame codec（`u32 BE` 長 + JSON、8 MiB 上限、未知 type はエラー、未知 field は無視） | 実装済み | `crates/protocol/src/wire.rs::{frames_roundtrip_over_duplex, oversized_frame_rejected, unknown_fields_are_ignored_but_unknown_types_fail, response_payload_bound_leaves_room_for_the_envelope}`、`crates/runtime-bridge/src/session.rs::{write_loop_replaces_unencodable_frames_and_keeps_writing, session_ends_when_the_frame_writer_dies}`、`crates/runtime-bridge/src/runtime_api.rs::{oversized_response_is_413_and_error_frame, response_is_measured_as_canonical_json, response_limit_is_clamped_to_frame_capacity}`、`crates/runtime-bridge/tests/roundtrip.rs::{oversized_response_is_reported, response_larger_than_a_frame_is_reported_not_silenced}`（frame に収まらない Response は `Error{response_too_large}` に置き換え、session は止まらない）、`crates/application/src/config.rs::limits_must_leave_frame_headroom`、`crates/application/src/bridge_session.rs::oversized_invoke_is_refused_without_touching_the_session` |
| 2 | Runtime API の path / header / event type / DTO | 実装済み | `crates/protocol/src/runtime_api.rs::http_event_roundtrip`、`crates/runtime-bridge/src/runtime_api.rs::{next_delivers_headers_and_payload_then_response_completes, next_long_polls_until_dispatch, shutdown_answers_next_with_410, error_report_maps_panic_and_handler}` |
| 3 | bridge バイナリ（`--transport unix\|vsock`、`--init` モード、exit code 0/2/3/4） | 実装済み・KVM実測あり | `crates/runtime-bridge/src/cli.rs`、`crates/runtime-bridge/src/main.rs`（PID 1 なら `--init` を自動で有効化）、`crates/runtime-bridge/src/init.rs::parses_tachyon_keys`、`crates/runtime-bridge/tests/roundtrip.rs::{echo_roundtrip_then_shutdown, hello_reject_exits_with_2, init_error_exits_with_3, host_disconnect_kills_user_and_exits_4}`、`docs/evidence/kvm-20260915T080221Z/hello-console.txt`（vsock、init モード） |
| 4 | 1 in-flight、`Ready` は 1 回、in-flight 中の終了は `Error{crash}` | 実装済み・KVM実測あり | `crates/runtime-bridge/src/runtime_api.rs::{ready_is_emitted_once, second_dispatch_while_busy_is_rejected}`、`crates/runtime-bridge/tests/roundtrip.rs::{second_invoke_while_busy_is_protocol_error, panic_and_handler_error_are_reported, user_exit_before_ready_is_init_error}`、E2E step 11（panic → `crash`） |
| 5 | `Cancel{grace_ms}` で SIGTERM → grace → SIGKILL | 実装済み・KVM実測あり（Shutdown 時の grace と process provider の grace はテストのみ） | `crates/runtime-bridge/tests/roundtrip.rs::{cancel_kills_after_grace, bridge_sigterm_during_shutdown_kills_a_sigterm_ignoring_user_at_once}`、`crates/providers/process/src/lib.rs::{completed_terminate_lets_a_winding_down_bridge_exit_unsignalled, completed_terminate_kills_a_bridge_that_never_exits_after_the_grace, non_graceful_terminate_signals_without_waiting}`（provider の `TERMINATE_GRACE`（3 s）が bridge の Shutdown grace（2 s）より長いことを compile 時の assert で固定）、`docs/evidence/kvm-20260915T080221Z/timeout.json`（`cancel requested (grace 1000 ms)`、`cancel_sent: true`）、E2E step 18 |
| 6 | static musl ビルドで `/sbin/tachyon-init` として動く | 実装済み・KVM実測あり（aarch64） / 未検証（x86_64 での起動。CI は build だけ） | `scripts/kvm/bootstrap.sh`（musl ビルドと `PT_INTERP` 無しの確認）、`scripts/kvm/build-rootfs.sh`、`docs/evidence/kvm-20260915T080221Z/hello-console.txt` |

## PLT-4625 Rust handler SDK・axum HTTP アダプター・ローカル実行

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `run(handler)` が `tachyon.invoke.v1` を処理し、`Err` → `Handler.Error`、panic → `Runtime.Panic` | 実装済み・KVM実測あり（上限を超える error report はテストのみ） | `crates/sdk/src/lib.rs::{run_serves_one_event_then_stops_on_410, handler_error_and_panic_are_reported, oversized_handler_error_is_truncated_before_posting, error_reports_are_bounded}`、`crates/runtime-bridge/src/runtime_api.rs::oversized_error_report_settles_the_attempt`（上限超過の report は 413 を返すと同時に `Runtime.ErrorReportTooLarge` で attempt を完了する）、E2E step 09〜11 |
| 2 | `serve_http(router)` が `tachyon.http.v1` を `http::Request` に変換し `tower::Service` として呼ぶ（TCP を開かない） | 実装済み・KVM実測あり（percent-encoding を含む path はテストのみ） | `crates/sdk/src/http.rs::{get_root, status_and_query, echo_binary_body_keeps_content_type, repeated_headers_are_preserved, invalid_event_is_rejected, path_reaches_the_router_as_a_valid_encoded_uri, encode_path_borrows_conforming_paths}`、`apps/gateway/src/handlers.rs::adapter_path_keeps_percent_encoding_and_normalises_the_root`、`apps/gateway/tests/gateway_integration.rs::http_adapter_forwards_the_raw_request_path`（gateway は受け取った percent-encoded の path をそのまま渡し、`%20`・`%2F`・`%3F` で経路が変わらない）、`examples/http-axum/src/main.rs::routes_answer_through_the_sdk_adapter`、E2E step 13〜16 |
| 3 | 非 http event を `serve_http` に渡すと `Runtime.UnsupportedEvent` | 実装済み | `crates/sdk/src/lib.rs::serve_http_rejects_non_http_events` |
| 4 | `TACHYON_UNISOLATED=1` を SDK が読める | 実装済み | `crates/protocol/src/lib.rs`（`env::UNISOLATED`）、`crates/sdk/src/lib.rs`（`is_unisolated`）、`crates/application/tests/pipeline.rs::happy_path_records_timings_evidence_secrets_and_cleanup`（dev_only provider で `TACHYON_UNISOLATED=1` を渡す）、E2E の `invocations.json`（process は `unisolated: true`、firecracker は `false`） |
| 5 | ローカル実行（`tsls dev`）で process provider を使って往復できる | 実装済み（単体テスト） / 未検証（`tsls dev` を実行した記録が evidence に無い。E2E は gateway を起動して `tsls functions ...` を使う） | `apps/cli/src/commands/dev.rs::{config_renders_valid_toml_matching_the_documented_schema, missing_sibling_is_usage_error}`、`docs/cli.md` |

## PLT-4626 同期 Invoke Gateway

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `POST /v1/functions/{id}:invoke` と `ANY /v1/functions/{id}/http/{*path}` | 実装済み・KVM実測あり | `apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`、`apps/cli/tests/mock_gateway.rs::invoke_query_and_colon_routes`、E2E step 09〜18 |
| 2 | alias → revision の解決と受付時の固定、`revision_not_ready` / `function_deleted` は 409 | 実装済み | `crates/application/tests/pipeline.rs::revision_is_pinned_at_accept_even_if_alias_changes_mid_flight`、`crates/application/tests/pipeline.rs::alias_cas_conflict_and_rollback`（`RevisionNotReady`）、`crates/application/tests/pipeline.rs::cross_tenant_resources_are_not_found`（`FunctionDeleted`）、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（409 `function_deleted`） |
| 3 | payload 上限 413、容量超過 429、queue 超過 504 | 実装済み / 未検証（KVM での同時実行 = M10） | `apps/gateway/tests/gateway_integration.rs::invoke_failures_map_to_status_codes`（413）、`crates/application/tests/pipeline.rs::capacity_exceeded_and_queue_timeout`（429 / 504） |
| 4 | Idempotency-Key（一致は既存を返し再実行しない、不一致は 409） | 実装済み | `crates/application/tests/pipeline.rs::{idempotency_replay_and_conflict, capacity_rejection_does_not_consume_the_idempotency_key, concurrent_requests_with_the_same_key_run_once}`、`crates/application/src/repository.rs::idempotency_key_is_bound_with_its_invocation`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`。429 / 400 で終わった key と再起動時の扱いは PLT-4614 #6 |
| 5 | `InvocationResponse` に attempts / timings / boot_evidence / deadlines が入る | 実装済み・KVM実測あり | `crates/application/tests/pipeline.rs::happy_path_records_timings_evidence_secrets_and_cleanup`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`、E2E step 21、`docs/evidence/20260915T125610Z-firecracker/invocations.json` |
| 6 | client 切断後も deadline まで追跡して記録する | 実装済み（コード: driver を request とは別の task で実行、client deadline の enforcement） / 未検証（client 切断を検査するテストなし） | `crates/application/src/services/invoke.rs`（`tokio::spawn(driver.run(..))`）。client deadline は PLT-4614 #4 のテスト |

## PLT-4627 ホスト強制 timeout・cancel・結果不明・後始末

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | Lease の `(attempt_id, epoch)` 不一致・解放後の結果を拒否する | 実装済み | `crates/domain/src/environment.rs::lease_fencing`、`crates/application/src/bridge_session.rs::full_session_with_lease_fencing_and_log_bounds` |
| 2 | `execution_deadline` 到達で `Cancel` → grace 1 s → `terminate` → `Failed{Timeout}` / 504 | 実装済み・KVM実測あり | `crates/application/tests/pipeline.rs::hang_times_out_cancels_and_terminates_with_timeout`、`docs/evidence/kvm-20260915T080221Z/timeout.json`（`outcome=timeout`、`terminate.was_running=true`、`leftovers.process_alive=false`）、E2E step 18（`timeout_seconds = 2`、504 / exit 4、壁時計 < 10 s）。M5 の条件での単独測定は §「ADR-0001 残る測定の状況」 |
| 3 | `POST :cancel` → `Cancelled` / 499、cancel 自体は 202 | 実装済み・KVM実測あり（cancel API の応答は 202 ではなく 200 `InvocationResponse`） | `apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（cancel は 200 `cancelled`、待っていた invoke は 499、2 回目の cancel も 200）、`crates/application/tests/pipeline.rs::capacity_exceeded_and_queue_timeout`、`apps/cli/tests/mock_gateway.rs::cancel_routes`、E2E step 24、`docs/api.md` |
| 4 | 接続断で `OutcomeUnknown`、自動再実行しない、環境 terminate | 実装済み / 未検証（KVM で firecracker を外から kill = M11） | `crates/domain/src/invocation.rs::outcome_unknown_only_after_running`、`crates/application/tests/pipeline.rs::{disconnect_after_invoke_is_outcome_unknown, guest_exit_before_the_invoke_is_delivered_is_a_crash_not_outcome_unknown}`、`crates/application/src/bridge_session.rs::{init_error_timeout_and_disconnect, failed_invoke_write_still_reads_what_the_guest_queued}`、`crates/application/src/services/invoke.rs::undelivered_invoke_is_never_outcome_unknown`（Invoke frame が guest に届いていない失敗は `outcome_unknown` にせず、`Host.InvokeTooLarge`（`platform_error`）、`Runtime.Exited` / `Host.BridgeDisconnectedBeforeInvoke`（`crash`）に分類。`docs/threat-model.md` §9） |
| 5 | `terminate_environment` が冪等で `cleaned` を列挙、orphan 0 | 実装済み・KVM実測あり / 未検証（KVM 上の 2 回目の terminate = M6） | `crates/provider-port/src/execution.rs`（`TerminateReport`）、PLT-4621 #5 のテスト、`crates/application/tests/pipeline.rs::driver_panic_after_environment_creation_still_terminates_it`（環境作成後に driver が panic しても `terminate(Crashed)` し、環境を `Failed` にする）、`crates/providers/process/src/lib.rs::{orphan_directory_is_listed_and_cleaned, pid_file_rejects_pids_that_address_groups, argv_matcher_requires_an_exact_element, procargs2_buffer_yields_argv_only, stale_pid_file_never_kills_an_unrelated_process, orphaned_bridge_with_matching_argv_is_terminated}`（古い pid file の pid は、argv に environment id がある場合だけ signal する。後ろの 2 テストは Linux / macOS のみ）、process provider の grace は PLT-4624 #5 のテスト、`crates/providers/process/tests/roundtrip.rs::{create_invoke_terminate_roundtrip, connect_timeout_kills_and_cleans_up}`、E2E step 19・27（`scripts/e2e/orphan-check.sh`）、fc-smoke の `leftovers` |
| 6 | gateway 再起動時に Running を `OutcomeUnknown` に reconcile し、`list_environments` の孤児を terminate する | 実装済み / 未検証（KVM 実機での孤児回収。fake provider のテストのみ） | `crates/application/src/repository.rs::restart_separates_dispatched_work_from_work_that_never_started`、`crates/application/tests/pipeline.rs::{startup_reconcile_terminates_orphans_and_spares_live_environments, startup_reconcile_marks_environments_the_provider_no_longer_has, startup_reconcile_never_blocks_startup_on_a_provider_error, startup_reconcile_can_be_turned_off}`、`apps/gateway/tests/gateway_integration.rs::bootstrap_converges_on_a_state_file_left_behind_by_a_crash`、`crates/application/src/services/reconcile.rs` |
| 7 | timeout / 強制終了した環境を再利用しない | 実装済み（P1 は再利用なし） | `docs/architecture.md` §1 |

## PLT-4628 Invocation 単位のログ・履歴・トレース

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | log 行を 16 KiB で char boundary 切り、`truncated` を立てる | 実装済み | `crates/domain/src/log.rs::bounded_line_respects_char_boundaries`、`crates/runtime-bridge/src/process.rs::truncates_long_lines_without_buffering_them` |
| 2 | invocation ごとに 2000 行 / 1 MiB で打ち切り、`LogsResponse.dropped = true` | 実装済み | `crates/domain/src/limits.rs`、`crates/application/src/repository.rs::logs_are_bounded_per_invocation`、`crates/application/src/bridge_session.rs::full_session_with_lease_fencing_and_log_bounds` |
| 3 | `phase`（boot / init / handler / shutdown）と `stream`（stdout / stderr / platform）を保持する | 実装済み | `crates/domain/src/log.rs`、`apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`（`handler`/`stdout` と `boot`/`platform` の行）、`crates/runtime-bridge/src/process.rs::init_phase_before_ready` |
| 4 | `GET /v1/invocations/{id}/logs`、`GET /v1/functions/{id}/invocations` | 実装済み・KVM実測あり | `apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`、`apps/cli/tests/mock_gateway.rs::{logs_render_markers, invocation_detail_and_history}`、E2E step 20・21 |
| 5 | `trace_id` が invoke → Invoke frame → Runtime API header → 応答 header まで伝播する | 実装済み（コード） / 未検証（通しで検査するテストなし） | `crates/runtime-bridge/src/runtime_api.rs::next_delivers_headers_and_payload_then_response_completes`（Runtime API の trace header）、`apps/gateway/src/handlers.rs`、E2E step 09 の `trace=` 出力 |
| 6 | secret 値がログに出ない | 実装済み | `crates/application/tests/pipeline.rs::secret_values_never_reach_host_logs`（host の tracing 出力と invocation ログ）、`scripts/e2e/demo.sh` の「secret values absent from gateway log, evidence and state」ステップ（`docs/evidence/20260915T171415Z-process/`、`docs/evidence/20260915T171631Z-firecracker/` で PASS）、`crates/runtime-bridge/tests/roundtrip.rs::hello_ack_env_never_reaches_bridge_logs`（bridge の stderr）、`crates/protocol/src/wire.rs::hello_ack_debug_redacts_env`。PLT-4623 #3 も参照 |

## PLT-4629 CLI functions deploy / invoke / logs / rollback / dev

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `tsls deploy`（artifact upload → revision → alias） | 実装済み・KVM実測あり | `apps/cli/tests/mock_gateway.rs::{deploy_uploads_polls_and_reads_alias, deploy_no_wait_returns_after_create, deploy_missing_binary_is_usage_error}`、`apps/cli/src/commands/deploy.rs::{builds_request_from_args, rejects_bad_env}`、E2E step 06〜08、`docs/cli.md` |
| 2 | `tsls invoke`（結果、`invocation_id`、provider が dev_only なら警告） | 実装済み | `apps/cli/tests/mock_gateway.rs::{invoke_success_prints_output_and_invocation_id, invoke_error_classes_map_to_exit_codes, invoke_warns_on_stderr_for_a_dev_only_provider, invoke_does_not_warn_for_an_isolated_provider, failed_provider_lookup_warns_without_changing_the_invoke_result, http_adapter_warns_for_a_dev_only_provider}`（`tsls functions invoke` / `http` は provider が dev_only なら stderr に警告し、stdout と exit code は変えない。provider を取得できない場合もその旨を警告する）、`docs/adr/0002-process-provider-dev-only.md` §「決定」4 |
| 3 | `tsls logs` | 実装済み・KVM実測あり | `apps/cli/tests/mock_gateway.rs::logs_render_markers`、`apps/cli/src/commands/logs.rs::formats_lines_and_markers`、E2E step 20 |
| 4 | `tsls rollback`（`previous_revision_id` へ CAS 更新） | 実装済み・KVM実測あり | `apps/cli/tests/mock_gateway.rs::{rollback_uses_previous_revision_and_cas_generation, rollback_to_explicit_revision_and_conflict}`、E2E step 22 |
| 5 | `tsls dev`（process provider でローカル往復） | 実装済み（単体テスト） / 未検証（実行記録なし） | PLT-4625 #5 と同じ |
| 6 | CLI は HTTP 経由でのみ application に触れる | 実装済み | `apps/cli/Cargo.toml`（内部 crate への依存は `tachyon-serverless-api-types` だけ）、`docs/architecture.md` §2。`tsls dev` も gateway バイナリを子プロセスとして起動し HTTP で話す |

## PLT-4630 Rust サンプル 3 種と E2E デモ

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | `examples/hello`（JSON invoke） | 実装済み・KVM実測あり | `examples/hello/src/main.rs`、E2E step 06・09〜11、`docs/evidence/kvm-20260915T080221Z/hello.json` |
| 2 | `examples/http-axum`（`serve_http`） | 実装済み・KVM実測あり | `examples/http-axum/src/main.rs::routes_answer_through_the_sdk_adapter`、E2E step 07・13〜16 |
| 3 | `examples/cpu-burn`（timeout kill の実演） | 実装済み・KVM実測あり | `examples/cpu-burn/src/main.rs`、E2E step 08・17・18、`docs/evidence/kvm-20260915T080221Z/timeout.json` |
| 4 | E2E スクリプト: deploy → invoke → logs → rollback → 環境破棄を process provider と Firecracker provider の両方で通す | 実装済み・KVM実測あり | `scripts/e2e/demo.sh`、`docs/evidence/20260915T073238Z-process/summary.json`（27/27）、レビュー指摘修正の統合後の `docs/evidence/20260915T171415Z-process/summary.json`（28/28）と `docs/evidence/20260915T171631Z-firecracker/summary.json`（28/28）、`docs/evidence/20260915T125610Z-firecracker/summary.json`（27/27）。環境破棄は step 19・27 |
| 5 | Firecracker provider での実行証跡（`BootEvidence.guest_boot_id`、`AttemptTimings`、`GET /v1/provider`）を残す | 実装済み・KVM実測あり | `docs/evidence/20260915T125610Z-firecracker/provider.json`、`docs/evidence/20260915T125610Z-firecracker/invocations.json`、`docs/evidence/20260915T125610Z-firecracker/steps/21-history_shows_attempts_with_boot_evidence.log` |
| 6 | process provider の結果を microVM の証跡として使わない | 実装済み（規則、process の `boot_evidence` に `guest_boot_id` が無い、CLI の dev_only 警告） | `docs/adr/0002-process-provider-dev-only.md`、`apps/cli/tests/mock_gateway.rs::{invoke_warns_on_stderr_for_a_dev_only_provider, http_adapter_warns_for_a_dev_only_provider}`、`docs/adr/0001-execution-provider-firecracker-first.md` §「受入規則」、`docs/evidence/20260915T073238Z-process/invocations.json` |

---

## ADR-0001 残る測定の状況

測定の定義は `docs/adr/0001-execution-provider-firecracker-first.md` §「残る測定」。値はすべて aarch64 の nested virtualization 上の参考値（§「証跡」の制約を参照）。

| # | 状態 | 記録 |
|---|---|---|
| M1 | 実装済み・KVM実測あり | `docs/evidence/kvm-20260915T080221Z/hello.json` と `docs/evidence/20260915T125610Z-firecracker/provider.json` の `preflight`（9 項目 ok）。nested virtualization の有無は `PreflightReport` の項目に無く、`docs/kvm.md` §5 に記録 |
| M2 | 実装済み・KVM実測あり（参考値） / 未検証（N ≥ 20、中央値・p95） | gateway 経由（E2E、11 attempt）: `environment_boot_ms` 3522〜4754。fc-smoke（2 回）: `boot_ms` 13468、11036 |
| M3 | 同上 | gateway 経由: `runtime_init_ms` 331〜410（guest 申告 `guest_init_ms` 219〜274）。fc-smoke: `init_ms` 1341、1224 |
| M4 | 同上 | gateway 経由の hello / http-axum: `handler_ms` 73〜93、`total_ms` 4296〜5355。fc-smoke の hello: `handler_ms` 319、`total_ms` 16337 |
| M5 | 実装済み・KVM実測あり（条件は ADR と異なる） | E2E step 18（`timeout_seconds = 2`）: `handler_ms` 2002、`total_ms` 7178、`Failed{Timeout}`、orphan 0。fc-smoke の timeout demo（deadline 3 s）。ADR の条件（`timeout_seconds = 5`、deadline + grace から terminate 完了まで ≤ 2 s）での単独測定は無い |
| M6 | 未検証 | KVM 上で 2 回目の `terminate_environment` を呼んだ記録が無い。単体テストは `crates/providers/firecracker/src/provider.rs::terminate_missing_env_is_idempotent_noop` |
| M7 | 実装済み・KVM実測あり | E2E step 19・27（13 invocation の後に `orphan-check: clean (firecracker)`）、fc-smoke の `leftovers` |
| M8 | 実測済み | 3 宛先（1.1.1.1:443 / 169.254.169.254:80 / 10.0.2.2:80）すべて `NetworkUnreachable`、DNS 解決なし、interface は loopback のみ、default route 0。`docs/evidence/isolation-20260916T020934Z/egress.json` |
| M9 | 実測済み（ephemeral storage を除く） | guest vCPU 1、MemTotal 232 MiB（要求 256 MiB）、128 MiB 環境での 512 MiB 確保は `crash` / `Runtime.Crash`。ephemeral storage は依然として未強制。`docs/evidence/isolation-20260916T020934Z/resources.json`、`docs/evidence/isolation-20260916T020934Z/alloc-invocation.json` |
| M10 | 未検証 | fake provider の `crates/application/tests/pipeline.rs::capacity_exceeded_and_queue_timeout` だけ |
| M11 | 未検証 | fake provider の `crates/application/tests/pipeline.rs::disconnect_after_invoke_is_outcome_unknown` だけ |
| M12 | 実装済み・KVM実測あり（nested virtualization） | 上記の evidence はすべて aarch64 |
| M13 | 未検証 | 1 MiB payload / 6 MiB response の転送時間は未測定 |

## 横断: 未検証・未着手

| 項目 | 状態 | 理由 |
|---|---|---|
| x86_64 KVM host での実行（`docs/inventory-tachyon-apps.md` §6 の第一 profile） | 未検証 | 実測は Apple M4 上の aarch64 nested virtualization だけ。CI は x86_64 musl の build だけで起動しない |
| bare metal（nested virtualization なし）での測定 | 未検証 | 同上 |
| baseline profile どおりの測定（N ≥ 20、中央値・p95、hello / http-axum / cpu-burn） | 未検証 | 記録は E2E 1 回分（11 attempt）と fc-smoke 2 回 |
| 別開発者・別 host による追試 | 未着手 | 記録は 1 人・1 host |
| self-hosted KVM runner での `.github/workflows/kvm-integration.yml` | 未着手 | runner が未用意（workflow のコメント） |
| TiDB 永続化と migration（PLT-4618） | 未着手 | P1 非対象 |
| egress 制御（restricted / public-web）と、cgroup 等による host 側の資源強制（PLT-4622） | 未着手 | P1 は NIC を付けない egress none と `machine-config` だけ。その実効性（M8・M9）も未検証 |
| Kata / Cloud Hypervisor adapter | 未着手 | ADR-0001 で後続 adapter と決めた |
| OCI image の pull・実行 | 未着手 | P1 非対象（参照の受理と理由付き `Failed` だけ） |
| jailer / 専用ユーザー | 未着手 | `docs/threat-model.md` §14-2 |

## 更新ルール

- 各 PR で該当行の状態と証跡を更新する。「実装済み」にするときはテスト名・ファイルが実在することを、「KVM実測あり」にするときは `docs/evidence/` の該当ディレクトリを PR で示す。
- 実機の記録の置き場所: `scripts/e2e/demo.sh` は `docs/evidence/<UTC>-<provider>/`、`scripts/kvm/smoke.sh` は `docs/evidence/kvm-<UTC>/` に書く。profile（host arch、nested virtualization の有無、vCPU / memory、kernel / rootfs の sha256）を読み取れるようにする。
