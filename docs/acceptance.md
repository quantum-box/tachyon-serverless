# 受入チェックリスト（PLT-4613〜PLT-4633、PLT-4636、PLT-4645、PLT-4651 X1）

- 対象: Linear プロジェクト「Tachyon Serverless — 動作プロトタイプ」P0〜P1 と、P2 のうち着手済みの PLT-4631、PLT-4632、PLT-4633
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
| `docs/evidence/20260917T024812Z-process/` | `scripts/e2e/demo.sh`（PLT-4651 の SDK / bridge 変更後の P1 互換確認。設定は下の行と同じく listen・data_dir・workdir だけを変えたコピー） | macOS、process provider（隔離なし） | 28/28 PASS |
| `docs/evidence/restore-aware-20260917T025043Z-process/` | `examples/restore-aware`（PLT-4651、実験）を gateway + process provider で deploy し、`{"n":97}` / `{"n":91}` と、`RESTORE_AWARE_FAIL=bootstrap` / `after_restore` の revision を invoke した結果・ログ | macOS、process provider（隔離なし） | 成功 2（`restored=false`）、`Runtime.PreCheckpointFailed` 1、`Runtime.AfterRestoreFailed` 1 |
| `docs/evidence/20260917T020229Z-process/` | `scripts/e2e/demo.sh`（PLT-4618: 台帳が `state.db`。step 27 は `state.db` と `state.db-wal` も検査。port 8080 が使用中のため `config/gateway.dev.toml` の listen と data_dir だけを変えたコピーを `TSLS_GATEWAY_CONFIG` で指定） | macOS、process provider（隔離なし） | 28/28 PASS |
| `docs/evidence/20260917T034846Z-process/` | `scripts/e2e/demo.sh`（PLT-4631: dispatcher の登録・slot の acquire / complete・heartbeat 付きの gateway での P1 互換確認。`gateway.log` に `dispatcher registered`、`startup reconcile finished` に `foreign` / `reclaimed_dispatchers` / `fenced_*`。設定は listen・data_dir・workdir だけを変えたコピー） | macOS、process provider（隔離なし） | 28/28 PASS |

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
| 5 | 本番 cluster・remote に触れない | 実装済み（規則） | `docs/inventory-tachyon-apps.md` §3.12、`docs/adr/0001-execution-provider-firecracker-first.md` §「決定」、`.github/workflows/kvm-integration.yml`（PLT-4645 以降: fork PR では KVM job を起動しない、`permissions: contents: read`、secret なし。`docs/ci.md` §4） |

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
| 5 | CI（fmt / clippy `-D warnings` / test）が PR で走る | 実装済み（workflow 定義） / 未検証（この更新では GitHub Actions 上の実行結果を確認していない） | `.github/workflows/ci.yml`（fmt / clippy / test / build、x86_64 musl の guest ビルドと static 確認、`bash -n`・shellcheck・`scripts/e2e/selftest.sh`）。KVM 統合と変更範囲別 gate は PLT-4645（`docs/ci.md`）。self-hosted KVM runner は未用意 |
| 6 | 依存の追加はルート `[workspace.dependencies]` 経由のみ | 実装済み（規約） / 未検証（自動検査なし） | `Cargo.toml`。検査するテストは存在しない |

## PLT-4618 Function・Invocation・実行環境のモデルと DB migration

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | ID が `<prefix>_<26 文字 lowercase ULID>` で、prefix 不一致・大文字・短い ULID を拒否する | 実装済み | `crates/domain/src/ids.rs::{generated_ids_have_prefix_and_lowercase_ulid, wrong_prefix_is_rejected, uppercase_or_short_ulid_is_rejected, existing_tenant_ids_parse, serde_roundtrip_validates}` |
| 2 | Function / Revision / Alias の状態遷移と不変性（`spec_digest`、generation CAS） | 実装済み | `crates/domain/src/revision.rs::{valid_spec_passes_and_digest_is_stable, invalid_specs_are_rejected, revision_lifecycle_and_terminal_rejection, failure_allowed_from_any_non_terminal_state}`、`crates/domain/src/alias.rs::cas_update_and_rollback_pointer` |
| 3 | Invocation / Attempt の状態遷移（terminal 後の更新拒否、`OutcomeUnknown` は Running からのみ） | 実装済み | `crates/domain/src/invocation.rs::{happy_path, cannot_succeed_before_running, outcome_unknown_only_after_running, queue_timeout_fails_from_queued, attempt_terminal_once}` |
| 4 | ExecutionEnvironment / Lease（状態遷移、`(attempt_id, epoch)` fencing） | 実装済み | `crates/domain/src/environment.rs::{lifecycle, failure_from_any_state_and_lost, lease_fencing}` |
| 5 | repositories と永続化（`state.db`） | 実装済み | `crates/application/src/repository/contract_tests.rs` を `memory::*` と `sqlite::*` の両方で実行、`crates/application/src/repository/sqlite/tests.rs::{persistence_roundtrip_and_restart_reconcile, restart_separates_dispatched_work_from_work_that_never_started}`。P1 の `state.json` write-through は廃止（下の「PLT-4618 永続化（2026-09-17）」） |
| 6 | DB migration | 実装済み（埋め込み SQLite、前進のみ） / 未着手（TiDB。ADR-0003 で SQLite を選び、TiDB は将来の adapter） | `crates/application/src/repository/sqlite/migrations/`、`docs/adr/0003-execution-state-persistence.md`「実装メモ」 |
| 7 | `env_` prefix の tachyon-apps との衝突を解消する方針を決める | 方針決定済み（`docs/adr/0003-execution-state-persistence.md`） | `docs/inventory-tachyon-apps.md` §3.1, §7-1 |

### PLT-4618 永続化（2026-09-17、`feat/plt-4618-sqlite`）

ADR-0003 の決定 2〜4 と移行の実装。テストは `cargo test -p tachyon-serverless-application --lib repository` で再現する。契約テスト（`contract_tests.rs`）は同じ関数を `repository::contract_tests::memory::<名前>` と `repository::contract_tests::sqlite::<名前>` の 2 回実行する。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 作成 / 復元 / 状態遷移の unit test | 実装済み | `contract_tests.rs::{an_invocation_is_created_restored_and_driven_to_a_final_state, a_revision_cannot_be_tampered_with_and_its_final_status_is_final, attempts_and_leases_are_final_once_settled, an_environment_copy_from_another_epoch_is_refused}`、`sqlite/tests.rs::persistence_roundtrip_and_restart_reconcile`（file を閉じて開き直した後の復元） |
| 2 | terminal 再更新拒否の property test | 実装済み | `contract_tests.rs::terminal_rows_are_never_rewritten_property`（64 seed × 24 step の遷移と古い snapshot の再書き込み。外部 crate を使わない決定的な生成器）。invocation のみが property test で、attempt / environment / lease / revision は例示テスト |
| 3 | alias 更新競合を拒否する | 実装済み | `contract_tests.rs::{alias_updates_are_compare_and_set, concurrent_alias_updates_have_exactly_one_winner}`、`sqlite/tests.rs::cas_holds_across_separate_connections_to_the_same_file`（同じ file に別 connection を持つスレッド）、`crates/application/tests/pipeline.rs` の alias 更新テスト（`expected_generation` 不一致で 409）。**OS プロセスを分けた競合テストは無い**（未検証、PLT-4631） |
| 4 | tenant 越境を拒否する | 実装済み | `contract_tests.rs::{rows_never_cross_a_tenant, idempotency_key_is_bound_with_its_invocation, artifact_ownership_is_per_tenant}`（他 tenant の function の revision / invocation、他 tenant の revision を指す alias、他 tenant の invocation の attempt、他 tenant の revision の environment、reuse key の tenant 不一致、他 tenant の environment の lease、tenant の書き換え）。API 層の越境は既存の `pipeline.rs` / `gateway_integration.rs` |
| 5 | ID 重複を拒否する | 実装済み | `contract_tests.rs::{duplicate_ids_are_refused_for_every_entity, revision_numbers_are_allocated_and_unique_per_function, function_name_unique_per_tenant}` |
| 6 | revision 改変を拒否する | 実装済み | `contract_tests.rs::a_revision_cannot_be_tampered_with_and_its_final_status_is_final`（spec / spec_digest / number / function / tenant の変更、Ready 後の状態変更を拒否） |
| 7 | 空 DB へ migration を適用できる | 実装済み | `sqlite/tests.rs::migrations_apply_to_an_empty_database`、E2E（`docs/evidence/20260917T020229Z-process/gateway.log` の `state store opened` `migrations_applied=[1, 2]`） |
| 8 | 既存 DB（古い schema）へ migration を適用できる | 実装済み | `sqlite/tests.rs::{migrations_upgrade_a_database_at_an_older_version, a_database_newer_than_the_binary_is_refused, a_failing_migration_leaves_the_previous_schema_intact}` |
| 9 | 既存の `state.json` 台帳を DB に移行できる | 実装済み | `sqlite/tests.rs::{a_p1_state_json_is_imported_once_and_moved_aside, an_interrupted_import_only_finishes_the_rename, a_state_json_next_to_a_populated_database_is_refused, corrupt_state_file_is_refused_with_a_hint, state_without_artifact_owners_still_loads}`。手動確認: 開発機の P1 `data/state.json`（210 KiB、function 3 / revision 27 / invocation 54 / attempt 36 / environment 54）の**コピー**を gateway で 2 回起動し、1 回目で全行を取り込み rename、2 回目は再 import しないことを確認（自動化していない） |
| 10 | expand / contract と index 設計のレビュー | 実装済み（設計と規則の文書化、index 利用のテスト） / 未検証（第三者レビュー、10k 行での計測） | `sqlite/migrations/001_initial.sql` 冒頭の規則と各 index の用途コメント、`sqlite/migrations.rs` の expand → 切り替え → contract の規則、`002_output_retention.sql`（expand のみ）、`sqlite/tests.rs::pool_lookups_use_their_indexes`（`EXPLAIN QUERY PLAN`）。contract migration の実例はまだ無い |
| 11 | 入出力本文を無制限に DB 保存しない（参照・digest・保持期限） | 実装済み | 入力は digest とサイズのみ。出力は `[invoke] inline_output_max_bytes` 以下だけ本文、store も `limits.max_response_bytes` 超を拒否（`contract_tests.rs::inline_output_is_bounded`）。`[store] output_retention_seconds`（既定 7 日）後に digest へ置換（`sqlite/tests.rs::inline_output_is_replaced_by_its_digest_after_retention`）。置換後は replay / 詳細 API が出力本文を返さない |
| 12 | secret を DB に保存しない | 実装済み | domain の行に secret 値の field が無い。`pipeline.rs::happy_path_records_timings_evidence_secrets_and_cleanup`（`state.db` と `-wal` の bytes に secret 値が無い）、E2E step 27（`state.db` / `state.db-wal` を検査） |
| 13 | DB file の権限 | 実装済み（新規作成時 0600） / 未検証（Linux 実機） | `sqlite/tests.rs::the_database_file_is_private_to_its_owner`（macOS で実行）、`docs/threat-model.md` §14-4 |
| 14 | TiDB | 未着手 | ADR-0003 で単一 host は SQLite と決定。TiDB 互換は主張しない。変わる点は ADR-0003「TiDB（MySQL protocol）adapter にするときに変わるもの」 |
| 15 | 同じ `data_dir` を複数 gateway が同時に使う / lease の期限評価（ADR-0003 A1〜A4, A6, A7） | 実装済み（A1・A2・A3・A6、PLT-4631） / 未着手（A4、意図的） / 未検証（A7） | 下の「PLT-4631」節と ADR-0003「実装メモ（PLT-4631）」 |

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
| 2 | revision の resource / timeout / env の範囲検査 | 実装済み | `crates/domain/src/revision.rs::invalid_specs_are_rejected`（`ephemeral_storage_mib` は PLT-4622 で下限 32 を追加し 32..=2048。CLI は `--ephemeral-storage-mib`） |
| 3 | `TACHYON_` prefix の env と重複を拒否する | 実装済み | 同上 |
| 4 | egress `none` は tap を作らず NIC なし | 実装済み・KVM実測あり（M8: 3 宛先すべて `NetworkUnreachable`、DNS 解決せず、interface は loopback のみ。`docs/evidence/isolation-20260916T020934Z/`、再計測 `docs/evidence/isolation-20260917T011555Z/`、`docs/evidence/isolation-20260917T031126Z/` でも同じ） | `crates/providers/firecracker/src/egress_gate.rs::{plans_with_networking_fail_closed_without_a_policy, a_vm_config_without_nic_passes}`。process provider は host の network を共有する（egress はどれも unsupported） |
| 4a | egress `none` / `restricted` / `public-web` を default-deny で適用する（PLT-4622 の本題） | 実装済み・KVM実測あり（aarch64 nested 1 host。`docs/evidence/isolation-20260917T031126Z/`、NET PASS） | 設計 `docs/adr/0005-egress-profiles.md`。環境ごとの tap と `table inet tachyon_egress`（`crates/providers/firecracker/src/network.rs`）。allowlist は `RevisionSpec.egress_allow`（`crates/domain/src/egress.rs::allow_rules_refuse_special_ranges_empty_ports_and_ipv6`、`crates/domain/src/revision.rs::egress_allowlists_are_validated_per_profile`、CLI `--egress` / `--egress-allow`）。規則の生成と読み戻し `network.rs::{public_web_denies_special_ranges_and_foreign_dns_before_accepting, restricted_accepts_only_the_allowlist_and_ends_in_drop, base_script_is_default_deny_for_taps, read_back_fails_closed_on_any_divergence}`。capability `egress_restricted` / `egress_public_web` = supported（権限の無い host では理由付き unsupported: `provider.rs::network_capabilities_follow_the_host`、`create_rejects_non_none_egress_where_it_cannot_be_enforced`）。実測: public-web は 1.1.1.1 / 1.0.0.1:443 と resolver 経由の DNS が成功し拒否対象 16/16 が失敗、restricted（`1.1.1.1/32:443`）は許可 1 件だけ成功し 11/11 が失敗（`net-checks-{publicweb,restricted}.json`）。hostname の allowlist は未実装（ADR-0005 §決定 7）。帯域の上限なし |
| 4b | 管理網・node・metadata（169.254.0.0/16）・link-local・RFC1918 / CGNAT / loopback と IPv6 を遮断 | 実装済み・KVM実測あり | `domain::BLOCKED_IPV4` / `BLOCKED_IPV6` を host の集合として全 profile に適用し、node への `input` と tap への `output` を drop。実測: 169.254.169.254:80、10.0.2.2、100.64.0.1、192.168.0.1、管理網の gateway 192.168.5.2（:22 / :53 / UDP DNS）、node 192.168.5.15（:22 は host から open、:8080）、node の tap 172.30.0.1 / .5 がすべて timeout。IPv6（`[2606:4700:4700::1111]:443`、`[::ffff:169.254.169.254]:80`）は guest の `ipv6.disable=1` で失敗し、host 側でも IPv6 を drop（IPv6 を host の drop 規則まで到達させた試験はしていない。guest に IPv6 stack が無いため） |
| 4c | 2 tenant 間の通信を遮断 | 実装済み・KVM実測あり | tenant B（listen 8080）と tenant A を同時に起動（`net-cross-during.txt` に 2 lease / 2 tap / map 2 要素）。A から B の guest :8080 / :22 と B の tap :22 / :8080 は timeout、B の受付 0（`net-checks-cross.json`、`net-cross-b.json`）。tap 宛ては `forward` で応答以外 drop、guest の pool は `blocked_ipv4` 内 |
| 4d | DNS と直接 IP、redirect 経由の回り込みを遮断 | 実装済み・KVM実測あり | public-web: resolver 1.1.1.1 への UDP DNS は応答、8.8.8.8 と 192.168.5.2 は無応答、`169.254.169.254.nip.io` は解決されるが接続 timeout、httpbin の 302 → `http://169.254.169.254/latest/meta-data/` の追従も timeout。restricted: resolver が無く example.com は解決されず、1.1.1.1:53 も拒否。redirect 検査は外部の httpbin.org に依存する（届かなければ inconclusive として記録） |
| 5 | vCPU / memory が `machine-config` に反映され、超過 alloc が `Crash` に分類される | 実装済み・KVM実測あり（M9: guest の vCPU 1 = 要求 1、MemTotal 232 MiB / 要求 256 MiB、128 MiB 環境で 512 MiB 確保は 80 MiB 到達後に `crash` / `Runtime.Crash`。`docs/evidence/isolation-20260916T020934Z/`、再計測 `docs/evidence/isolation-20260917T011555Z/` でも同じ） | `hello.json` の `evidence.details.{vcpus, mem_mib}`（1 / 256）、capability `enforce_resource_limits` = supported（PLT-4622 で昇格。下の 5b と合わせて）。host 側の CPU / memory 上限は 5e |
| 5b | ephemeral storage の上限: guest が host のディスクを使い切れない | 実装済み・KVM実測あり（DISK: `ephemeral_storage_mib = 64` の環境で `/tmp` に 256 MiB 書こうとして 58 MiB で `ENOSPC`（`/dev/vdc` の ext4、差は ext4 のメタデータ）、`/` と `/function` への書き込みは `EROFS`、fill 中の host の空きの減少は最大 73,224,192 B（約 70 MiB。確保済みの scratch drive 64 MiB と function drive）。`docs/evidence/isolation-20260917T011555Z/`） | `crates/providers/firecracker/src/drive.rs::{scratch_drive_is_exactly_the_requested_size, reserved_image_has_requested_length}`、`crates/providers/firecracker/src/host_guard.rs`（console の cap、`fc.log` の watchdog、空き容量の budget 検査）、`tests/fake_vmm.rs::{full_lifecycle_with_fake_vmm, console_log_is_capped, create_fails_closed_when_the_host_disk_cannot_hold_the_budget}`、`crates/runtime-bridge/src/init.rs`（`tachyon.scratch_dev` を `/tmp` に mount）、`examples/isolation-probe` の `{"probe":"disk"}`、`scripts/kvm/measure-isolation.sh`（exit 3 = DISK FAIL）。未検証: console 洪水と `fc.log` の上限の KVM 実測（fake VMM と unit test だけ）、drive の `rate_limiter`（未設定） |
| 5c | egress の起動ゲート: network policy が効く前に user code が動かない（初期 egress race） | 実装済み・KVM実測あり（none: `evidence.details.network_interfaces` = 0、`docs/evidence/isolation-20260917T011555Z/disk-invocation.json`。restricted / public-web: policy を付けた 4 回の起動すべてで `egress policy installed and verified` が `InstanceStart accepted` より前、`docs/evidence/isolation-20260917T031126Z/``net-race.json`） | `crates/providers/firecracker/src/egress_gate.rs`（none は NIC / MMDS を拒否、restricted / public-web は検証済み tap を指す eth0 1 本だけを計画と `GET /vm/config` で許可: `a_policy_allows_exactly_its_interface_once_and_never_mmds`、`a_vm_config_with_exactly_the_policed_nic_passes`）、`network.rs::HostNetwork::{setup, verify}`（chain を読み戻してから NIC を計画に入れ、`InstanceStart` 直前に再度読み戻す。tap の link up も検証後）、`tests/fake_vmm.rs::egress_gate_refuses_to_start_a_vm_with_a_network_interface`。policy が崩れた状態で実機の起動を拒否させる試験は unit test（読み戻しの不一致）と fake VMM だけ |
| 5d | 後始末: tap・nftables 規則を terminate で消し、起動時に孤児を回収する | 実装済み・KVM実測あり（実行後に tap 0、table なし、lease 0。`docs/evidence/isolation-20260917T031126Z/``net-cleanup.txt`、teardown 時の counter は `net-counters.txt`） | `network.rs::HostNetwork::{teardown, sweep}`（削除後に読み戻して確認、最後の lease が消えたら table も削除。`list_environments` が環境ディレクトリの無い tap / chain / map 要素を削除）、`provider.rs`（起動失敗時と terminate 時に呼ぶ）。孤児の sweep を実機で起こした試験（gateway を kill して再起動）はしていない |
| 6 | 1 environment = 1 tenant × 1 revision × 1 attempt、破棄後に再利用しない | 実装済み・KVM実測あり | `crates/domain/src/revision.rs`（`ExecutionPolicy`）、`crates/application/tests/pipeline.rs::happy_path_records_timings_evidence_secrets_and_cleanup`、`docs/evidence/20260915T125610Z-firecracker/gateway.log`（invoke ごとに別の env id で `firecracker spawned` → `environment terminated`） |
| 7 | jailer / 専用ユーザー（non-root / capabilities / seccomp の Firecracker での読み替え） | 実装済み・KVM実測あり（HOST: 起動した 30/30 の VMM が uid 64000・`CapEff` 0・chroot・新しい PID / mount namespace で、`firecracker` / `fc_api` / `fc_vcpu 0` の `Seccomp` が 2。M8 / M9 / DISK / NET も jailer 下で PASS、実行後に jail・cgroup・VMM 0。`docs/evidence/isolation-20260917T041930Z/` の `vmm-isolation.jsonl`・`host-checks.json`・`host-cleanup.txt`） / 未検証（x86_64、bare metal、環境ごとの uid、network namespace） | `crates/providers/firecracker/src/jail.rs::{args_put_firecracker_flags_after_the_separator, layout_and_exec_name, prepare_links_and_remove_stays_inside_the_base}`、`vmm.rs::env_paths_layout`、`provider.rs::jailed_paths_live_in_the_chroot`、`crates/application/src/config.rs::firecracker_cgroup_and_jailer_sections`、`scripts/kvm/host-watch.sh`、`scripts/kvm/measure-isolation.sh`（exit 6 = HOST FAIL）。`[provider.firecracker.jailer]`（`config/gateway.firecracker.toml` で有効。dev では任意）。egress の tap は jail の uid / gid を owner にして host namespace に置く（ADR-0005 選択肢 C）。読み替えは `docs/threat-model.md` §14-2。全環境が同じ uid |
| 8 | 2 tenant を同じ host に並べた干渉（noisy neighbor）の計測: CPU / OOM / disk 埋めの負荷で上限が効き、同居 tenant が守られる | 実装済み・KVM実測あり（aarch64 nested 4 vCPU host 1 台・1 回。NOISY PASS: tenant A（500 m）の 4 thread の CPU burn は cgroup の `cpu.stat` で 5 秒窓の最大 0.517 core・平均 0.501 core（quota 0.5 の 1.10 倍以下、guest の steal 50.7%）。同時に A の scratch drive 埋め（58 MiB で ENOSPC、その後 1 MiB の fdatasync 書き直し 72,579 回）と 512 MiB 確保（7 回とも `crash`、host の `oom_kill` 0）。tenant B（1000 m）の固定の仕事は 13/13 成功、CPU 部分の中央値 1708 → 2127 ms（×1.245、上限 ×1.30）、fsync 書き込み p50 0.501 → 0.526 ms（×1.051、上限 ×3.00）、handler 1831 → 2341 ms（×1.279）、A 終了後は 1712 ms に戻る。`docs/evidence/isolation-20260917T041930Z/noisy.json`） / 未検証（IO 帯域の強制: `io.max` と drive の `rate_limiter` は無い、x86_64 / bare metal、複数回の分布、CPU の遅延は上限に近い） | `examples/isolation-probe/src/load.rs`（`cpu` / `io` / `work`）、`scripts/kvm/measure-isolation.sh` の NOISY（exit 5。上限値は初回計測の前に固定し、外れたら FAIL として記録する）、`docs/kvm.md` §3.6 |
| 5e | host 側の CPU / memory / pids 上限（VMM への cgroup v2） | 実装済み・KVM実測あり（HOST: 30/30 の VMM が `0::/tachyon/<env>` にいて `cpu.max` = `50000 100000`（500 m）/ `100000 100000`（1000 m）、`memory.max` = guest + 64 MiB。`memory.peak` は 128 MiB の環境で全量確保しても最大 170,496,000 B（上限 201,326,592 B、VMM と page cache の上乗せは約 42 MiB）、`oom_kill` は全環境 0。NOISY の CPU 実測は上の 8。`docs/evidence/isolation-20260917T041930Z/`） / 未検証（x86_64、bare metal、systemd の委譲 subtree を `root` にした構成） | `crates/providers/firecracker/src/cgroup.rs::{limits_follow_cpu_millis_and_memory, memory_max_may_round_down_to_a_page_only, host_support_explains_itself}`、`provider.rs::{resource_limits_claim_follows_the_host_cgroups, required_cgroups_fail_closed_before_anything_exists}`、`preflight.rs::preflight_reports_structured_failures_on_any_host`（`host_cgroup`）、`crates/application/src/config.rs::firecracker_cgroup_and_jailer_sections`。VMM は fork と exec の間に cgroup へ入り（`vmm.rs::spawn_vmm`）、`cgroup.procs` と `/proc/<pid>/cgroup` で所属を確認してから構成する。terminate は `cgroup.kill` → 統計のログ → rmdir、起動時 reconcile が孤児を消す。`profile = "production"` では `mode = "required"` 以外は設定エラーで、委譲の無い host では preflight が失敗し環境を作らない（設定値だけで合格にしない）。dev の `best-effort` で cgroup が使えない host では `enforce_resource_limits` を `unverified` にする |

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


## PLT-4632 ExecutionEnvironment pool・再利用キー・reconciler

環境の再利用は二重の gate の内側にある。provider が `idle_quiesce` と `idle_resume` の両方を `Supported` と申告し、かつ `[pool] enabled = true` のときだけ有効になる。process は `Unsupported` のままで、Firecracker は PLT-4633 で休止・再開を実装し、実機計測を経て `Supported` になった（次節）。`[pool]` の既定は無効なので、**既定の挙動は P1 と同じ destroy-after-invoke のまま**である。本節の検証は fake provider による自動テストで行っており、実機の休止・再開は次節に記録する。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 完全一致の再利用キーだけ再利用し、他 tenant・他 revision・設定変更・失効した secret 世代と混ざらない | 実装済み | `crates/application/src/repository.rs::only_an_exactly_matching_reuse_key_is_reused`（8 次元すべての不一致）、`crates/application/tests/pipeline.rs::{another_revision_never_reuses_the_first_revisions_environment, a_rotated_secret_supersedes_the_reuse_key_and_forces_a_cold_start}` |
| 2 | `Busy` を配らず、`Ready` になる前に処理を流さない | 実装済み | `crates/application/src/repository.rs::{nothing_is_dispatched_before_ready_and_busy_is_never_handed_out, a_ready_environment_is_not_in_the_pool_and_is_never_claimed, concurrent_claims_never_hand_the_same_environment_to_two_callers}`、`crates/application/tests/pipeline.rs::a_busy_environment_is_never_handed_to_a_concurrent_invocation` |
| 3 | controller 再起動で所有を照合して状態を修復する | 実装済み | `crates/application/tests/pipeline.rs::a_pooled_environment_is_reclaimed_after_a_restart`、`crates/application/src/services/reconcile.rs`、PLT-4627 #6 の各テスト |
| 4 | 休止能力が未検証の profile は再利用を feature gate で無効にし、終了後に破棄する | 実装済み | `crates/application/tests/pipeline.rs::a_provider_without_supported_idle_capabilities_keeps_destroy_after_invoke`、`crates/application/src/services/pool.rs`（`PoolPolicy::decide`） |
| 5 | idle TTL と drain で回収し、terminate に失敗しても取りこぼさない | 実装済み | `crates/application/tests/pipeline.rs::idle_environments_are_reaped_by_the_ttl_sweeper_and_by_a_drain`、`crates/application/src/services/pool.rs::{a_failed_terminate_keeps_the_environment_for_the_next_sweep, a_failed_terminate_while_retiring_is_retried_and_metered_once}` |
| 6 | 前の attempt が残したフレームで次の attempt が決まらない | 実装済み | `crates/application/tests/pipeline.rs::{a_late_frame_from_the_previous_attempt_cannot_settle_the_reused_one, a_guest_that_exited_after_answering_is_not_pooled}`、`crates/application/src/bridge_session.rs::draining_attributes_trailing_frames_to_the_attempt_that_left_them` |
| 7 | 再利用しても利用量を取りこぼさず、環境の寿命を一度だけ計上する | 実装済み | `crates/application/tests/pipeline.rs::{every_invocation_on_a_reused_environment_is_metered, a_reaped_pooled_environment_reports_its_lifetime_to_usage, an_environment_the_driver_ends_reports_its_whole_life, the_usage_sequence_of_a_reused_environment_never_restarts}` |
| 8 | 異常終了した環境を pool に戻さない | 実装済み | `crates/application/tests/pipeline.rs::an_environment_whose_guest_crashed_is_never_pooled` |
| 9 | warm dispatch が届かなかった場合は cold と同じ分類をしてから cold で再試行する | 実装済み | `crates/application/tests/pipeline.rs::{an_undelivered_warm_dispatch_is_classified_like_a_cold_one, a_warm_dispatch_into_a_dead_guest_falls_back_to_a_cold_start}`、`docs/threat-model.md` §9 |
| 10 | secret 値そのものが再利用キーや台帳へ入らない | 実装済み | `crates/application/src/services/pool.rs::the_secret_generation_is_salted_and_unambiguous`（プロセスごとの salt 付き digest） |
| 11 | release と claim が競合しても、session の無い行を掴まない | 実装済み | `crates/application/src/services/pool.rs::a_claim_racing_a_release_never_takes_a_row_without_its_session` |
| 12 | 再試行に必要な間だけ payload を保持する | 実装済み | `crates/application/src/services/invoke.rs::a_dispatched_payload_is_retained_only_while_a_cold_retry_can_need_it` |
| 13 | KVM 実機での再利用 | 実装済み・KVM実測あり | `docs/evidence/warm-20260916T162532Z/summary.txt`（6 invocation 中 5 が warm、同一環境を 6 epoch 再利用）。詳細は次節 |
| 14 | 複数プロセス間での slot・lease・pool membership の原子性 | 実装済み（PLT-4631） | 次々節「PLT-4631」。pool の claim / sweep は環境の owner（dispatcher）に限る（`contract_tests.rs::the_pool_only_hands_out_and_sweeps_its_owners_environments`） |

## PLT-4633 idle 休止・再開（warm 再利用の実機計測）

休止・再開は Firecracker の `PATCH /vm {"state": "Paused"/"Resumed"}` で実装している（`crates/providers/firecracker/src/provider.rs`）。pool は Idle 行を公開する前に休止し、払い出す前に再開してから guest に `Ping` を送り、`Pong` が返らない環境は配らずに retire する。計測は `scripts/kvm/measure-warm.sh`（`docs/kvm.md` §3.7）で取り、証跡は `docs/evidence/warm-20260916T162532Z/`。この証跡をもって `idle_quiesce` / `idle_resume` を `Unverified` から `Supported` に上げた（`docs/adr/0001` §「決定」5）。`[pool]` の既定は off のままなので、既定の挙動は変わらない。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 実機で休止と再開が往復し、同じ環境が再利用される | 実装済み・KVM実測あり | `docs/evidence/warm-20260916T162532Z/summary.txt`: 6 invocation 中 5 が warm、環境は 1 つを 6 epoch |
| 2 | warm が cold より速いことを実測で示す | 実装済み・KVM実測あり | `docs/evidence/warm-20260916T162532Z/comparison.json`: cold `total_ms` 14663 / warm 230（中央値、差 14433）、`resume_ms` 9、`readiness_ms` 9 |
| 3 | 休止中の環境が host の CPU を消費しない | 実装済み・KVM実測あり | `docs/evidence/warm-20260916T162532Z/paused-vmm.json`: 3 秒間の CPU tick 0、state `Sl`。RSS は 38 MiB のままで、**休止は memory を返さない** |
| 4 | 再開後、guest が応答することを確かめてから dispatch する | 実装済み | protocol v2 の `Ping` / `Pong`（`docs/protocol.md` §A）、`crates/application/src/bridge_session.rs::the_readiness_probe_only_passes_when_the_guest_answers`、`crates/application/tests/pipeline.rs::a_pooled_guest_that_stops_answering_is_never_dispatched_into` |
| 5 | 再開が拒否されたら warm を諦めて cold に落ちる | 実装済み | `crates/providers/firecracker/src/provider.rs::a_refusal_is_success_only_for_the_state_that_was_requested`、`crates/application/src/services/pool.rs` の retire 経路 |
| 6 | 休止を client の応答経路から外す | 実装済み | `crates/application/src/services/pool.rs::the_quiesce_runs_after_the_caller_is_gone_without_publishing_early`、`crates/application/tests/pipeline.rs::a_slow_quiesce_does_not_hold_up_the_callers_response` |
| 7 | 休止した guest に Shutdown を送って待たない | 実装済み | `TerminateReason::Quiesced`、`crates/application/src/services/pool.rs::a_paused_environment_is_reaped_without_waiting_for_its_guest` |
| 8 | 再開後の deadline が休止時間の分だけ伸びない | 実装済み | `HostMessage::Invoke.remaining_ms`、`crates/runtime-bridge/src/session.rs::a_resumed_guest_gets_a_deadline_in_its_own_clock` |
| 9 | 計測専用 switch が本番 profile で拒否される | 実装済み | `crates/application/src/config.rs::the_measurement_switch_is_refused_under_production` |
| 10 | 証跡が gate の状態を記録する | 実装済み・KVM実測あり | `docs/evidence/warm-20260916T162532Z/summary.json` の `reuse`（`enabled=true`、`verified=false`、`measurement_only=true`）。昇格前の計測であることが記録に残る |
| 11 | x86_64 / bare metal での計測 | 未検証 | 記録は aarch64 の nested virtualization のみ（`docs/kvm.md` §5） |
| 12 | 長時間 idle のあとの再開、N ≥ 20 の分布 | 未検証 | 今回の記録は 1 環境・warm 5 回。TTL 満了と drain の回収は fake provider のテストのみ |

## PLT-4651 (X1) Rust SDK の初期化保存点・復元後 hook（実験 API）

実験 API。SDK の feature `experimental-restore`（既定 off）の `lifecycle::builder().bootstrap(..).after_restore(..).run(..)` / `.serve_http(..)` と、bridge の Runtime API `lifecycle/{bootstrap, checkpoint, continue, error}`（`docs/protocol.md` §B-X1）。**snapshot の取得・復元は実装していない**（PLT-4653）。同梱の provider はどれも snapshot を取らないので bridge は常に `cold` を返し、`restored` はテストの mock restore 通知からしか出ない。P0〜P4 はこの Issue に依存しない。データは合成データ（素数表）だけを使う。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 通常起動も snapshot 不要で同じ API 経由で動く | 実装済み（process provider で 1 回実行） | `crates/sdk/src/lifecycle.rs::cold_start_takes_the_same_path_and_serves`、`crates/runtime-bridge/src/runtime_api.rs::lifecycle_cold_start_gates_ready_until_continue`、`crates/runtime-bridge/src/session.rs::lifecycle_timeouts_are_typed_by_phase`（`NoSnapshot` が `{"kind":"cold"}` を返す）、`docs/evidence/restore-aware-20260917T025043Z-process/`（`examples/restore-aware` を gateway + process provider で deploy / invoke。`restored=false`、`generation=0`、ログに `lifecycle continue answered (cold)`）。Firecracker では未実行 |
| 2 | X1 無効時に P1 の互換性を壊さない | 実装済み | feature 無しの `cargo test -p tachyon-serverless-sdk`（既存 20 テスト、`lifecycle` は compile されない）、`crates/runtime-bridge/src/runtime_api.rs::without_lifecycle_next_still_implies_ready`、host↔bridge frame と `PROTOCOL_VERSION`（2）は無変更、`docs/evidence/20260917T024812Z-process/summary.json`（`scripts/e2e/demo.sh` 28/28 PASS。port 8080 が使用中のため `config/gateway.dev.toml` の listen・data_dir・workdir だけを変えたコピーを `TSLS_GATEWAY_CONFIG` で指定）。注: workspace では `examples/restore-aware` が feature を有効にするので、`cargo test --workspace` の SDK は feature 有りで build される（`run` / `serve_http` の挙動は feature に依存しない） |
| 3 | 本番 Secret / DB 接続 / 常駐 task を保存前に作らない example | 実装済み | `examples/restore-aware/src/main.rs`（bootstrap は素数表だけ。identity・`/dev/urandom` からの RNG reseed・時計・secret の有無・fake connection は after_restore だけで作る。理由は module doc）、`examples/restore-aware/src/main.rs::{table_is_a_correct_sieve, copies_get_distinct_identity_and_random_streams}`、`crates/sdk/src/lifecycle.rs::hooks_run_in_order_around_a_mock_restore`（bootstrap 中に Tokio runtime が無いこと、after_restore は SDK が continue 後に作った runtime の中で動くことを assert）。SDK 自身は checkpoint 前に runtime・thread・signal handler・永続接続を作らない（lifecycle 呼び出しは 1 リクエスト 1 接続の blocking HTTP） |
| 4 | pre-checkpoint / after-restore の失敗と timeout を区別する | 実装済み（process provider で失敗 2 種を 1 回ずつ実行） | 型: `Runtime.PreCheckpointFailed` / `Runtime.AfterRestoreFailed` / `Runtime.PreCheckpointTimeout` / `Runtime.AfterRestoreTimeout` / `Runtime.CheckpointTimeout`（bridge 側の待ち）。`crates/runtime-bridge/src/runtime_api.rs::{lifecycle_errors_are_typed_by_phase, lifecycle_continue_waits_for_the_restore_notification}`、`crates/runtime-bridge/src/session.rs::{lifecycle_timeouts_are_typed_by_phase, lifecycle_after_restore_failure_is_an_init_error}`、`crates/sdk/src/lifecycle.rs::{bootstrap_failure_never_reaches_checkpoint_or_ready, after_restore_failure_never_posts_ready}`（error と panic の両方）、`docs/evidence/restore-aware-20260917T025043Z-process/{fail-bootstrap.json, fail-after-restore.json}`（`init_error` / 各 error_type）。timeout は単体テストのみ |
| 5 | 不完全な状態で Ready にならない | 実装済み | lifecycle が開いている間 bridge は continue 前の `ready` と、明示的な `ready` 前の `next` を 409 にする。`crates/runtime-bridge/src/runtime_api.rs::lifecycle_cold_start_gates_ready_until_continue`、`crates/runtime-bridge/src/session.rs::lifecycle_ready_reaches_the_host_only_after_the_restore`（mock restore 通知まで host に `Ready` frame が届かず、continue だけでも届かない）、`crates/sdk/src/lifecycle.rs::after_restore_failure_never_posts_ready` |
| 6 | 任意ライブラリ・multithread runtime が透過的に snapshot-safe とは主張しない | 実装済み（文書） | `docs/protocol.md` §B-X1「snapshot-safe を主張しない」、`crates/sdk/src/lifecycle.rs` の module doc「What this does not do」、`RuntimeFlavor::MultiThread` の doc、`crates/protocol/src/runtime_api.rs`（`lifecycle` module doc）、`examples/restore-aware/src/main.rs` の module doc |
| 7 | 検証: mock restore 通知による hook 順序 | 実装済み | `crates/sdk/src/lifecycle.rs::hooks_run_in_order_around_a_mock_restore`（`POST bootstrap → hook bootstrap → POST checkpoint → GET continue → (通知) → hook after_restore → POST ready`）、`crates/runtime-bridge/src/session.rs::lifecycle_ready_reaches_the_host_only_after_the_restore`（`run_session_with` に mock `RestoreSource` を注入）、`crates/protocol/src/runtime_api.rs::continuation_wire_shape` |
| 8 | 実際の snapshot / restore での hook | 未着手 | PLT-4653。restore を bridge に知らせる frame（version を上げる）か `HelloAck` の能力 field、restore 後の init budget の host 側の扱い、restore 後に新しい secret を渡す経路はいずれも未実装（`docs/protocol.md` §B-X1「互換性の規則」「環境変数」） |
| 9 | Firecracker（KVM）での実行 | 未検証 | `cargo check --target aarch64-unknown-linux-musl` は通るが、microVM 内で `examples/restore-aware` を動かした記録は無い |

## PLT-4631 実行 slot の lease・fencing・Invoke 冪等性

gateway プロセスごとの dispatcher（owner）と lease、slot の原子的取得、世代付きの完了通知、lease 失効時の fence と終了確認、`Idempotency-Key` の失効を実装した（`docs/architecture.md` §4「dispatcher・lease・fencing」、`docs/adr/0003-execution-state-persistence.md`「実装メモ（PLT-4631）」、`docs/threat-model.md` §6-3・§6-8・§10・T24・§14-8）。テストは次で再現する: `cargo test -p tachyon-serverless-application --lib repository`（契約テストは `repository::contract_tests::{memory,sqlite}::<名前>` の 2 回）、`cargo test -p tachyon-serverless-application --test leases`、`cargo test -p tachyon-serverless-application --test pipeline`、`cargo test -p tachyon-serverless-gateway --test gateway_integration`。**provider はすべて fake**（`crates/providers/fake`）で、KVM 実機・2 つの gateway プロセスを並べた E2E は無い。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 同時 acquire で勝者は一つ | 実装済み | property（N = 2, 8, 16, 4 の 4 ラウンド、ラウンドごとに slot を解放して次の epoch で再び競合）: `contract_tests.rs::acquire_is_a_cas_with_exactly_one_winner_per_epoch`（両 store、スレッド）。同じ file に別 connection: `sqlite/tests.rs::concurrent_acquires_on_separate_connections_have_one_winner_per_epoch`（N = 2, 4, 8, 12、未 release の lease は常に 1）。**OS プロセス**: `sqlite/tests.rs::separate_processes_racing_for_one_slot_or_one_key_have_one_winner`（テスト binary を 6 プロセス起動、全員の準備完了を待ってから同時に acquire、勝者 1・epoch + 1） |
| 2 | 古い epoch の完了通知は状態を上書きしない | 実装済み | `contract_tests.rs::a_completion_with_a_stale_epoch_never_overwrites_state`（前の assignment の遅れた完了、現在の lease に前の epoch を付けた完了、reclaim 後の完了がいずれも `Stale` で、`Running` / `Succeeded` / `OutcomeUnknown{Host.LeaseExpired}` が変わらない）、`tests/leases.rs::a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced`（guest が reclaim の後に正しい `(attempt_id, epoch)` で `Response` を返しても台帳は reclaim の結果のまま、呼び出し元にも結果を返さない）、既存の session 側 fencing `pipeline.rs::a_late_frame_from_the_previous_attempt_cannot_settle_the_reused_one` |
| 3 | 同じキー / 入力の再送は同じ Invocation | 実装済み | 同一 gateway: `pipeline.rs::{idempotency_replay_and_conflict, concurrent_requests_with_the_same_key_run_once, capacity_rejection_does_not_consume_the_idempotency_key}`、HTTP: `apps/gateway/tests/gateway_integration.rs::full_api_roundtrip`。別 gateway が実行中: `tests/leases.rs::a_key_replayed_on_another_gateway_returns_the_same_invocation_and_never_runs_twice`（実行中の invocation を台帳で追って同じ id・同じ出力、実行は 1 回）。OS プロセス / 別 connection で同じ key を同時に結び付け: `sqlite/tests.rs::{separate_processes_racing_for_one_slot_or_one_key_have_one_winner, reclaim_and_key_binding_are_exactly_once_across_connections}`。保持期限: `contract_tests.rs::idempotency_bindings_expire_after_their_invocation_finished`（実行中は失効しない、`finished_at + retention` で失効、失効後の同 key は新規、purge） |
| 4 | 違う入力は 409 | 実装済み | `pipeline.rs::idempotency_replay_and_conflict`（`AppError::IdempotencyConflict`、本文の `invocation_id` と `error_type = Host.IdempotencyKeyReused`、何も実行しない）、`gateway_integration.rs::full_api_roundtrip`（HTTP 409 の本文）、`tests/leases.rs::a_key_replayed_on_another_gateway_returns_the_same_invocation_and_never_runs_twice`（別 gateway が実行中でも 409） |
| 5 | lease 失効だけで未停止の環境へ新しい処理を重ねない | 実装済み（fake provider） / 未検証（KVM 実機、2 プロセスの gateway） | fence（`Draining`、epoch + 1）の後は claim・acquire・pool の対象外で、`confirm_terminated`（同じ epoch の CAS）でだけ `Lost`: `contract_tests.rs::reclaim_happens_once_fences_and_only_a_confirmed_terminate_settles`、`domain/src/environment.rs::a_fenced_environment_is_never_reusable_and_its_old_epoch_is_stale`。provider の terminate 成功後にだけ settle: `tests/leases.rs::a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced`、`pipeline.rs::a_pooled_environment_is_reclaimed_after_a_restart`。terminate に失敗し続ける間は fenced（`Draining`、claim 不可、`pending`）のまま 3 周期残り、成功した周期でだけ `Lost`: `tests/leases.rs::a_fenced_environment_stays_fenced_until_its_terminate_succeeds` |
| 6 | host fencing / 終了確認と連携する | 実装済み（fake provider） / 未検証（Firecracker） | 上の #5。起動時 reconcile は他の live dispatcher の環境を terminate・`Lost` にしない: `tests/leases.rs::two_gateways_on_one_data_dir_never_settle_each_others_work`（`foreign` に数え、fake の terminate 呼び出し 0） |
| 7 | 開始後の同期失敗を再実行しない | 実装済み | dispatch 後の接続断は `OutcomeUnknown` で再実行しない: `pipeline.rs::disconnect_after_invoke_is_outcome_unknown`。再実行するのは handler が開始していない warm の配送失敗だけ: `pipeline.rs::{an_undelivered_warm_dispatch_is_classified_like_a_cold_one, a_warm_dispatch_into_a_dead_guest_falls_back_to_a_cold_start}`。lease の reclaim も再実行しない（`tests/leases.rs::a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced`、`fake.created()` は 1） |
| 8 | 外部副作用の exactly-once は保証しない | 文書化済み | `docs/api.md` §5.6「再実行しない」、`docs/threat-model.md` §9・§10・§15、`crates/application/src/services/invoke.rs` の module doc |

検証項目:

| 検証 | 状態 | 証跡 |
|---|---|---|
| 競合 property test | 実装済み | 上の #1、`contract_tests.rs::concurrent_claims_never_hand_the_same_environment_to_two_callers`（pool の claim） |
| dispatcher 二重起動 | 実装済み（1 プロセス内に 2 つの `Application`） / 未検証（2 つの gateway プロセスを HTTP で並べた E2E） | `tests/leases.rs::{two_gateways_on_one_data_dir_never_settle_each_others_work, renewal_keeps_the_lease_and_a_graceful_stop_hands_over_at_once}`、`contract_tests.rs::a_live_dispatcher_is_never_reclaimed_and_a_stopped_one_is_at_once`。別 gateway が駆動中の invocation の cancel は 409（`two_gateways_...`） |
| 遅延 callback | 実装済み | 上の #2 |
| lease 失効 / 時刻差 | 実装済み（注入した `FixedClock`） / 未検証（実時計の飛び、プロセスの停止） | `contract_tests.rs::{leases_renew_only_while_unexpired_and_expire_past_the_clock_skew, a_fenced_dispatcher_can_neither_renew_nor_acquire}`、`tests/leases.rs::{a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced, renewal_keeps_the_lease_and_a_graceful_stop_hands_over_at_once}`（2 つの gateway に別々の `FixedClock`。期限 + 1 s（skew 2 s の内側）では回収せず、+ 3 s で 1 回だけ回収、renew し続ける限り 25 s 先を行く時計からも回収されない）、`domain/src/environment.rs::a_lease_is_renewed_only_while_unexpired_and_expires_past_the_skew`。**OS プロセス**: `sqlite/tests.rs::a_lease_left_by_an_exited_process_is_reclaimed_once_and_only_after_expiry`（lease を持ったまま exit した子プロセス。別 instance は期限前に回収できず、以後 1 回だけ。同じ instance の再起動は pid の不在で即時） |
| E2E（process provider、P1 互換） | 実装済み | `docs/evidence/20260917T034846Z-process/`（28/28 PASS） |

関連する修正: `pipeline.rs::concurrent_requests_with_the_same_key_run_once` が稀に ``alias `prod` not found`` で落ちた原因は、revision の検証 task が「`Ready` の書き込み」と「`prod` alias の publish」を別々の store 更新で行い、`RevisionService::wait_terminal` が `Ready` を見た時点で戻っていたこと（テストの deploy helper がその直後に alias を読む）。`wait_terminal` が同じプロセスで走っている検証 task の完了（publish を含む）まで待つようにした。publish の直前に 50 ms の遅延を入れると修正前は毎回同じ失敗になり、修正後は通ることを手元で確認した（遅延は commit していない）。HTTP で `GET revision` を poll する client からは、`Ready` と alias の移動の間の短い窓は従来どおり見えうる。

## PLT-4645 API 契約・tenant 境界・KVM 統合試験を変更範囲別 CI へ接続

Issue 名は「Kata 統合試験」だが、ADR-0001 により KVM 統合は Firecracker で行う。構成・信頼モデル・runner 登録・canary / rollback は `docs/ci.md`。記録日 2026-09-17、branch `ci/plt-4645-scoped-gates`。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 全 PR で unit / property / OpenAPI / runtime 契約試験を回す | 実装済み（GitHub Actions で実行確認） | `.github/workflows/ci.yml` の `rust`（`cargo test --workspace`）と `contracts`（`apps/gateway/tests/openapi_snapshot.rs` と `docs/openapi.json`、`crates/protocol/tests/golden.rs` と `crates/protocol/tests/golden/`、`scripts/ci/security-regression.sh`）。branch での `workflow_dispatch` 実行 https://github.com/quantum-box/tachyon-serverless/actions/runs/35179100905 で全 job success（security regression 63/63、rebase 前）。PR event での実行は、この記録の時点では PR を作っていないため未確認 |
| 2 | tenant 認可・環境 key・lease・deadline・egress 準備順序・資源上限の回帰を検知する | 実装済み（hosted、fake provider / 偽 Firecracker の範囲） / 未検証（KVM 実機） | `scripts/ci/security-regression.list`（6 分類 83 テスト。PLT-4631 の lease / fencing と PLT-4622 追補の egress allowlist・nftables 規則を含む。list にあるテストが走らなければ失敗）。`docs/evidence/ci-gates-20260917T041348Z/summary.txt`: 一時コピーに既知の悪い変更を入れ、tenant 検査の除去・reuse key 比較の無視・epoch fencing の除去・経過済み client deadline の受理・pre-boot egress 検査結果の無視・memory 下限の除去・security テストの rename がそれぞれ gate で失敗した。このとき既存の `crates/domain/src/invocation.rs::rejects_elapsed_client_deadline_and_bad_key` は不正な idempotency key でも失敗するため deadline 検査の除去を検出できないと分かり、`crates/domain/src/invocation.rs::an_elapsed_client_deadline_alone_is_rejected` を追加した。KVM 上の同種の検査（`scripts/kvm/measure-isolation.sh` ほか）は runner 未登録のため CI からは未実行 |
| 3 | runtime / network / kernel / billing 変更向けの専用 KVM integration を分ける | 実装済み（workflow 定義） / 未検証（KVM job の実行） | `.github/workflows/kvm-integration.yml`（`runs-on: [self-hosted, linux, kvm]`、smoke → measure-isolation → measure-warm → E2E firecracker → 常に cleanup + orphan check → 常に artifact）、`scripts/ci/classify-changes.sh`（分類規則は `scripts/ci/selftest.sh` で固定）。**runner が未登録のため `kvm` job は一度も実行されていない** |
| 4 | KVM 未実行を合格と表示せず、required / optional の gate が明確 | 実装済み（GitHub Actions で pending と失敗を確認） | `docs/ci.md` §2 の gate matrix、`scripts/ci/kvm-gate.sh`。https://github.com/quantum-box/tachyon-serverless/actions/runs/35179136197（`workflow_dispatch`、runner なし）: `kvm` job は queued のまま、`kvm-gate` は開始されず pending。run を cancel すると `kvm-gate` は `FAILED - KVM integration is REQUIRED and finished with 'cancelled'` で失敗。label なし / fork の skip が失敗になることは `scripts/ci/selftest.sh` と `docs/evidence/ci-gates-20260917T041348Z/cases/*-kvm-*.log`。required check（`ci-gate`、`kvm-gate`）の branch protection への登録は未実施（owner 作業） |
| 5 | docs / UI のみの変更に重い KVM 試験を無条件で実行しない | 実装済み（分類の自己テスト） / 未検証（docs-only PR での実行） | `scripts/ci/classify-changes.sh`（docs-only は `rust` / `contracts` / `guest-musl-build` を skip、KVM 不要）、`scripts/ci/selftest.sh`。この repository に UI は無い |
| 6 | 未信頼 PR へ production credential・常駐 runner 権限を渡さない | 実装済み（workflow の規則） / 未検証（runner 側の設定） | fork PR は `kvm` job を起動しない（classify と job の `if:` の二重検査）、`pull_request_target` 不使用、全 job `contents: read`、`persist-credentials: false`、secret 不使用、action は SHA pin、KVM job は cache 不使用。public repository なので `if:` は多層防御にすぎず、runner group の制限・fork PR 承認・ephemeral runner（`docs/ci.md` §4.3、§5）が必要だが、runner が無いため未設定 |
| 7 | 実行後 cleanup を確認する | 実装済み（workflow 定義） / 未検証（KVM job の実行） | `kvm` job の `Cleanup and orphan check`（`if: always()`、`scripts/kvm/teardown.sh --purge`、`scripts/e2e/orphan-check.sh firecracker` / `process`、残存 firecracker process で失敗、結果を artifact の `cleanup.txt` に保存）、開始時の残存 VM 拒否 |
| 8 | test artifact に commit / profile を保存する | 実装済み（profile script の自己テスト） / 未検証（KVM job の artifact） | `scripts/ci/kvm-profile.sh`（commit、ref、run、Firecracker 版と sha256、guest kernel sha256 / key、rootfs sha256、gateway config sha256、host uname / CPU / memory / KVM / nested virtualization）、artifact 名 `kvm-evidence-<sha>-<run_id>-<attempt>` |
| 9 | 検証: 意図的な fixture 破損で CI 失敗を確認 | 実装済み（ローカル） | `scripts/ci/prove-gates.sh`、`docs/evidence/ci-gates-20260917T041348Z/`（origin/main（PR #13 まで）へ rebase した commit `88c0a92` で実行。baseline 3 gate PASS、既知の悪い変更 16 件すべて検出。golden fixture の手編集、`docs/openapi.json` の手編集、wire field / tag / header の rename、API schema field の rename を含む）。破損を含む branch は push していないので、GitHub Actions 上での失敗は確認していない |
| 10 | runtime profile 変更時の canary / rollback 手順を残す | 実装済み（文書） / 未検証（実施） | `docs/ci.md` §6、`kvm-integration.yml` の `workflow_dispatch` 入力 `firecracker_version` / `ci_version` / `guest_kernel_series` |

## PLT-4636 regional 設定 cache・認可 lease・control plane 停止時の挙動

control plane（`combined`）が generation 付きの設定を配信し、data plane（`data_plane`、または `combined` 自身）が期限付き cache と認可 lease だけで invoke を受け付ける構成を実装した（`docs/adr/0007-config-distribution-and-auth-leases.md`、`docs/architecture.md` §4「設定配信と認可 lease」、`docs/api.md` §7、`docs/threat-model.md` B5・T25・T26・§14-9）。記録日 2026-09-17、branch `feat/plt-4636-config-cache`。単一 region / cell のみ。テストは次で再現する: `cargo test -p tachyon-serverless-application --test config_cache`、`cargo test -p tachyon-serverless-application --lib -- control repository::config config::tests::control_plane error::tests`、`cargo test -p tachyon-serverless-gateway --test gateway_integration`、`scripts/control-plane/outage-e2e.sh`。単体・結合テストの provider は fake、E2E は process provider（隔離なし）で、**Firecracker では実行していない**。

| # | 受入条件 | 状態 | 証跡 |
|---|---|---|---|
| 1 | 有効期限内の配信済み設定だけで、既存環境への Invoke を継続できる | 実装済み（fake provider / process provider の 2 プロセス E2E） | `tests/config_cache.rs::invokes_continue_from_the_cache_while_the_control_plane_is_down`（refresh 失敗後、TTL の半分まで invoke 成功、entry は `stale_but_valid`、`/readyz` 相当の view は `control_plane_reachable: false` / `new_invocations: accepted`）、warm 環境の継続は `the_outage_policy_restricts_cold_starts_but_not_running_environments`（`allow_cold_start = false` でも `StartKind::Warm` で成功）。HTTP: `gateway_integration.rs::a_data_plane_gateway_serves_invokes_from_delivered_configuration`。E2E: `docs/evidence/20260917T045347Z-split-process/` step 10（管理 gateway を SIGTERM で止めた後も data plane の invoke が 200、`readyz-control-plane-down.json`）と step 12（停止の前から走っていた cpu-burn の invocation が TTL 切れの後に 200） |
| 2 | 認可 lease 期限切れ / 不明な tenant / 未配信版は新規受付を拒否する | 実装済み | lease 切れ: `tests/config_cache.rs::new_work_is_refused_exactly_at_valid_until_with_the_matching_reason`（`valid_until - 1 ms` は成功、`valid_until` で `Host.AuthLeaseExpired`、台帳に invocation が増えない）。不明な tenant: `unknown_tenants_and_undelivered_revisions_are_refused`（grant だけ届いた credential は `Host.UnknownTenant`、tenant が届くと成功）。未配信版: 同テスト（management で作った pin 用 revision は data plane で `Host.ConfigNotDelivered`、同じ時点の management では成功、次の refresh 後は data plane でも成功）。初回配信前: `invokes_continue_...`（`Host.ConfigNotDelivered`）。E2E step 13（`refusal-auth-lease-expired.json`） |
| 3 | 管理 API / Kubernetes API 停止時に、既存実行と新規起動の制限を区別して返す | 実装済み（Kubernetes は provider 制御 API に読み替え） | `/readyz` の `control_plane.{existing_executions, new_invocations, new_cold_starts, refusal, reason}`。管理 API 停止: `tests/config_cache.rs::{new_work_is_refused_exactly_at_valid_until_with_the_matching_reason, the_outage_policy_restricts_cold_starts_but_not_running_environments}`（`existing_executions: continue` のまま `new_invocations: refused` / `Host.ConfigExpired`、`allow_cold_start = false` では再利用 off なら受付前に 503 `Host.ColdStartRestricted`、on なら warm は成功し cold が要る invocation だけ `Failed{Host.ColdStartRestricted}` / HTTP 503）。provider 制御 API 停止: `a_failing_provider_control_api_refuses_cold_starts_only`（preflight 失敗後の cold start は 503 `Host.ProviderControlUnavailable`、`control_plane_reachable: true` のまま）。data plane の管理 API は 503 `Host.ControlPlaneUnavailable`、store 障害は 503 `Host.StoreUnavailable`: `gateway_integration.rs::a_data_plane_gateway_serves_invokes_from_delivered_configuration`、`error.rs::tests::control_refusals_are_distinct_and_retryable_ones_are_503`。E2E step 6・11（`readyz-config-expired.json`: `ready: false`、`existing_executions: continue`、`refusal: Host.ConfigExpired`）。**provider 制御 API の停止は fake provider でだけ確認し、E2E では再現していない** |
| 4 | 古い generation が新設定を巻き戻さず、接続回復後に収束する | 実装済み | 順序逆転: `tests/config_cache.rs::older_generations_never_roll_back_a_newer_configuration`（新しい配信の後に古い全量配信 → 丸ごと無視、現在の generation を名乗る古い route entry → entry 単位で無視、後退した配信は有効期限を延ばさない）、刻印側: `repository/config.rs::tests::stamps_changes_ignores_stale_values_and_tombstones_removals`、再起動をまたぐ単調性: `generations_are_monotonic_across_control_plane_restarts`。収束: `a_reconnect_converges_to_the_latest_generation`（切断中に publish した rev2 に、再接続の最初の refresh で generation ごと追いつく）、`a_revoked_token_stops_working_within_one_refresh_or_one_auth_lease`。E2E step 7（deploy v2 の 0.9 s 以内に data plane が v2）・step 15（管理 gateway の再起動から 0.5 s で受付再開、`readyz-reconnected.json` の `cache.reconnects = 1`、tenant B の token を消した再起動で B は 401） |

検証項目:

| 検証 | 状態 | 証跡 |
|---|---|---|
| 接続断 | 実装済み | 上の #1、E2E（管理 gateway の SIGTERM） |
| 期限境界 | 実装済み（注入した `FixedClock`） / 実時間は E2E で 1 回 | `new_work_is_refused_exactly_at_valid_until_with_the_matching_reason`（config TTL 20 s と auth lease 30 s のそれぞれ ±1 ms）。E2E: 停止から 7.9 s で最初の `Host.ConfigExpired`（config TTL 8 s、refresh 0.5 s。script は TTL - refresh - 1 s 〜 TTL + 3 s を要求） |
| 設定順序逆転 | 実装済み | 上の #4 |
| revoke の遅延上限 | 実装済み（文書化: 1 refresh / 最大 `auth_lease_seconds`） | `a_revoked_token_stops_working_within_one_refresh_or_one_auth_lease`、`docs/threat-model.md` §14-9 |
| dispatcher owner の再接続 | 実装済み（1 プロセス内に 2 つの `Application`） | `a_dispatcher_fenced_during_the_outage_stays_fenced_after_the_reconnect`（停止中に reclaim された data plane は再接続で即座に `Fenced` を知り、invoke は 503 `provider_unavailable` のまま）。E2E の再接続ログ `control plane reachable again; ... dispatcher lease re-validated`（`data-plane.log`） |
| 2 プロセス E2E | 実装済み（process provider） / 未検証（Firecracker） | `scripts/control-plane/outage-e2e.sh`、`docs/evidence/20260917T045347Z-split-process/`（19/19 PASS）。既存 E2E の回帰: `docs/evidence/20260917T045324Z-process/`（`scripts/e2e/demo.sh`、28/28 PASS） |
| 予算 token との接続 | 未着手（P3、PLT-4643） | policy は egress profile の許可だけ |

残り・制約: data plane は control plane と同じ `data_dir`（台帳と artifact store を共有する 1 cell）でしか動かない（独立した store と artifact 配布は後続）。配信は平文 HTTP で `internal_token` の rotation 手順は無い。`combined` の gateway は同じ `state.db` への他プロセスの commit のたびに publication を刻印し直す（function 数に比例）。期限は data plane の wall-clock で判定し、時計のずれの補正は無い。複数 region / cell、push 配信は未着手。

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
| M9 | 実測済み | guest vCPU 1、MemTotal 232 MiB（要求 256 MiB）、128 MiB 環境での 512 MiB 確保は `crash` / `Runtime.Crash`（`docs/evidence/isolation-20260916T020934Z/resources.json`、`alloc-invocation.json`）。ephemeral storage は PLT-4622 で強制し、64 MiB の環境で 58 MiB 書いて `ENOSPC`（`docs/evidence/isolation-20260917T011555Z/disk.json`）。jailer と host cgroup 下の再計測でも同じ（`docs/evidence/isolation-20260917T041930Z/`） |
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
| self-hosted KVM runner での `.github/workflows/kvm-integration.yml` | 未検証 | workflow・gate（`kvm-gate`）・runner 登録手順は PLT-4645 で用意（`docs/ci.md` §5）。runner が未登録のため `kvm` job は一度も実行されていない |
| TiDB 永続化（PLT-4618） | 未着手 | ADR-0003 で単一 host は埋め込み SQLite（`state.db`、migration 実装済み）と決定。TiDB は将来の adapter |
| network と drive の帯域上限、IO の cgroup 上限 | 未着手 | VMM への host 側 cgroup（CPU / memory / pids）と 2 tenant 同居の計測は PLT-4622 で実装・実測済み（上の PLT-4622 5e・8、`docs/evidence/isolation-20260917T041930Z/`）。`io.max`、drive と NIC の `rate_limiter` は無い。実測は aarch64 nested 1 host だけ |
| Kata / Cloud Hypervisor adapter | 未着手 | ADR-0001 で後続 adapter と決めた |
| OCI image の pull・実行 | 未着手 | P1 非対象（参照の受理と理由付き `Failed` だけ） |
| jailer の残り（環境ごとの uid、network namespace） | 未着手 | jailer 自体は PLT-4622 で導入・実測（上の PLT-4622 7）。`docs/threat-model.md` §14-2 |

## 更新ルール

- 各 PR で該当行の状態と証跡を更新する。「実装済み」にするときはテスト名・ファイルが実在することを、「KVM実測あり」にするときは `docs/evidence/` の該当ディレクトリを PR で示す。
- 実機の記録の置き場所: `scripts/e2e/demo.sh` は `docs/evidence/<UTC>-<provider>/`、`scripts/control-plane/outage-e2e.sh` は `docs/evidence/<UTC>-split-process/`、`scripts/kvm/smoke.sh` は `docs/evidence/kvm-<UTC>/` に書く。profile（host arch、nested virtualization の有無、vCPU / memory、kernel / rootfs の sha256）を読み取れるようにする。
