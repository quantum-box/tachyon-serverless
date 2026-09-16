# Changelog

このプロジェクトの主な変更点を記録します。

フォーマットは [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) に基づき、
バージョニングは [Semantic Versioning](https://semver.org/lang/ja/) に従います。

## [Unreleased]

### Added

- MIT License を追加
- README / CONTRIBUTING / CODE_OF_CONDUCT / SECURITY などのリポジトリ基本ドキュメントを追加
- GitHub の Issue / Pull Request テンプレートと Dependabot 設定を追加
- P2 の前提（Linear PLT-4627 / PLT-4618）
  - 再起動時の分類を dispatch 済みかどうかで分ける。`Running` だったものは `OutcomeUnknown{Host.Restarted}`、未 dispatch は `Failed{platform_error}`
  - 起動時に `ExecutionProvider::list_environments` で孤児環境を回収し、結果を構造化ログと `GET /readyz` の `reconcile` に出す。`[reconcile] on_startup` で無効化できる
  - 実行状態の永続化方針を `docs/adr/0003-execution-state-persistence.md` に決定（P2 が要求する複数プロセス間の原子性と移行手順）
- P1 動作プロトタイプ（Linear PLT-4613〜PLT-4630）。SLA なし、API と設定は予告なく変わる
  - Rust workspace（Rust 1.95.0 / edition 2024）と契約 crate: `crates/domain`（ID・entity・状態遷移・`ErrorClass`・`Limits`）、`crates/protocol`（host ↔ bridge frame、Runtime API）、`crates/provider-port`（`ExecutionProvider` ほかの port）、`crates/api-types`（DTO・`ErrorCode`）
  - `crates/application`: function / revision / alias / invoke / 履歴の usecase、in-memory repository と `data_dir/state.json` への永続化（壊れたファイルは退避方法を示して拒否）、静的 token と secret binding、容量と queue、queue / init / execution の deadline、cancel、再起動時の reconcile
  - `apps/gateway`（axum）: 管理 API、同期 invoke と HTTP アダプター、logs・履歴・usage、`/healthz`・`/readyz`・`/openapi.json`、`profile = "production"` での dev_only provider の拒否
  - `apps/cli`（`tsls`）: `functions`（create / deploy / invoke / http / logs / rollback / cancel ほか）、`provider`、`health`、使い捨ての gateway で往復する `dev`
  - 実行 provider: `crates/providers/firecracker`（Firecracker v1.17.0、vsock、環境ごとの read-only function drive、host 強制 timeout、冪等な terminate、`fc-smoke`）、`crates/providers/process`（隔離なし・開発専用）、`crates/providers/fake`（テスト用）
  - `crates/runtime-bridge`（guest の `/sbin/tachyon-init`。unix / vsock transport、PID 1 で init モードに自動で入り loopback を up にする）と `crates/sdk`（`run(handler)`、`serve_http(router)`）
  - サンプル `examples/hello`、`examples/http-axum`、`examples/cpu-burn`
  - スクリプト `scripts/kvm/`（preflight / bootstrap / build-rootfs / smoke / teardown）と `scripts/e2e/`（demo / orphan-check / selftest）
  - CI `.github/workflows/ci.yml`（fmt / clippy / test / build、musl の guest ビルド、shell scripts）と手動起動の `.github/workflows/kvm-integration.yml`
  - 文書 `docs/architecture.md`、`docs/protocol.md`、`docs/threat-model.md`、ADR 0001 / 0002、`docs/api.md`、`docs/cli.md`、`docs/kvm.md`、`docs/inventory-tachyon-apps.md`、`docs/acceptance.md`
  - 実行記録 `docs/evidence/20260915T073238Z-process/`（process provider、macOS、E2E 27/27）、`docs/evidence/kvm-20260915T080221Z/`（Firecracker microVM の smoke、aarch64 Linux/KVM、Lima の nested virtualization）、`docs/evidence/20260915T125610Z-firecracker/`（Firecracker provider の E2E 27/27）
  - 2026-09-16 のコードレビューで確認された指摘と、受入条件ごとの残りの未検証項目は `docs/acceptance.md` に記載
