# tachyon-serverless

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/quantum-box/tachyon-serverless/actions/workflows/ci.yml/badge.svg)](https://github.com/quantum-box/tachyon-serverless/actions/workflows/ci.yml)

Tachyon のサーバーレス実行基盤の **動作プロトタイプ**。single-binary の関数を登録し、版（revision）を publish し、同期 invoke で microVM（Firecracker）または開発用の子プロセスで実行し、結果・ログ・起動証跡を回収して rollback するまでを、この public リポジトリ単体で通す。

> [!WARNING]
> P1 プロトタイプです。SLA・料金・本番運用の約束はありません。API と設定は予告なく変わります。

## 何であって、何でないか

**である**

- `関数登録 → Revision publish → Invoke → 実行環境起動 → guest runtime bridge → handler 実行 → result / log 回収 → rollback → 環境破棄` の縦断を動かす最小実装
- 実行 provider を `ExecutionProvider` trait の背後に隠し、**Firecracker (Linux/KVM)** を第一候補、**process provider（隔離なし・開発専用）** を macOS 用の代替とする
- 失敗の分類（`user_error` / `crash` / `init_error` / `timeout` / `outcome_unknown` ...）と、host 側計測の timings、環境が本当に起動した証跡（`boot_evidence`）を API と CLI で見せる

**でない**

- 本番サービス、マルチノード、永続 DB（P1 は in-memory + `state.json`）
- warm 再利用、idle 休止、snapshot/restore、非同期 invoke、cron、Console UI、egress 制御、OCI image の pull（Capability / API で明示的に `unsupported`）

## 状態

| 項目 | 状態 |
|---|---|
| フェーズ | P1（Linear PLT-4617〜PLT-4630） |
| 実行 provider | `process`（macOS/Linux, 隔離なし）/ `firecracker`（Linux/KVM） |
| 同時実行 | 1 環境 1 実行、destroy-after-invoke |
| 永続化 | in-memory + `data_dir/state.json` |
| SLA | なし |

## アーキテクチャ

```
                 tsls (apps/cli)                 curl / SDK client
                        │  HTTP (docs/api.md)         │
                        ▼                             ▼
        ┌──────────────────────────────────────────────────┐
        │ apps/gateway  (axum)  管理 API / invoke / logs   │
        │   auth → Function/Revision/Alias → capacity →    │
        │   ExecutionProvider.create_environment(spec)     │
        └───────────────┬──────────────────────────────────┘
                        │ provider-port (trait)
          ┌─────────────┴──────────────┐
          ▼                            ▼
  providers/firecracker         providers/process (dev only, no isolation)
  Firecracker API + vsock       child process + unix socket
          │                            │
          ▼ microVM                    ▼ host process
  ┌──────────────────┐          ┌──────────────────┐
  │ runtime-bridge   │ Hello /  │ runtime-bridge   │
  │ (/sbin/init)     │ HelloAck │                  │
  │  Runtime API ──▶ user process (sdk::run / serve_http)
  └──────────────────┘          └──────────────────┘
```

- crate の依存方向と実行の流れ（同期 invoke の 11 ステップ）: [docs/architecture.md](docs/architecture.md)
- host ↔ bridge ↔ user process のプロトコル: [docs/protocol.md](docs/protocol.md)

## Quickstart（macOS / Linux, process provider）

process provider は **隔離を提供しない**（関数はあなたの子プロセスとして動く）。開発専用。

```sh
# 1. toolchain (Rust 1.95.0 は rust-toolchain.toml / mise.toml で固定)
mise install

# 2. build
cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli \
            -p tachyon-serverless-runtime-bridge -p example-hello

# 3. 使い捨て gateway で hello を end-to-end 実行
target/debug/tsls dev --binary target/debug/example-hello --payload '{"name":"demo"}'
```

gateway を自分で起動する場合:

```sh
target/debug/tachyon-serverless-gateway --config config/gateway.dev.toml   # 別ターミナル
export TSLS_API_URL=http://127.0.0.1:8080 TSLS_TOKEN=dev-token-tenant-a
target/debug/tsls provider
target/debug/tsls functions create --name hello
target/debug/tsls functions deploy --function hello --binary target/debug/example-hello --env GREETING=v1
target/debug/tsls functions invoke hello --payload '{"name":"demo"}'
target/debug/tsls functions logs --function hello
```

## Quickstart（Linux / KVM, firecracker provider）

`/dev/kvm` のある Linux ホストで Firecracker microVM を使う。kernel / rootfs / firecracker の準備（`.kvm/`）、musl での guest ビルド、smoke テストの手順は [docs/kvm.md](docs/kvm.md)。

```sh
cargo build --release --target x86_64-unknown-linux-musl \
  -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn
target/debug/tachyon-serverless-gateway --config config/gateway.firecracker.toml
TSLS_PROVIDER=firecracker scripts/e2e/demo.sh
```

## E2E デモ（PLT-4630）

```sh
scripts/e2e/demo.sh                       # process provider
TSLS_PROVIDER=firecracker scripts/e2e/demo.sh
```

build → gateway 起動 → provider 表 → `hello` / `http-axum` / `cpu-burn` の作成と deploy → 成功 / `user_error` / `crash` / `init_error` / HTTP passthrough / `timeout`（壁時計 < 10 s）/ 孤児プロセス無し / ログ / boot evidence / rollback / 他テナント 404 / cancel を順に検証し、`docs/evidence/<UTC>-<provider>/` に `provider.json` `invocations.json` `summary.json` `gateway.log` を残して、gateway を SIGTERM で止め、PASS/FAIL 表を出す。ヘルパは `scripts/e2e/lib.sh`、孤児チェックは `scripts/e2e/orphan-check.sh`、ヘルパ自体のテストは `scripts/e2e/selftest.sh`。

## CLI

`tsls` の全コマンド・終了コード・`--json` の約束は [docs/cli.md](docs/cli.md)。

```
tsls functions {create,list,get,delete,deploy,invoke,http,invocations,invocation,logs,
                revisions,revision,aliases,alias-set,rollback,cancel}
tsls provider | health | dev
```

## API

エンドポイント表・リクエスト / レスポンスのフィクスチャ・エラー本文・ステータスコードは [docs/api.md](docs/api.md)。gateway は `GET /openapi.json` で OpenAPI を返す。DTO の正本は `crates/api-types`。

## 設計文書

- [docs/architecture.md](docs/architecture.md) — 目的・crate 構成・invoke の流れ・設定・決め事
- [docs/protocol.md](docs/protocol.md) — host ↔ bridge frame、Runtime API、Firecracker guest 規約
- [docs/threat-model.md](docs/threat-model.md) — 脅威モデル（process provider の非隔離を含む）
- [docs/kvm.md](docs/kvm.md) — Linux/KVM 環境の準備と smoke
- [docs/adr/](docs/adr/) — 設計判断の記録
- Linear: [Tachyon Serverless — 動作プロトタイプ](https://linear.app/quantum-box/project/tachyon-serverless-動作プロトタイプ-269d7e9f95f9)
- RFC: quantum-box/knowledge#284「Tachyon Serverless 全体設計 RFC v0.1」

## リポジトリ構成

```
crates/domain          ID / entity / 状態遷移 / エラー分類
crates/protocol        host↔bridge frame, Runtime API
crates/provider-port   ExecutionProvider ほか port trait
crates/api-types       管理 / Invoke API の DTO
crates/application     usecase, in-memory repository, invoke pipeline
crates/providers/*     process / firecracker / fake
crates/runtime-bridge  guest 側 agent
crates/sdk             run(handler) / serve_http(router)
apps/gateway           axum gateway
apps/cli               tsls
examples/*             hello / http-axum / cpu-burn
scripts/e2e            デモと検証スクリプト
```

## 非対象

warm 再利用、idle 休止、snapshot/restore、非同期 invoke、cron、Console UI、TiDB 永続化、egress restricted / public-web、OCI image の pull。

## コントリビュート

Issue や Pull Request を歓迎します。build / test / lint の手順は [CONTRIBUTING.md](CONTRIBUTING.md)、参加にあたっては [行動規範](CODE_OF_CONDUCT.md) を守ってください。

## セキュリティ

脆弱性を発見した場合は、公開 Issue ではなく [SECURITY.md](SECURITY.md) の手順で報告してください。secret の値は HelloAck の env にだけ載り、ログ・API・`state.json` には書きません。

## ライセンス

[MIT License](LICENSE) © 2026 Quantum Box, Inc.
