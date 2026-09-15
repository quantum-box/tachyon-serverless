# Tachyon Serverless — プロトタイプ アーキテクチャ

- 対象: Linear プロジェクト「Tachyon Serverless — 動作プロトタイプ」P0〜P1（PLT-4613〜PLT-4630）
- 基準 RFC: quantum-box/knowledge PR #284「Tachyon Serverless 全体設計 RFC v0.1」
- 状態: プロトタイプ。SLA・料金・本番運用の約束ではない。

## 1. 目的と範囲

public な独立リポジトリ単体で、次を通す。

```
関数登録 → Revision publish → Invoke → 実行環境起動 → guest runtime bridge
  → handler 実行 → result / log 回収 → rollback → 環境破棄
```

- 実行 provider は **Firecracker (Linux/KVM)** を第一候補とし、`ExecutionProvider` trait の背後に隠す。
- macOS などの開発機では **process provider**（隔離なし、dev 専用）で同じ縦断を確認できる。process provider の成功は microVM の成功ではない。gateway は `profile = "production"` で dev_only provider を拒否する。
- P1 は 1 環境 1 同時実行、destroy-after-invoke。再利用・warm・snapshot は未対応（Capability に `Unsupported` と明示）。

## 2. crate 構成と依存方向

```
apps/gateway  ──▶ application ──▶ domain
   │                 │  ▲            ▲
   │                 ▼  │            │
   │            provider-port ◀── providers/{process,firecracker,fake}
   │                 │
   ▼                 ▼
api-types        protocol ◀── runtime-bridge (guest) ◀── sdk (user code)
   ▲
apps/cli
```

| crate | 役割 | 依存してはいけないもの |
|---|---|---|
| `crates/domain` | ID, entity, 状態遷移, エラー分類 | tokio / axum / DB / hypervisor |
| `crates/protocol` | host↔bridge frame, bridge↔SDK Runtime API | axum, provider 実装 |
| `crates/provider-port` | `ExecutionProvider` ほか port trait | 具体 provider |
| `crates/api-types` | 管理/Invoke API の DTO, エラー code | application |
| `crates/application` | usecase, repository (in-memory), invoke pipeline, bridge session (host 側), watchdog, log 保持 | axum, hypervisor 固有 API |
| `crates/providers/process` | 子プロセス + unix socket。dev 専用 | — |
| `crates/providers/firecracker` | Firecracker API socket, vsock, drive, cleanup | domain 以外の上位 |
| `crates/providers/fake` | テスト専用。duplex stream 上のスクリプト guest | — |
| `crates/runtime-bridge` | guest 側 agent。vsock/unix で host と接続、Runtime API を HTTP で提供、user process を起動・監視 | — |
| `crates/sdk` | `run(handler)`, `serve_http(router)`, `Context` | bridge 内部 |
| `apps/gateway` | axum。管理 API + Invoke + logs + OpenAPI | — |
| `apps/cli` | `tsls` CLI。deploy / invoke / logs / rollback / dev | application（HTTP 経由のみ） |
| `examples/*` | hello / http-axum / cpu-burn | — |

## 3. 実行の流れ（同期 Invoke, P1）

```
client ─POST /v1/functions/{id}:invoke─▶ gateway
  1 認証: Bearer token → Principal{tenant, roles}. 他 tenant の資源は 404。
  2 Function 取得 (deleted → 409 function_deleted)。alias→Revision 解決 (Ready でなければ 409 revision_not_ready)。
     Revision は受付時に固定される。実行中に alias を変えても版は変わらない。
  3 payload 上限 (limits.max_payload_bytes → 413)。Idempotency-Key があれば既存 Invocation を返す
     (同キー・異なる input digest → 409 conflict)。
  4 Invocation(Accepted) + deadlines を作成し保存。
     queue_deadline   = now + queue_timeout (config, default 10s)
     client_deadline  = now + min(client_timeout_ms header, timeout_seconds + init + queue)
  5 容量: revision.max_concurrency と gateway 全体上限の semaphore。空きがなければ bounded queue
     (config max_queue) で queue_deadline まで待つ。溢れ → 429 capacity_exceeded。queue_deadline 超過 → 504 queue_timeout。
  6 ExecutionEnvironment(Requested→Provisioning) を作成。provider.create_environment(spec)
     (connect_timeout = initialization_timeout_seconds)。失敗 → InitError / PlatformError、環境 Failed。
  7 BridgeSession: Hello 受信 → 検証 → HelloAck(entrypoint, env(+secrets), limits) 送信。
     Ready を init_deadline まで待つ。InitError / 接続断 / timeout → 502 init_error、環境終了。
  8 Attempt(Dispatched, epoch) + Lease を作成。Invocation(Running)。Invoke frame 送信。
     execution_deadline = dispatch 時刻 + timeout_seconds。
  9 Response / Error を execution_deadline まで待つ。
     - Response          → Succeeded (output inline ≤ config inline_output_max, http_status は http event のみ)
     - Error(Handler)    → Failed{user_error}     - Error(Panic)  → Failed{crash}
     - Error(Crash)      → Failed{crash}          - Error(ResponseTooLarge/Protocol) → Failed{platform_error}
     - deadline          → Cancel(grace 1s) 送信 → provider.terminate → Failed{timeout}
     - 接続断 (結果未受信) → OutcomeUnknown（自動再実行しない）
     - cancel API       → Cancel → terminate → Cancelled
 10 Lease release、Attempt/Invocation terminal 更新、UsageEvent(host 観測)、
     provider.terminate_environment（destroy-after-invoke、冪等）。timeout/強制終了した環境は再利用しない。
 11 client 切断は完了と見なさない。invoke タスクは spawn され、切断後も deadline まで追跡し記録する。
```

タイミング (`AttemptTimings`) は host 側で計測する: queue_wait / environment_boot (create 開始→bridge 接続) / runtime_init (接続→Ready) / handler (Invoke 送信→Response) / response / total。

## 4. 環境変数・設定

gateway 設定 `config/gateway.toml`（例は `config/gateway.dev.toml`）:

```toml
listen = "127.0.0.1:8080"
profile = "dev"                # dev | production。production では dev_only provider を拒否
data_dir = "./data"            # artifacts と state.json

[provider]
kind = "process"               # process | firecracker

[provider.process]
bridge_binary = "target/debug/tachyon-serverless-runtime-bridge"
workdir = "./data/process"

[provider.firecracker]
firecracker_binary = ".kvm/bin/firecracker"
kernel = ".kvm/vmlinux"
rootfs = ".kvm/rootfs.ext4"    # read-only base rootfs（bridge を /sbin/tachyon-init として含む）
workdir = ".kvm/run"           # 環境ごとの socket / drive / log
vsock_port = 5000

[capacity]
max_concurrency = 8            # gateway 全体
max_queue = 32
queue_timeout_seconds = 10

[[identity.tokens]]
token = "dev-token-tenant-a"
tenant_id = "tn_01hzzzzzzzzzzzzzzzzzzzzzza"
subject = "dev-a"
roles = ["deploy", "invoke"]

[[secrets.bindings]]
tenant_id = "tn_01hzzzzzzzzzzzzzzzzzzzzzza"
binding_ref = "demo-secret"
value = "s3cr3t-a"
```

`tn_...` は `<prefix>_<26 文字 lowercase ULID>` 形式でなければならない（domain が検証する）。

## 5. 決め事（実装者が守ること）

1. domain / application は `firecracker` `kube` `axum` を import しない。provider は `ExecutionProvider` だけを実装する。
2. Secret 値は `SecretValue`（Debug は redacted）で運び、HelloAck の env にだけ載せる。ログ・API 応答・state.json に書かない。
3. guest の自己申告（handler_ms, Ready 時刻）は参考値。課金・timeout・権限の根拠は host 側計測。
4. 結果は `attempt_id` と `epoch` が Lease と一致するものだけ受理する。
5. terminate は冪等。terminate 後に process / socket / tap / drive / workdir が残らない（`TerminateReport.cleaned` に列挙）。
6. process provider は `TACHYON_UNISOLATED=1` を guest env に載せ、`Capabilities.dev_only = true`。
7. すべての ID は domain の型を使い、文字列で持ち回らない。
8. 失敗の分類は `ErrorClass` を正とし、HTTP status は `api-types::ErrorCode::http_status()`。
9. ログは invocation 単位で `Limits` の行数・bytes 上限を守り、超過分は `dropped = true` で観測できるようにする。
10. 「動いた」証跡: 環境の `BootEvidence`（guest_boot_id, host_pid, provider details）を Invocation の attempt に残し、API と CLI で表示する。

## 6. 非対象（P1）

warm 再利用、idle 休止、snapshot/restore、非同期 invoke、cron、Console UI、TiDB 永続化、egress restricted/public-web、OCI image の pull。これらは Capability / API で明示的に Unsupported を返す。
