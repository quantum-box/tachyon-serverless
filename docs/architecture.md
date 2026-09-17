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
- P1 は 1 環境 1 同時実行、destroy-after-invoke。環境 pool（warm 再利用）は §4「環境 pool と再利用キー」の 2 重 gate の背後にあり、既定では働かない。process は `idle_quiesce` / `idle_resume` を `Unsupported`、firecracker は PLT-4633 の実機計測（`docs/evidence/warm-20260916T162532Z/`）を経て `Supported` と報告する。`[pool]` の既定が off なので、**既定の設定では同梱のどちらの provider でも destroy-after-invoke のまま**である。snapshot は未対応（Capability に `Unsupported` と明示）。

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
| `crates/durable-port` | `EventQueue`（永続 queue）と `ObjectStore`（tenant / region 別の暗号化 object）の port、queue 契約テスト（feature `testkit`）。PLT-4638 | 具体 queue / storage |
| `crates/adapters/queue-nats` | NATS JetStream の `EventQueue`（stream as code、認証必須）と検証用 `tachyon-queue-probe`。PLT-4638 | application |
| `crates/api-types` | 管理/Invoke API の DTO, エラー code | application |
| `crates/application` | usecase, repository (in-memory), invoke pipeline, bridge session (host 側), watchdog, log 保持, 設定配信と data plane の cache（`control/`、PLT-4636）, 埋め込み SQLite queue・filesystem object store・object GC（`durable/`、PLT-4638） | axum, hypervisor 固有 API, HTTP client |
| `crates/providers/process` | 子プロセス + unix socket。dev 専用 | — |
| `crates/providers/firecracker` | Firecracker API socket, vsock, drive, cleanup | domain 以外の上位 |
| `crates/providers/fake` | テスト専用。duplex stream 上のスクリプト guest | — |
| `crates/runtime-bridge` | guest 側 agent。vsock/unix で host と接続、Runtime API を HTTP で提供、user process を起動・監視 | — |
| `crates/sdk` | `run(handler)`, `serve_http(router)`, `Context`。実験 feature `experimental-restore` で `lifecycle`（PLT-4651） | bridge 内部 |
| `apps/gateway` | axum。管理 API + Invoke + logs + OpenAPI。`role = "data_plane"` では invoke だけ（設定は `GET /v1/internal/config` から pull、`config_client.rs`） | — |
| `apps/cli` | `tsls` CLI。deploy / invoke / logs / rollback / dev | application（HTTP 経由のみ） |
| `apps/load` | `tsls-load`: 上限を宣言した local 限定の負荷シナリオ、`/metrics` の sampling、検出器、timeline（PLT-4637、`docs/metrics.md` §6） | HTTP 経由のみ（test だけ application の metric catalog を読む） |
| `examples/*` | hello / http-axum / cpu-burn / isolation-probe / restore-aware（実験、PLT-4651） | — |

## 3. 実行の流れ（同期 Invoke, P1）

```
client ─POST /v1/functions/{id}:invoke─▶ gateway
  1 認証: Bearer token → Principal{tenant, roles}。invoke 系は配信済み grant（認可 lease 付き）の cache だけで引く
     (PLT-4636、§4「設定配信と認可 lease」)。lease 切れ → 503 Host.AuthLeaseExpired、未知の tenant → 403
     Host.UnknownTenant、未配信 → 503 Host.ConfigNotDelivered。他 tenant の資源は 404。
  2 Function 取得 (deleted → 409 function_deleted)。alias→Revision 解決 (Ready でなければ 409 revision_not_ready)。
     function / route / revision / policy はすべて設定 cache から読み、管理 store は読まない。期限切れ → 503
     Host.ConfigExpired、route の先や pin した revision が未配信 → 503 Host.ConfigNotDelivered、egress が policy 外 →
     403 Host.PolicyDenied。環境再利用が off なら cold start の可否 (§4) もここで判定する。
     Revision は受付時に固定される。実行中に alias を変えても版は変わらない。route を選んだ alias の generation を
     Invocation.alias_generation に記録する（受付時刻 = route の解決時刻、PLT-4635）。削除中の function（deleting）も 409。
  3 payload 上限 (limits.max_payload_bytes → 413)、trace id ≤ 256 bytes、Idempotency-Key 1..=256 文字 (→ 400)。
     ここまで何も記録しない (拒否された request は key を消費しない)。Idempotency-Key が既存 Invocation に
     結び付いていれば、容量に関係なくそれを返す (同キー・異なる input digest → 409 conflict、本文に結び付いた
     invocation_id と error_type = Host.IdempotencyKeyReused)。その Invocation を別の gateway が実行中なら、
     台帳を追って terminal になるか元の client_deadline まで待ってから返す (再実行しない)。key は Invocation が
     terminal になってから [store] idempotency_retention_seconds 後に失効する (PLT-4631)。
  4 Invocation(Accepted, dispatcher_id = この gateway の dispatcher) + deadlines を組み立てる。
     dispatcher の lease を失っている (heartbeat が拒否された) gateway は受け付けない (503 provider_unavailable)。
     queue / init / execution は client_deadline を超えない。
     client_deadline  = now + min(client_timeout_ms header, timeout_seconds + init + queue)
     queue_deadline   = min(now + queue_timeout (config, default 10s), client_deadline)
  5 容量 (admission、PLT-4634、ADR-0006): 配置 (required_region と node の region) が合わなければ 503 placement、
     revision の起動失敗 breaker が open なら 503 circuit_open。tenant ごとの公平 queue に入れ、node の資源
     (revision の resources + VMM / bridge の overhead)、node の max_concurrency、tenant quota、
     revision.max_concurrency、起動の合流 (desired)、start rate をすべて満たした順に grant する。待機中のまま
     queue の件数 / payload bytes / tenant の持ち分を超えたら 429 capacity_exceeded (reason queue_full / quota。
     ledger にも key にも残らない)。queue_deadline 超過 → 504 queue_timeout (reason capacity / quota /
     queue_deadline)。grant は cold 起動の予約 (Starting) か、pool の idle 環境の約束 (warm) で、予約は
     Ready で Busy、pool へ渡すと pool が持ち、環境が消えたときに解放される (§4「admission・autoscaler」)。
     Invocation の保存と Idempotency-Key の結び付けは 1 回の store 更新で行う (同 key の並行 request は
     1 つだけが受け付けられ、残りは同じ Invocation を返す)。
  6 warm の環境が無く cold start になる場合、revision と tenant の認可が今も有効で、provider の preflight が失敗して
     おらず、control plane が到達不能なら [control_plane_outage] allow_cold_start = true であることを確認する。
     満たさなければ環境を作らず Failed{platform_error, Host.ConfigExpired | Host.AuthLeaseExpired |
     Host.ProviderControlUnavailable | Host.ColdStartRestricted} (HTTP 503)。
     待機中・環境の用意の前・dispatch の直前に function が削除されていれば、handler を起動せず
     Failed{platform_error, Host.FunctionDeleted} (HTTP 409、PLT-4635)。
     ExecutionEnvironment(Requested→Provisioning) を作成し、HelloAck(entrypoint, env(+secrets), limits) を組み立てる。
     secret binding を解決できなければ環境を作らずに 502 init_error (Host.SecretBindingUnavailable。他 tenant の
     binding と存在しない binding は同じ応答)。secret backend の障害は 500 platform_error (Host.SecretBackend)。
     provider.create_environment(spec) (connect_timeout = init_deadline までの残り)。失敗 → InitError / PlatformError、環境 Failed。
  7 BridgeSession: Hello 受信 → 検証 → HelloAck 送信。Ready を init_deadline まで待つ。
     InitError / 接続断 / timeout → 502 init_error、環境終了。
     client_deadline で待ちを打ち切った場合と、Ready 後 (Attempt 作成前) に client_deadline を過ぎていた場合は
     handler を起動せず 504 timeout (Host.ClientDeadline)、環境 stop + terminate(Cancelled)。
  8 SlotStore::acquire で slot を取る: 環境の (state = Ready, epoch) を CAS し、epoch を 1 進めて Busy にし、
     Lease(owner = dispatcher, attempt, epoch, 所有期限 = now + lease_ttl, execution_deadline)、
     Attempt(Dispatched, epoch)、Invocation(Running) を 1 トランザクションで書く。負け (環境が動いた、fenced、
     dispatcher が reclaim 済み、Invocation が既に terminal) なら handler を起動せず Failed{platform_error,
     Host.SlotLost}。Invoke frame 送信。
     execution_deadline = min(dispatch 時刻 + timeout_seconds, client_deadline)。guest の deadline_ms も同じ値。
     Invoke を届けられなかった場合 (handler は未開始、OutcomeUnknown にしない):
     encode 不能 → 500 platform_error (Host.InvokeTooLarge)。書き込み失敗 → guest が閉じる前に送った
     Exited を読めれば 502 crash (Runtime.Exited)、無ければ 502 crash (Host.BridgeDisconnectedBeforeInvoke)。
     再利用環境 (warm) への書き込み失敗も同じ手順・同じ分類で attempt に記録する。分類は誰が dispatch したかで
     変わらない。warm だけはその後 cold で 1 回だけやり直す (§4「環境 pool と再利用キー」)。
  9 Response / Error を execution_deadline まで待つ。
     - Response          → Succeeded (output inline ≤ config inline_output_max, http_status は http event のみ)
     - Error(Handler)    → Failed{user_error}     - Error(Panic)  → Failed{crash}
     - Error(Crash)      → Failed{crash}          - Error(ResponseTooLarge/Protocol) → Failed{platform_error}
     - deadline          → Cancel(grace 1s) 送信 → provider.terminate → Failed{timeout}
                           (client_deadline で打ち切った場合は error_type = Host.ClientDeadline)
     - 接続断 (Invoke 書き込み後、結果未受信) → OutcomeUnknown（自動再実行しない）
     - cancel API       → Cancel → terminate → Cancelled
 10 SlotStore::complete (fenced callback): Lease が未 release で (attempt_id, epoch) が一致し、環境が同じ epoch の
     ときだけ Lease release と Attempt/Invocation の terminal 更新を 1 トランザクションで書く。reclaim 済みなら
     何も書かず (stale)、結果も返さない (台帳の OutcomeUnknown{Host.LeaseExpired} が答え)。UsageEvent(host 観測)、
     provider.terminate_environment（destroy-after-invoke、冪等）。timeout/強制終了した環境は再利用しない。
     driver が panic した場合も terminate(Crashed)、Lease 解放、Attempt / 環境 Failed、EnvironmentStopped を記録する。
     UsageEvent は計測点で usage journal に同期 append してから次へ進む。attempt の後始末の後に AttemptSettled
     (区間・outcome・bytes)、環境の終わりに EnvironmentStopped (PLT-4642、§4「usage の計測と仮料金」)。
     新規 invoke は受付前に journal の残量を確認し、満杯・停止なら 503 usage_journal_full で何も記録しない。
     [budget] enabled の gateway は、static admission（placement・quota 0 等）の後・queue / capacity の前に
     run の最大料金を予算 store に予約し（上限を超えるなら 429 budget_exhausted、予算不明・collector 停止・
     store 停止なら 503 budget_unavailable）、queue で待った run は grant の時点で再確認する。run の終わりに
     AttemptSettled を出した attempt id を記録し、collector の後に ledger の実測で精算する (PLT-4643、§4「予算」)。
 11 client 切断は完了と見なさない。invoke タスクは spawn され、切断後も deadline まで追跡し記録する。
```

タイミング (`AttemptTimings`) は host 側で計測する: queue_wait / environment_boot (create 開始→bridge 接続) / runtime_init (接続→Ready) / handler (Invoke 送信→Response) / response / total。

## 4. 環境変数・設定

gateway 設定 `config/gateway.toml`（例は `config/gateway.dev.toml`）:

```toml
listen = "127.0.0.1:8080"
profile = "dev"                # dev | production。production では dev_only provider を拒否
data_dir = "./data"            # artifacts と state.db（台帳）

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
max_concurrency = 8            # node 全体の Starting + Busy
max_queue = 32
queue_timeout_seconds = 10
max_queue_bytes = 33554432     # 待ち行列の payload 合計

[capacity.node]                # PLT-4634。省略時は資源の上限なし
name = "kvm-node"
region = "jp"                  # 配置ラベル。required_region はこれと一致する node でだけ動く
memory_mib = 6144              # cpu_millis / ephemeral_storage_mib も同様。省略した次元は上限なし
vmm_overhead_memory_mib = 64   # 環境ごとに予約へ足す。firecracker では VMM cgroup の memory_overhead_mib と揃える（既定 16）
bridge_overhead_memory_mib = 0 # guest 内の bridge は guest memory に含まれる（process provider の既定 8）

[capacity.tenant_defaults]     # [[capacity.tenants]] で tenant ごとに上書き（tenant_id, max_concurrency, max_queue, weight, required_region）
max_concurrency = 6            # 1 tenant の同時環境数。node の max_concurrency 未満にする（非 preemptive のため）
max_queue = 16

[capacity.start_rate]
per_second = 20
burst = 20

[capacity.circuit_breaker]
failure_threshold = 5
cooldown_seconds = 30

[capacity.autoscaler]
rate_window_seconds = 10

[scaling]                      # PLT-4635。scale reconciler（idle sweep・min_ready・drain）
reconcile_interval_ms = 1000   # reconcile の周期
scale_down_cooldown_seconds = 30  # scale-up / 活性化の後、この間は scale-down しない（revision の既定）
# drain_timeout_seconds = 961  # drain（alias 切替・secret 世代・削除）開始からこれを過ぎて実行中の invocation は Host.DrainTimeout。
                               # 省略時 = limits.max_execution_timeout_seconds + cancel grace + 60 s（どの revision の timeout より長い）
# allow_short_drain = false    # 上の値を「最大 timeout + grace」以下にするときだけ true（長い handler を drain で止めることを受け入れる）
prestart_backoff_seconds = 5   # min_ready の先行起動が失敗した後の待ち

[store]
backend = "sqlite"             # sqlite（<data_dir>/state.db、既定）| memory（再起動で消える）
output_retention_seconds = 604800  # インライン出力の保持期限。過ぎたら digest に置き換える。0 は無期限
idempotency_retention_seconds = 86400  # Idempotency-Key の保持期限（Invocation が terminal になってから）。0 は無期限

[logs]                         # invocation log（<data_dir>/logs/logs.db、docs/adr/0018）。[store] backend = "sqlite" のときだけ
retention_seconds = 604800     # 最後の行からこれを過ぎた invocation の log を消す。0 は期限なし
max_total_bytes = 1073741824   # 保存した行の bytes の上限。超えたら terminal の invocation を古い順に消す。0 は上限なし
# max_lines_per_attempt = 2000 # 既定は [limits] max_log_lines_per_invocation
# max_bytes_per_attempt = 1048576
flush_interval_ms = 200        # writer の commit 間隔（crash で失いうる窓）

[dispatcher]                   # PLT-4631。gateway プロセスごとの所有者 id と lease
# instance = "gateway-a"       # 既定 "gateway@<listen>"。同じ data_dir を共有する gateway 同士で重複させない
lease_ttl_seconds = 30         # dispatcher lease と slot lease の所有期限
heartbeat_interval_seconds = 10  # renew と reclaim の周期（lease_ttl_seconds 未満）
max_clock_skew_ms = 2000       # 他の dispatcher の期限を判定するときに許す時計のずれ

[reconcile]
on_startup = true              # 起動時に provider の孤児環境を回収する（既定 true）

[control_plane]                # PLT-4636。設定配信と認可 lease
role = "combined"              # combined（管理 API + invoke）| data_plane（invoke だけ、設定は url から pull）
# internal_token = "..."       # GET /v1/internal/config の credential と token digest の鍵（16 bytes 以上）。
                               # combined は設定したときだけ endpoint を出す。data_plane は必須
# url = "http://127.0.0.1:8080"  # data_plane: 管理 gateway
refresh_interval_ms = 2000     # 到達できる間の refresh 周期
config_ttl_seconds = 60        # function / route / revision / policy の有効期限（最後に確認した refresh の開始から）
auth_lease_seconds = 60        # grant / tenant の有効期限（認可 lease、revoke の遅延上限）
backoff_initial_ms = 500       # refresh 失敗後の待ち（倍々）
backoff_max_ms = 10000
fetch_timeout_ms = 2000
allowed_egress = ["none", "restricted", "public-web"]  # combined が配信する policy

[control_plane_outage]
allow_cold_start = true        # control plane に届かない間も、有効な設定の範囲で新しい環境を起動するか

[pool]
enabled = false                # 環境再利用（warm）。既定 off
max_idle_per_revision = 1      # reuse key ごとに idle で残す環境数
idle_ttl_seconds = 60          # これを超えて idle な環境は sweeper が破棄する
max_total_idle = 8             # 全 reuse key 合計の idle 上限
allow_unverified_idle = false  # 計測専用。未計測（Unverified）の idle capability を受け入れる。既定 off
                               # profile = "production" では拒否される

[queue]                        # PLT-4638。既定 "none"（何も接続しない）
backend = "none"               # none | sqlite（<data_dir>/queue.db、dev 専用）| nats
# [queue.nats]
# url = "nats://127.0.0.1:14222"
# stream = "TACHYON_EVENTS"
# subject_prefix = "tachyon.events"
# user = "gateway"             # user + password_file か nkey_seed_file のどちらか一方が必須（匿名は不可）
# password_file = "target/queue/nats/gateway.password"   # mode 0600 でなければ拒否
# [queue.limits]
# max_messages = 100000
# max_bytes = 268435456
# max_message_bytes = 262144
# max_age_seconds = 604800     # 0 は拒否（無期限の stream を作らない）
# duplicate_window_seconds = 120

[objects]                      # PLT-4638。既定 "none"
backend = "none"               # none | filesystem
# root = "./data/objects"
# regions = ["local"]          # これ以外の region への put は拒否
# key_file = "secrets/object.key"   # 64 hex（32 bytes）、mode 0600。key_env とどちらか一方
# max_object_bytes = 8388608
# tenant_quota_bytes = 1073741824
# default_ttl_seconds = 604800
# orphan_grace_seconds = 3600
# gc_interval_seconds = 600

[invoke_async]                 # PLT-4639。[queue] があり台帳が sqlite のときだけ有効
inline_input_max_bytes = 65536 # これ以下の入力は台帳、超えるものは [objects]（無ければ 413）
max_pending_events = 10000     # outbox の未送信がこれ以上なら 429 backlog（queue 停止中なら 503 queue_unavailable）
max_pending_age_seconds = 300  # 最古の未送信がこれより古くても同じ
queue_deadline_seconds = 86400 # 受け付けた非同期 invocation の queue deadline（強制は PLT-4640）
publish_batch = 100
publish_interval_ms = 200      # 受付があれば即座に起こされる
claim_ttl_seconds = 30         # publisher が落ちたとき、その行が再 publish されるまでの最大の遅れ
retry_initial_ms = 500         # publish 失敗の backoff（2 倍ずつ、retry_max_ms まで）
retry_max_ms = 30000
sent_retention_seconds = 3600  # 送信済みの outbox 行を消すまで

[usage]                        # PLT-4642。usage journal / collector / 仮料金（請求はしない）
journal_max_events = 100000    # 未回収 event の上限（件数）。越える append は unjournaled として数える
journal_max_bytes = 67108864   # 同（bytes）
admission_headroom_events = 1000   # 残りがこれ以下なら新規 invoke を 503 usage_journal_full（fail closed）
admission_headroom_bytes = 1048576
on_journal_full = "refuse"     # refuse | accept_unmetered（dev profile だけ）
collect_interval_ms = 1000     # collector（journal → ledger）の間隔
collect_batch = 500
# price_table = "config/price-table.toml"   # 省略時は組み込みの provisional-dev-2026-09-v1
[usage.billing]
enabled = false                # true は設定エラー（prototype では請求を無効に固定）

[budget]                       # PLT-4643。予算の予約・上限・alert（仮料金の単位。決済はしない）
enabled = false                # true: 実行前に最大料金を予約し、予算が分からなければ受付を止める
period = "calendar_month_utc"  # 唯一の期間
# file = "config/budgets.toml" # publication のたびに読み直す（inline の tenants と併用不可）
reservation_slack_ms = 250     # 最大料金の billable ms に足す余裕
expiry_grace_seconds = 30      # 終わりを報告しない run は run deadline + これで失効（最大額を hold）
max_unsettled_age_seconds = 30 # 終わった run の精算がこれより遅れたら 503 Host.BudgetUnknown
settle_batch = 500
# [[budget.tenants]]
# tenant_id = "tn_01hzzzzzzzzzzzzzzzzzzzzzza"
# soft_limit_micros = 1000000          # alert の基準（止めない）
# alert_thresholds_percent = [50, 80, 100]
# hard_limit_micros = 5000000          # 停止（確約 = 予約 + 精算済み + unmetered hold）
# [[budget.tenants.functions]]
# function_id = "fn_..."
# hard_limit_micros = 1000000
# [budget.default_tenant]              # entry の無い tenant の予算。無ければその tenant は Host.BudgetUnknown
# hard_limit_micros = 0

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

### 起動時の後始末（restart reconcile）

前のプロセスが crash / kill で落ちた場合、台帳（`state.db`）も host の資源も中途半端に残る。gateway は次の順で収束させる。

1. **台帳（所有者の無い行）**（`crates/application/src/repository/restart.rs` の規則を `SqliteStore::open` が 1 トランザクションで適用する）。対象は **owner を持たない行**（schema 2 以前の gateway が書いた行と `state.json` の import）だけ。`Running` だった Invocation は `Invoke` frame を書き終えており handler が走った可能性があるため `OutcomeUnknown{Host.Restarted}`（自動再実行しない。`docs/threat-model.md` §9）。`Accepted` / `Queued` のままだったものは一度も dispatch していないので `Failed{platform_error, Host.Restarted}`。**ただし非同期 invocation（`invocation_inputs` を持つ `Accepted` / `Queued`、PLT-4639）は対象外**で、入力と outbox event が永続化されているので再起動後も配送を続ける。Attempt は所属する Invocation に従い、Environment は `Lost`、未 release の Lease は release、Invocation の無い Idempotency key は削除。terminal なものは触らない。
2. **台帳（dispatcher の reclaim）**（`Application::bootstrap` が新しい dispatcher を登録した直後に `Dispatcher::reclaim_ledger`）。**lease を失った dispatcher の行だけ**を回収する（次節「dispatcher・lease・fencing」）。生きている別 gateway の行には触らない。
3. **host の資源**（`crates/application/src/services/reconcile.rs::ReconcileService`、`serve()` が listener を accept させる前に呼ぶ）。まず 2 で fence された環境を `terminate_environment(Reconcile)` し、成功を確認できたものだけ `Lost` にする。続いて `ExecutionProvider::list_environments` を呼び、この gateway が active として知らない環境を `terminate_environment(Reconcile)` で回収する（process / socket / drive / workdir。冪等）。台帳には active なのに provider が知らない環境は `Lost` にする。**生きている別 dispatcher が所有する環境と fenced の環境はこの判定から外す**（数だけ `foreign` に出す）。台帳の snapshot に無い id は terminate の前に台帳を読み直し、その間に別 gateway が記録していれば外す。
4. **観測**。結果（found / adopted / terminated / failed / lost / foreign と reclaim の dispatchers / leases / invocations / fenced / terminated / pending）を構造化ログ `startup reconcile finished` に出し、`GET /readyz` の `reconcile` にも載せる。

規則: provider の列挙や terminate が失敗しても起動は止めない（warn を出して続行し、`reconcile.error` に残す）。実行中の invocation の環境は `create_environment` より前に台帳へ記録されるため必ず「知っている」側に入り、reconcile が terminate することはない。`[reconcile] on_startup = false` で 3 と 4 だけを止められる（1 と 2 は常に走り、fenced 環境の terminate は heartbeat の周期で行う）。

### 永続化（`state.db`、PLT-4618）

台帳は `<data_dir>/state.db`（埋め込み SQLite）に置く。決定と比較は `docs/adr/0003-execution-state-persistence.md`、実装は `crates/application/src/repository/`。

| 項目 | 内容 |
|---|---|
| 実装 | `SqliteStore`（`repository/sqlite/`）。`InMemoryStore` はテスト用の volatile 実装で、同じ契約テスト（`repository/contract_tests.rs`）を両方に流す |
| 書き込み | 1 操作 = 1 つの `BEGIN IMMEDIATE` トランザクション（WAL、`synchronous = FULL`、`busy_timeout` 5 秒）。失敗は呼び出し側にエラーとして返る（P1 の「warn だけ」は廃止） |
| 更新の原子性 | 読んだ値を条件にした CAS。alias は `generation`、environment は `epoch` と terminal flag（claim / release / sweep は `state` も）、invocation / attempt は terminal flag、lease は released flag。0 行更新は「負け」 |
| 行の不変条件 | `repository/guard.rs`。親と tenant が違う行、id・tenant・親 id・spec・digest など identity の変更、terminal 行の書き換え、別 epoch の environment のコピー、上限を超えるインライン出力を `RepoError::Refused` で拒否する。同一 id の insert は `Conflict` |
| schema | `repository/sqlite/migrations/NNN_*.sql`。`schema_version` に適用済みを記録し、起動時に未適用分を 1 トランザクションで適用する。前進のみで、binary より新しい schema は起動を拒否する |
| 本文 | 入力は digest とサイズだけ。出力は `[invoke] inline_output_max_bytes` 以下のときだけ本文を持ち（store 側でも `limits.max_response_bytes` で拒否）、`[store] output_retention_seconds` を過ぎると digest に置き換える（起動時と 10 分ごと）。secret 値は書かない。invocation log は `state.db` には書かない（次節「invocation log」、ADR-0018） |
| `state.json` からの移行 | `state.json` があり DB が空なら、起動時に 1 度だけ取り込み `state.json.imported-<UTC>` に rename する（削除しない）。行のある DB と `state.json` が同時にあれば両方の path を挙げて起動を拒否する。壊れた `state.json` は従来どおり拒否する。逆方向の変換は無い（戻すには rename された JSON を戻し、`state.db*` を退避する） |
| 権限 | `state.db` は新規作成時に mode `0600`。`-wal` / `-shm` も SQLite が同じ mode で作る |
| port の分割 | control-plane の 8 trait と、cell-local の `SlotStore`（`repository/slot.rs`: dispatcher、slot の acquire / complete / release、lease の renew / reclaim、fencing、pool membership）。両方とも同じ `state.db` に載る（ADR-0003 決定 1・2） |
| 対象外 | 複数 host、ネットワーク FS 上の `data_dir`、TiDB、backup / PITR、保存時暗号化。同じ `data_dir` を同じ host の複数 gateway が同時に開く構成は PLT-4631 で扱えるようになった（次節）。ただし `[dispatcher] instance` と `listen` は gateway ごとに変えること |

### invocation log（`logs.db`、ADR-0018）

`GET /v1/invocations/{id}/logs`・`tsls functions logs`・console が読む log は、台帳が durable なとき `<data_dir>/logs/logs.db`（別の SQLite、`crates/application/src/logs/`）に置く。`[store] backend = "memory"` のときは従来の memory buffer（`repository/logs.rs`）。

| 項目 | 内容 |
|---|---|
| 書き込み | bridge session の append は上限付き queue（`[logs] queue_max_lines` 20000 行 / `queue_max_bytes` 16 MiB）に入れて返るだけで IO を待たない。writer thread が `flush_interval_ms`（200 ms）ごと、または `flush_max_lines`（1000 行）で 1 トランザクションにまとめて commit（WAL、`synchronous = FULL`、busy timeout 1 s） |
| crash | commit 済みは残り file は壊れない。失うのは queue にあった行（直近 1 flush 間隔ぶん）だけ。graceful shutdown は queue を commit してから止まる |
| 読み取り | その時点までに queue に入った行の commit を `read_flush_wait_ms`（1 s）まで待って読む。`WHERE tenant_id = ? AND invocation_id = ? ORDER BY seq` |
| 上限 | 1 行 `[limits] max_log_line_bytes`（切って `truncated`）、invocation `max_log_lines_per_invocation` / `max_log_bytes_per_invocation`、attempt `[logs] max_lines_per_attempt` / `max_bytes_per_attempt`。数は行と同じトランザクションで保存するので再起動しても増えない。超過は `[tachyon] ` で始まる platform の marker 行 1 行と `dropped: true` |
| 保持 | 60 s ごと: 最後の行が `retention_seconds`（7 日）より古い invocation を消す。保存 bytes が `max_total_bytes`（1 GiB）を超えていれば、台帳で terminal の invocation を古い順に消す（実行中は消さない） |
| 障害 | queue 満杯・`logs.db` の lock・disk 満杯・開けない file では行を捨てて数え（`tsls_logs_lines_dropped_total{reason}`）、次に書けた flush で invocation に marker を書く。invoke は止めない。`/readyz` の `logs.healthy = false` は readiness に入れない |
| 権限 | `logs/` は 0700、`logs.db`（と `-wal` / `-shm`）は 0600。secret 値は host が書かない（user code が自分で出力した行は本文のまま入る） |

### dispatcher・lease・fencing（PLT-4631）

gateway プロセスは起動のたびに新しい **dispatcher**（`dsp_<ULID>`、`crates/application/src/services/dispatcher.rs`）として `state.db` の `dispatchers` に登録される（instance 名、host 名、pid、lease の期限）。受け付けた Invocation、作った ExecutionEnvironment、取った slot の Lease はすべてその dispatcher を owner として持つ。bridge session はプロセスの中にしか無いので、**環境の owner は変わらない**: 別の dispatcher は環境を fence して terminate することはできても、dispatch することはない（pool の claim / sweep も自分の環境だけ）。

| 仕組み | 内容 |
|---|---|
| slot の取得 | `SlotStore::acquire`。環境の `(state ∈ {Ready, Idle}, epoch)` の CAS、未 release の lease が無いこと、owner が lease の owner と同じで dispatcher が live（stopped / reclaimed でない）ことを条件に、epoch + 1・`Busy`・Lease（owner、attempt、epoch、所有期限、execution deadline）・Attempt・Invocation `Running` を 1 トランザクションで書く。起動したての環境は epoch 0 で、最初の attempt が epoch 1。pool の claim は `Idle` → `Ready`（予約）で epoch を動かさず、acquire が進める |
| heartbeat | `[dispatcher] heartbeat_interval_seconds` ごとに `Dispatcher::heartbeat`: dispatcher の lease と、まだ期限が来ていない自分の slot lease を `now + lease_ttl_seconds` に延ばす（1 トランザクション）。**期限を過ぎた lease は延ばさない**（誰かが reclaim している最中かもしれない）。拒否されたら dispatcher は fenced になり、新しい invoke を 503 で断り、`/readyz` も 503 にする |
| reclaim | 同じ周期で `ReconcileService::reclaim`。他の dispatcher のうち、lease の期限 + `max_clock_skew_ms` を自分の時計で過ぎたもの、graceful shutdown で stopped になったもの、**同じ host・同じ instance の前の incarnation でプロセスが無いと証明できたもの**（pid が存在しない、または同じプロセス内で handle が drop 済み）を `reclaimed` にし、その Lease を release（`reclaimed = 1`）、Invocation を `OutcomeUnknown{Host.LeaseExpired}`（dispatch 前なら `Failed{platform_error}`。stopped / 前の incarnation は `Host.Restarted`）、Attempt を同じ分類、環境を **fence**（`Draining`、epoch + 1、`fenced_at`）する。各行の CAS（`reclaimed_at IS NULL`、`released = 0`）により、複数のプロセスが同時に reclaim しても成立するのは 1 回だけ |
| fencing | 完了通知は `SlotStore::complete`（上記 §3-10）。reclaim で Lease が release され環境の epoch が進んでいるので、遅れて届いた結果は store が拒否し、台帳の `OutcomeUnknown` は上書きされない。reclaim された dispatcher は heartbeat も acquire もできない |
| 終了確認 | **lease の失効だけでは環境を空きにしない。** fenced の環境は pool にも容量にも数えられず、`terminate_environment` が成功した後の `SlotStore::confirm_terminated`（同じ epoch の CAS）でだけ `Lost` に落ちる。terminate が失敗した環境は fenced のまま次の周期で再試行する |
| 冪等性 | `Idempotency-Key` の結び付け `(tenant, function, key)` は `idempotency` 表の主キーで、どのプロセスからも 1 回しか書けない。入力 digest と、Invocation が terminal になった時点で `finished_at + idempotency_retention_seconds` の期限を持つ（実行中は失効しない）。失効した key は応答せず、同じ key の新しい request は新規として受け付け、期限切れの行は起動時と 10 分ごとに削除する |
| 同期 invoke の再実行 | しない。dispatch 後の失敗は `Failed` / `OutcomeUnknown` のまま（`docs/threat-model.md` §9）。例外は従来どおり「warm への `Invoke` が届かなかった」場合の cold 1 回だけ。handler の外部副作用は at-least-once（client の再送）か不明であり、exactly-once は保証しない |

時計: 期限の判定は判定する側の `Clock`（本番は wall clock）で行い、`max_clock_skew_ms` までのずれは許す。これを超えて時計がずれた、または heartbeat がプロセスの停止（GC、SIGSTOP、過負荷）で lease_ttl + skew より長く止まった場合、生きている dispatcher の仕事が reclaim されうる。そのときも fencing により結果は上書きされず、環境は terminate されてから `Lost` になる（handler の途中で止められうる）。

### 設定配信と認可 lease（PLT-4636）

決定と比較は `docs/adr/0007-config-distribution-and-auth-leases.md`。control plane（`role = "combined"`）と data plane（`role = "data_plane"`）を分け、invoke は data plane の cache だけを読む。`combined` の gateway は自分の publication を同じプロセスで読むので、1 プロセス構成でも invoke は同じ経路を通る。

```
management gateway (combined)                          data-plane gateway (data_plane)
  state.db: functions / aliases / revisions ─┐           ConfigCache (entry ごとに generation, valid_until)
  [[identity.tokens]], allowed_egress ────────┤ publish    ▲  refresh: refresh_interval_ms、失敗時は backoff
                                              ▼            │
  config_publication（generation を 1 txn で刻印）── GET /v1/internal/config?since=<gen> (Bearer internal_token)
                                                           │
                                                 invoke: authenticate(grant) → resolve(function, route,
                                                 revision, policy) → [cold start なら permit_cold_start]
```

| 仕組み | 内容 |
|---|---|
| 配信 | key（function / route / revision / grant / tenant / policy）ごとの entry と tombstone。grant の key は bearer token の HMAC-SHA256（鍵は `internal_token`）で、token そのものも secret の値も配信しない。revision は artifact digest・limits・egress・secret binding の参照を含む |
| generation | `config_publication` 表と `store_meta.config_generation`（migration 004）。publish は ledger の読みと刻印を 1 トランザクションで行い、内容が変わった key だけに次の generation を付け、消えた key を tombstone にする。値の版（alias generation など）が保存済みより小さい観測は無視。counter は `state.db` にあるので再起動をまたいで単調 |
| 有効期限 | entry の `valid_until` = 最後に確認した refresh の開始 + TTL（function / route / revision / policy は `config_ttl_seconds`、grant / tenant は `auth_lease_seconds`。control plane が配信で示す値と小さい方）。成功した refresh はすべての entry を確認し直す。状態は `unknown` / `fresh` / `stale_but_valid` / `expired` |
| 順序 | entry は保持より大きい generation でしか置き換えない。cache より小さい generation の配信は丸ごと無視し、何も延命しない |
| request 経路 | data plane は control plane を待たない。`combined` だけは自プロセスの設定書き込み（signal）か同じ `state.db` への他プロセスの commit（`PRAGMA data_version`）の後、または期限が近いときに答える前に refresh する |
| 既存実行と新規起動 | 実行中の invocation と warm 環境の利用は止めない。新しい invocation は有効な grant と設定が要る。cold start はさらに、起動時点で revision と tenant の認可が有効、provider の preflight が失敗していない、control plane 到達不能なら `allow_cold_start = true` を要する。`/readyz` の `control_plane` が `existing_executions` / `new_invocations` / `new_cold_starts` / `refusal` / `reason` と cache の状態を返す |
| 管理 API | data plane では 503 `control_plane_unavailable`（`Host.ControlPlaneUnavailable`）。store が応答しない（`RepoError::Store` / `Io`）ときも 503（`Host.StoreUnavailable`） |
| dispatcher | 再接続（失敗の後の成功）で dispatcher lease を即座に更新し直す。停止中に reclaim されていれば fenced のまま新しい仕事を拒否する |
| cell | data plane は control plane と同じ `data_dir` を使う（台帳・artifact store は cell で共有、判断に使う設定だけを配信で受け取る） |

revoke の遅延は、control plane に届く data plane で 1 refresh、届かない data plane で最大 `auth_lease_seconds`。

### admission・autoscaler（PLT-4634）

gateway 全体の semaphore を、資源で予約する admission に置き換えた（`crates/application/src/services/admission/`、決定と理由は `docs/adr/0006-autoscaling-and-admission.md`）。

- **予約（capacity ledger）**: 1 環境 = revision の `resources` + `[capacity.node]` の per-environment overhead（VMM + bridge + host 側の成果物）。状態は `Starting`（起動を許可した時点）→ `Busy`（Ready）→ `Parking`（pool が quiesce 中）→ `Idle` → `Draining`（terminate 中）→ 解放。warm の約束（`Promised`）は in-flight に数えるが資源は持たない（idle 環境がすでに持つ）。予約は `Grant` という値で、driver → pool → 次の driver と所有者が移り、最後の所有者が drop した時点で解放されるので、二重加算も取り残しも起きない。terminate に失敗した pool の環境は、再試行が成功するまで予約を持ち続ける。
- **node の上限**: `[capacity.node]` の cpu / memory / ephemeral storage（省略した次元は上限なし）と、`[capacity] max_concurrency`（Starting + Busy + Promised）。1 環境が node に一度も収まらなければ即時 429 `capacity`。
- **公平 queue**: tenant ごとの sub-queue。in-flight / weight の小さい tenant から、同値は最後の割り当てが古い順。node 全体の制約で止まった待機者より後ろには cold start を許さない。件数 `max_queue`、payload bytes `max_queue_bytes`、tenant の持ち分 `max_queue`、各待機者の `queue_deadline` で有界。admission は preemptive ではないので、1 tenant が先に全枠を取るのを防ぐのは tenant quota（`tenant_defaults.max_concurrency` を node の `max_concurrency` 未満にする）。
- **autoscaler**: revision ごとの `desired = ceil((max(到着率 × 平均時間, in_flight) + 待機数) / concurrency_per_environment)` を `[min_ready, min(max_concurrency, tenant quota)]` に clamp。`Starting + Busy + Idle + Parking < desired` のときだけ cold start する（N 件の burst は N 件起動しない）。起動は待機中の invocation の分だけで、例外は `min_ready` の先行起動（次節「スケール to zero・min_ready・drain」）。縮小は scale reconciler の idle sweep / drain と、資源不足時の idle eviction（`EnvironmentPool::evict_idle`）。
- **start rate / breaker**: node 全体の token bucket（`[capacity.start_rate]`）。revision ごとの起動失敗 breaker（`[capacity.circuit_breaker]`、create / handshake / init の失敗が連続 K 回で open、cooldown 後に probe 1 件）。open 中は 503 `circuit_open`。
- **配置**: `[capacity.node] region` と、tenant（`[[capacity.tenants]] required_region`）・revision（`required_region`、`RevisionSpec.placement`）の要求が一致しなければ、負荷に関係なく 503 `placement`。緩めない。
- **host と環境の区別**: `GET /v1/capacity`（`tsls capacity`）は node（`hosts = 1`、`host_scale_out = "not_supported"`、容量、overhead）と、予約・状態別の環境数・queue・start rate・拒否数・呼び出し元 tenant の revision を分けて返す。host の追加はこの prototype の範囲外。
- 状態はプロセスのメモリだけにある（再起動で到着率・breaker は消える。環境は起動時 reconcile が片付ける）。同じ `data_dir` を共有する 2 つ目の gateway は自分の予約しか数えないので、node の上限は 1 host 1 gateway の前提でだけ守られる。

### スケール to zero・min_ready・drain（PLT-4635）

決定と理由は `docs/adr/0009-scale-to-zero-and-drain.md`。実装は `crates/application/src/services/scaling.rs`（`ScaleController`）と admission の `try_scale_down` / `try_prestart` / `begin_drain`。

- **scale policy**（revision の `execution`）: `min_ready`（既定 0）、`idle_ttl_seconds`（既定 `[pool] idle_ttl_seconds`）、`scale_down_cooldown_seconds`（既定 `[scaling] scale_down_cooldown_seconds`）。環境数の上限は `max_concurrency`。
- **reconciler**: gateway が `[scaling] reconcile_interval_ms` ごとに `Application::reconcile_scaling` を呼ぶ（reuse の有無に関係なく常に動く）。1 回の処理は (1) 有効な設定 cache から route と削除済み function を読み、route から外れた revision（alias 切替）と削除された function の revision の drain を始める（cache が期限切れなら観測を保持して何もしない）、(2) drain timeout を過ぎた in-flight を止める（`Host.DrainTimeout`）、(3) pool の idle sweep、(4) `min_ready` の先行起動、(5) 削除の確定（`drained_at`、`combined` の gateway だけ）。
- **idle sweep**: pool は `Idle` の行ごとに予約へ `try_scale_down` を問い、admission が 1 つの lock の下で「待機者なし・約束済みでない・TTL 経過・cooldown 経過・route されていれば `min_ready` を割らない」（drain 中なら TTL / cooldown / `min_ready` を見ない）を確認して予約を `Draining` にしてから、台帳の CAS（`take_idle_for_termination`）で行を取る。claim が先なら何もしない（claimer が予約を `Busy` に戻す）。`Busy` の環境は列挙されない。
- **zero**: `min_ready = 0` の revision の最後の idle 環境が消えると環境数 0。次の invoke は cold start。環境数 0 でも gateway・`state.db`・node は動いており、host 費用は 0 にならない。pool が無い構成では環境は invocation と一緒に終わる。
- **min_ready**: route されていて drain 中でない revision で、provisioned（`Starting + Busy + Parking + Idle`）が `min_ready` 未満なら、待機者がいない場合に限り、全 cap の内側で admission の予約（`Starting`）を取って起動し、`Ready`（epoch 0）のまま pool に渡す。台帳は一度も割り当てられていない `Ready` だけを `Idle` にする。pool の per-key 上限は `min_ready` まで引き上げ、`max_total_idle` に達していれば起動しない。失敗は `prestart_backoff_seconds` 待つ。
- **drain**: alias 切替・関数削除の revision と、secret の値の変化で古くなった reuse key（invocation / 先行起動が計算した最新の key と違うもの）の環境は、pool に戻さず（`EnvironmentPool::release_for` が拒否）、idle は次の sweep で TTL に関係なく終える（待機者と約束は守る）。関数削除では待機中の invocation を `Host.FunctionDeleted` で終え、実行中は完了を待ち、何も残らなければ `drained_at` を記録する（`deletion_state`: `live` → `deleting` → `deleted`）。
- **観測**: `GET /v1/capacity` の revision ごとの `min_ready` / `idle_ttl_seconds` / `scale_down_cooldown_seconds` / `route_state` / `last_scale_event` と node 全体の `scaling`（`docs/api.md` §5.1.1・§8）。時系列の metrics は次節。

### metrics と負荷シナリオ（PLT-4637）

決定は `docs/adr/0011-reuse-and-scaling-metrics.md`、catalog・認証・detector・シナリオは `docs/metrics.md`。

- **`GET /metrics`**: `[metrics] bearer_token` の operator credential だけ（未設定なら 404、tenant の token は 401）。`Application::render_metrics` が 1 回の scrape で (1) `AdmissionController::metrics_view`（admission の lock 1 回で node・revision・tenant の状態・予約・queue・待ち時間・breaker と `AdmissionCounters`）、(2) pool の保持数と再利用の可否、(3) `Metrics`（attempt の start kind・phase histogram、boot identity、gate 拒否、heartbeat）、(4) この dispatcher の live 環境ごとの `ExecutionProvider::environment_stats`（idle CPU は前回の scrape との差分）、(5) 設定 cache の状態、(6) 非同期 outbox の backlog を読み、`metrics::render::render`（純関数）で exposition にする。tenant / revision / environment の label は `[metrics] max_*_series` で上限を持ち、超えた分は `_other`。
- **counter の置き場所**: admission の状態から導けない event だけを発生箇所で数える。admission 内の counter は revision entry（0 で忘れる）ではなく状態機械の直下に置く。
- **boot identity**: attempt の終了時に環境の `guest_boot_id` をその環境で最初に見た値と比べる（`same_boot` が再利用の証跡、`boot_changed` は不変条件の違反）。`GET /v1/capacity` の `reuse` は node 全体の mode（`warm_reuse` / `every_invocation_boots`）と件数だけで、tenant をまたぐ情報を含まない。
- **provider**: `environment_stats` は読み取り専用で既定 `None`。Firecracker は VMM の cgroup v2、process は bridge プロセスの procfs / `proc_pid_rusage`（子プロセスを含まない）。
- **検出と回帰**: 同じ条件を `deploy/prometheus/alerts.yml` と `apps/load/src/detect.rs` に置く。`scripts/load/scenarios.sh` が throwaway gateway を起動し、`tsls-load sample / load / report` で上限付きの負荷・250 ms ごとの sample・`summary.json` / `timeline.svg` を `docs/evidence/load-*` に残す。

### 環境 pool と再利用キー（PLT-4632）

invoke 後の環境を破棄せず `Idle` で残し、次の invoke に渡す仕組み（`crates/application/src/services/pool.rs`）。**2 つの gate が両方開いたときだけ**働く。

1. **capability**: provider が `idle_quiesce` と `idle_resume` の両方を `Supported` と報告すること。`Unverified`（コードはあるが実機で測っていない）では足りない。process は `Unsupported` のまま、firecracker は実機計測を経て `Supported`（`docs/evidence/warm-20260916T162532Z/`、`docs/adr/0001` §5）なので、firecracker で pool が働くかどうかは 2 つめの gate（`[pool] enabled`、既定 off）だけで決まる。
2. **設定**: `[pool] enabled = true`。既定は `false`。

どちらかが閉じていれば `EnvironmentPool` は「再利用しない」としか答えず、invoke pipeline は P1 と同じ destroy-after-invoke になる。理由は起動ログと `PoolPolicy::disabled_reason()` / `PoolPolicy::reason()` に出る。

### snapshot / clone（X1 実験、PLT-4653）

設計は `docs/adr/0017-snapshot-manifest-and-clone.md`、protocol は `docs/protocol.md` §A-X1。既定では無効（revision の `restore.policy = disabled`、`[snapshots]` 無効、bridge / Firecracker provider の feature `experimental-restore` が off）で、invoke の経路は変わらない。

- **層の分担**: domain（`snapshot.rs`）が manifest・署名・互換検査を純関数で持つ。application の `snapshot::SnapshotService` が作成（source を hold して provider に capture させ、封印して署名）と restore 計画（候補の状態・署名・互換・平文 digest の検査と restore 回数の予約）を行い、`services/invoke/restore.rs` が計画を clone・restore handshake・ready に繋ぐ。provider は bytes を動かすだけで、load してよいかは判断しない。
- **invoke への入り方**: `Driver::prepare_cold` の先頭で policy が `disabled` でないときだけ `try_restore` を呼ぶ。結果は restored の `Prepared`（以降は cold と同じ dispatch 経路）、`prefer` の cold（理由を evidence に付けて通常の cold へ）、`require` の失敗（`Host.RestoreRequiredUnavailable`）のいずれか。clone は admission の grant・台帳の環境行・usage の `EnvironmentStarted` を cold と同じように持つ。
- **環境**: clone は自分の環境 id・jail・cgroup・vsock listener・scratch copy を持ち、terminate / reconcile は通常の環境と同じ。平文 snapshot は `<workdir>/_snapshots/`（`list_environments` は環境 id でない directory を無視する）、封印した保管は `<data_dir>/snapshots/`。

### idle 休止・再開と計測 gate（PLT-4633）

gate が開いているとき、pool は環境の**休止と再開そのもの**も持つ。

- **pool に入るとき（`release`）**: 台帳の行を `Idle` にする**前**に `ExecutionProvider::idle_quiesce` を呼ぶ。行が `Idle` になった瞬間から claim できてしまい、claim 側は必ず resume するので、公開時点で休止済みでなければならないからである。
  - 休止そのものは **client の応答経路では行わない**。`release` は環境を pool に引き渡して即座に戻り、pause（hypervisor API と その timeout）は pool 自身の task で走る。warm 再利用は待ち時間を減らすためのものなので、次の invoke のための仕事を今の呼び出し元に払わせない。
  - 引き渡しても不変条件は変わらない。(1) **休止が完了するまで行は `Busy` のまま**で、`claim_for_reuse` は `Idle` しか配らないので誰も掴めない。(2) 休止できなかった環境・台帳に拒否された環境は **pool が terminate して計測する**（呼び出し側はもう居ない）。したがって `release` の `Ok` は「pool が所有を引き受けた」であって「pool に入った」ではない。
  - 引き渡し中の環境は pool の `settle()` が待つ。graceful shutdown の drain は sweep の前に必ずこれを待つので、休止中の環境が取り残されることはない。
- **pool から出すとき（`claim`）**: `idle_resume` → **readiness 検査**の順で行う。休止中の guest は自分について何も答えられないので、検査は再開の後でなければならない。**再開が確認できなかった環境には決して dispatch しない**: 既存の「guest が死んでいた」経路と同じく retire（`Draining` → terminate → `Failed`、計測は 1 回）し、cold start に落ちる。
  - readiness 検査は 2 段。`BridgeSession::drain_stale` が**すでに buffer に載っている** frame（前の attempt の残り、`Exited`）を消費し、続いて `BridgeSession::probe_ready` が `Ping` を送って `Pong` を待つ（上限 `READINESS_PROBE_TIMEOUT` = 500 ms、`docs/protocol.md` §A）。前者は host が既に知っていることしか分からないので、休止中に死んだ guest・応答しなくなった guest は後者でしか区別できない。答えが来なければ retire して cold start に落ちる（失敗の代償は cold start 1 回、見逃した場合の代償は execution deadline まで hang する invocation 1 回）。
  - `Pong` を返すのは bridge の frame loop であって user process ではない。つまりこの検査が示すのは「guest が scheduling されていて bridge が読んでいる」ことである。user process が死んでいる場合は `Exited` が queue に載るので `drain_stale` が拾う。
- **timing**: warm start は boot も init もしないので `environment_boot_ms` / `runtime_init_ms` は 0 で正しい。代わりに実際に掛かった `resume_ms` と `readiness_ms`（drain と probe の往復を含む）を `AttemptTimings` に記録し、API（`attempts[].timings`）と CLI に出す。0 で埋めて「warm は無料」に見せることはしない。値はミリ秒に**切り上げ**る（起きた仕事を 0 と報告しないため）。
- **休止した環境の終わらせ方**: pool が terminate するものは（sweep・drain・retire・拒否されたもの、いずれも）休止済みである。vCPU が止まっている guest は `Shutdown` frame を読めず自分で電源も切れないので、**frame は送らず** `TerminateReason::Quiesced` で終わらせる。この reason は provider に「猶予を待つな」と伝えるもので（`TerminateReason::waits_for_the_guest()`）、これが無いと sweep と drain のたびに provider の grace 分だけ止まる。再開が成功した後で使えないと分かった環境（readiness 検査に落ちたもの）は動いているので、従来どおり frame を送ってから terminate する。
- **guest に渡す deadline**: `Invoke` は host 時計の絶対 deadline と**残り時間 `remaining_ms`** の両方を運び、bridge は後者から `guest の現在時刻 + remaining_ms` として user process 向けの deadline を作る。休止していた guest の時計は止まっているため、絶対時刻では休止時間のぶんだけ余裕があるように見えてしまう。強制は従来どおり host 側が行う（`docs/protocol.md` §A・§B）。
- Firecracker 側の実装は `PATCH /vm {"state": "Paused"|"Resumed"}`（`docs/protocol.md` §C）。冪等で、VMM プロセスが死んでいる / API socket が無い / 環境が無い場合はそれぞれ別の error になる。「すでにその状態」を成功として扱うのは**要求した状態と一致するときだけ**で、`Resumed` を要求して「paused」と返された場合や解釈できない fault は失敗（= cold start）にする。

**計測 gate（`allow_unverified_idle`）。** capability を `Supported` にするには実機の計測が要るが、計測するには再利用が動いていなければならない。この鶏と卵を解くのが `[pool] allow_unverified_idle`（既定 `false`）で、これを立てたときだけ `Unverified` が capability gate を通る。`Unsupported`（コードが無い）は通らない。**`profile = "production"` ではこの switch を拒否する**（dev 専用 provider と同じ扱いで、設定検証で起動しない）。未計測の休止・再開コードを本番 traffic に当てることこそ capability gate が防いでいるものであり、起動に成功した gateway の warn は誰も読まないからである。計測は `profile = "dev"` で取る（`scripts/kvm/measure-warm.sh`、`docs/kvm.md` §3.7）。

この switch は「未検証の構成を検証済みにする」ものでは**ない**。したがって:

- `GET /v1/provider` は `reuse` を返す（`enabled` / `verified` / `reason` / `idle_quiesce` / `idle_resume`）。`enabled` は「この gateway が再利用するか」、`verified` は「**provider** が両方の idle capability を `supported` と申告しているか（＝実機で計測済みか）」で、2 つは独立である（再利用が off でも provider が計測済みなら `verified = true`）。switch で動いている間は `enabled = true` かつ **`verified = false`** で、`reason` は「計測のための実行であって検証済みの warm 構成ではない」と述べる。
- 起動ログは `environment_reuse` / `reuse_verified` / `reuse_reason` を必ず出し、switch で動いている場合はさらに `warn` を 1 行出す。
- 計測は `scripts/kvm/measure-warm.sh`（`docs/kvm.md` §3.7）で取り、証跡は `docs/evidence/warm-<UTC>/` に残す。`Unverified` → `Supported` への昇格は、その証跡を引用した別の変更である（`docs/adr/0001` §「決定」5）。firecracker はこの手順で `docs/evidence/warm-20260916T162532Z/` を取り、昇格済みである。

**再利用キー**（`ReuseKey`、RFC §5.3）は 8 field の複合キーで、**全 field が一致した環境だけ**が再利用される。1 field でも違えば別環境になる。

| field | 由来 |
|---|---|
| `tenant_id` / `revision_id` | 境界そのもの。またがない |
| `execution_role_version` | プロトタイプでは 1 固定（版管理された実行 role がまだ無い） |
| `configuration_version` | artifact digest・非 secret env var・`ExecutionPolicy`・binding 定義の digest |
| `resource_profile_digest` | `ResourceProfile` の digest |
| `runtime_profile` | guest が話す runtime protocol |
| `network_policy_version` | `EgressProfile` 由来 |
| `secret_binding_generation` | **解決後の** `(env 名, binding ref, 値)` の digest。値が rotate されれば generation が変わり、旧世代で起動した環境は再利用されない |

`secret_binding_generation` のために secret は環境を作る前に解決する。解決できない binding は台帳に行を作らず・何も起動せずに `502 init_error` で終わる。値は `HelloAck` 以外のどこにも出ない（`docs/threat-model.md` §6-4）。台帳（`state.json`）に載るのは「解決後の値から導いた digest」であり、**プロセスごとのランダム salt** を混ぜ、各部分を長さ prefix 付きで連結してから取る。したがって state file を読めても推測した secret と突き合わせられないし、generation はプロセス内でしか比較できない（pool はプロセス内のものなので、それで足りる）。

**状態と原子性**。pool の membership は台帳側（`EnvironmentRepository`）が持つ。

- `claim_for_reuse(key)`: reuse key 完全一致かつ **`Idle`**（＝ pool membership そのもの）の環境を 1 つだけ `Busy` にし、**epoch を 1 進める**。探索・状態遷移・epoch 加算を 1 回の store mutation で行うので、同時に 2 つの claim が走っても勝者は 1 つ（`repository.rs` の `concurrent_claims_never_hand_the_same_environment_to_two_callers`）。`Busy` は決して配られず、`Ready` 前の環境（`Requested` / `Provisioning` / `Initializing`）にも dispatch しない。`Ready` も配らない: それは cold start が今まさに dispatch しようとしている自分の環境であって、pool の持ち物ではない。
- `release_to_pool`: attempt が健全に終わった環境だけを `Idle` に戻す。`max_idle_per_revision` / `max_total_idle` を超える分と、epoch がずれた古い複製は拒否され、呼び出し側が今までどおり terminate する。「健全」は **attempt の結果**でも判定する: 成功と `UserError`（handler が返したエラー。guest は生きている）だけが対象で、`Crash` / `InitError` / `Timeout` / `PlatformError` / `OutcomeUnknown`、cancel、shutdown 中はいずれも戻さない。戻す前に、直前の attempt が残した frame を必ず drain する（`BridgeSession::drain_stale`）: 残った `Log` は**前の** invocation に付け、`Response` / `Error` は stale として捨て、`Exited` や EOF を見たら session を使用不可にして pool 入りを拒否する。`Exited` は attempt id も epoch も持たないので、lease では fence できない。
- pool は session と台帳の行を**同時に**公開する（`release` は session map の lock を握ったまま `release_to_pool` を呼ぶ）。行だけ見えて session が無い瞬間は存在しないので、claim 側が健全な環境を「死んでいる」と誤認して terminate することはない。
- epoch が進むことで、前の attempt が遅れて送ってきた frame は `ExecutionLease::accepts(attempt_id, epoch)` に一致せず捨てられる（`docs/threat-model.md` T05）。再利用が入って初めてこの fencing が効く。
- 取り出した warm 環境に `Invoke` frame を**渡せなかった**場合（idle の間に guest が死んでいた等）は、handler が始まっていないことが確定しているので、その環境を retire して **cold で 1 回だけ**やり直す。やり直しは cold 固定なので再帰しない。失敗した 1 行目の attempt の分類は cold と同じ規則で決める（`docs/threat-model.md` §9）: 書き込み失敗の後、guest が閉じる前に送った frame を短時間読み、`Exited` があれば `Crash` / `Runtime.Exited`、無ければ `Crash` / `Host.BridgeDisconnectedBeforeInvoke`。同じ guest の挙動が「warm だったから」別の分類になることはなく、warm 固有なのは**やり直すこと**だけである（やり直す理由は §9）。台帳には attempt が 2 行残り、usage には死んだ環境の `EnvironmentStopped` が 1 回だけ出る。

**回収**。idle TTL（revision の `idle_ttl_seconds`、既定 `[pool] idle_ttl_seconds`）を過ぎた環境は、scale reconciler（`[scaling] reconcile_interval_ms` ごと）の sweep が admission の判定（待機者・約束・cooldown・`min_ready`。前節「スケール to zero・min_ready・drain」）を通ったものだけ terminate する。graceful shutdown では TTL に関係なく全部落とす（pool の session はプロセスと運命を共にするため、跨いで生き残らせない）。terminate に成功した環境だけが terminal になり、そのとき pool が `UsageEvent{EnvironmentStopped}`（id は `<env>:<epoch>:pool-stopped`）を出す。terminate が失敗した環境は `Draining` のまま残し（`list_active` に残るので起動時 reconcile から見えるし、pool からは配られない）、次の sweep で再試行する。claim した環境が使えずに **retire** する場合（session が無い、guest が死んでいた）も同じ経路を通る: 先に `Draining` にしてから terminate し、成功したら `Failed` にして計測、失敗したら `Draining` のまま再試行を queue して**まだ計測しない**。再起動後は台帳上の非 terminal な環境がすべて `Lost` になり、host に残った実体は起動時 reconcile が orphan として回収する（前節）。

**計測**。1 つの環境が生涯に出す `EnvironmentStopped` はちょうど 1 回で、それは誰が終わらせたか（driver / sweeper / drain / retire / 失敗した terminate の再試行）に依らない。`monotonic_duration_ms` も 1 種類だけ:「台帳の `created_at` から終了時刻まで」の host 観測の生存時間である（`crates/application/src/services/pool.rs::environment_lifetime_ms`）。再利用される環境は個々の attempt より長く生きるので、attempt の stopwatch では測れない。`UsageEvent.sequence` は**環境ごとに単調**で、warm 再利用でも続き番号になる（pool が session と一緒に carry し、driver はその続きから採番する）。したがって 1 環境の event は `sequence` で並べられ、`event_id` = `<env>:<epoch>:<sequence>` も衝突しない。

**範囲**。この pool は 1 プロセス内だけのものである。guest への open な stream（`BridgeSession`）は永続化できないため in-process に留まり、複数プロセス間の原子性（pool membership、slot、Lease 期限）は本 store では依然として表現できない。方針は `docs/adr/0003-execution-state-persistence.md`。

Invocation の attempt には `StartKind`（`cold` / `warm` / `restored`）が記録され、API 応答の `attempts[].start_kind` に出る。

### durable queue と object store（PLT-4638）

非同期 invoke（PLT-4639 以降）のための配送と本文の置き場。PLT-4639 の `invokeAsync` が受付と outbox の配送に使う（次節）。 `[queue]` / `[objects]` の既定は `none` で、既定の gateway は何も接続しない。決定と実測は `docs/adr/0008-durable-queue-and-object-store.md`。

| 項目 | 内容 |
|---|---|
| port | `crates/durable-port`: `EventQueue`（publish（message id で dedup）、pull consumer の fetch、ack / nak / term、`max_deliver`、`ack_wait`、backlog stats）と `ObjectStore`（put / get / head / delete / usage / GC 候補の列挙） |
| 責任分界 | queue は at-least-once の**配送**だけ。invocation の受付・終了・冪等性の**決定**は台帳（`state.db`）。worker は台帳の CAS の後に ACK する。queue の dedup は publisher 再送よけで、kill -9 の後は削除済み message の id を忘れる（実測） |
| queue（本番候補） | NATS JetStream（`crates/adapters/queue-nats`）。起動時に stream を作成・更新: file storage、work-queue retention、`discard: new`（満杯なら publish を `queue_full` で拒否）、`max_msgs` / `max_bytes` / `max_msg_size` / `max_age` / `duplicate_window`、`num_replicas = 1`。subject は `<prefix>.<tenant>.<topic>`、consumer は durable pull・explicit ack。gateway は `[queue] backend = "nats"` なら bootstrap 前に接続し、認証に失敗したら起動しない |
| queue（dev / CI） | `SqliteEventQueue`（`<data_dir>/queue.db`、0600、WAL、FULL sync）。同じ契約テストを通す。`profile = "production"` では拒否 |
| server（IaC） | `deploy/nats/`（版と sha256 の pin、server 設定、account / user の雛形）、`scripts/queue/up.sh` / `down.sh`（docker 不要、local process、loopback だけ、`sync_interval: always`、password は生成して 0600）、`scripts/queue/verify.sh`（認証拒否・kill -9 後の再配送・容量境界・`max_age`・契約テスト・object store） |
| object | `FsObjectStore`: `<root>/<region>/<tenant>/<obj_id>.{data,meta}`（dir 0700、file 0600）。AES-256-GCM（鍵は key file / env、metadata に key id）、平文 SHA-256 を読むたびに検証、AAD で id・tenant・region・digest に束縛。別 tenant / region の scope からは `NotFound`。`max_object_bytes` と tenant quota は明示エラー |
| retention / GC | 台帳の `object_refs` / `object_tombstones`（migration 005）。GC は期限切れと orphan（grace を過ぎて一度も参照されていない）を、**非 terminal の invocation が参照していない場合だけ** tombstone → 削除する。tombstone 後の attach は拒否されるので、put と invocation insert の競合で消えた object を指すことはない |
| 保存先と複製 | queue は nats-server の 1 host の local disk、object は gateway の host の `data_dir`。**複製なし、HA ではない、region 障害に耐えない**（region は置き場所の境界であって複製先ではない） |

### usage の計測と仮料金（PLT-4642）

決定は `docs/adr/0012-usage-ledger-and-rating.md`、API は `docs/api.md` §5.10.1。

```
driver / pool ──UsageEvent v2 (segments・outcome・resources・bytes、各量に measurement)──▶ JournalingUsageSink
    1. usage journal に同期 append（<data_dir>/usage/journal.db、BEGIN IMMEDIATE、synchronous=FULL、hash chain）
         上限超過 → 拒否して tenant ごとの unjournaled に数える
    2. in-memory の view（履歴 API の UsageSummary）
collector（gateway の loop、[usage] collect_interval_ms、shutdown 時にも 1 回）
    cursor の後ろを batch で読む → chain を検証 → ledger に 1 トランザクション（event_id 主キー、INSERT OR IGNORE）
    → commit の後に cursor を CAS で進め、回収済みの行を消す
GET /v1/usage ── ledger（token の tenant だけ）──▶ rating（価格表 vN、AttemptSettled だけに価格を掛ける）
```

| 項目 | 内容 |
|---|---|
| 区間 | `queue_wait_ms`（受付→grant）、`vm_base_boot_ms`（create→bridge 接続。warm は resume + readiness）、`user_init_ms`（接続→Ready。warm は 0）、`handler_ms`（Invoke 送信→結果 / timeout / cancel。届かなかった attempt は 0）、`teardown_ms`（結果→terminate 完了、失敗なら unknown）、`idle_pooled_ms`。すべて host の `Instant` を ms に切り上げ |
| measurement | `host_measured` / `provider_reported`（terminate 直前の `ExecutionProvider::environment_stats`。`scope = cgroup_v2` だけ。process provider の bridge だけの sample は `unknown`）/ `guest_reported`（`Ready.init_ms`・`Response.handler_ms`、rating は読まない）/ `unknown`（0 として扱い `unmetered` に出す） |
| event | `AttemptSettled`（attempt ごと、課金対象、`attempt_kind = first / retry`、`outcome`）と `EnvironmentStopped`（環境ごとに 1 回、原価。dispatch 前に終わった環境にも出す）。`EnvironmentStarted` / `HandlerStarted` / `HandlerFinished` は監査用で合算しない |
| fail closed | 残りが headroom 以下・journal が開けない / 書けないなら、新規 invoke と `min_ready` の先行起動を拒否（503 `usage_journal_full`、`reason = usage_journal_full / usage_journal_unavailable`）。受付済みの実行は止めない。`/readyz` も 503 |
| 重複・再起動 | ledger は `event_id` で重複を捨てる。collector は ledger commit の後にだけ cursor を進めるので、crash は再配送になり二重計上にならない |
| 時計 | 量は monotonic。wall clock は日付への振り分けと `wall_clock_skew_ms` の記録だけ。環境寿命（原価）は台帳の wall clock の差で参考値 |
| 境界 | 利用量（`usage`）・仮料金（`provisional_charges_micros`、価格表 version 付き）・原価（`cost`）・不明（`unmetered` / `unjournaled_events`）・guest 申告を別の欄に出す。表は `function_usage_*` で、tachyon-apps の build 課金とは別 pipeline。`[usage.billing] enabled = true` は設定エラー |
| 範囲外 | 実請求・決済、stream（JetStream）経由の配送、regional ledger、Firecracker 実機での cgroup 値の確認。予算上限は次節（PLT-4643） |

### 予算の予約・上限・fail closed（PLT-4643）

決定は `docs/adr/0016-budget-reservation-and-admission.md`、API は `docs/api.md` §5.10.2。

```
control plane: [budget] (inline / file を publication ごとに再読込) ──ConfigKey::Budget{tenant} (auth lease, generation)──▶ ConfigCache
invoke: 設定 cache → usage journal → static admission (precheck) → BudgetService::reserve → queue / capacity (admit)
          reserve: 最大料金 = f(timeout・初期化 timeout・handshake・cancel grace、要求 vCPU / memory、課金区間、
                   request × 2 + max response bytes、invocation 1) を <data_dir>/usage/budget.db に
                   BEGIN IMMEDIATE で「totals を読む → 上限検査 → 行と totals を書く」(CAS)
          admit が拒否 / idempotency の競合に負け → release
driver:   queue で待った run は grant 時に recheck → run の終わりに finish(AttemptSettled を出した attempt id、journal head)
collector の後 (Application::collect_usage): settle_ready
          finish 済み: ledger に attempt が揃う or journal cursor ≥ head → rating で置き換え（不足分は unmetered hold）
          未 finish で run deadline + grace を過ぎた: expire（最大額を unmetered hold）
```

| 項目 | 内容 |
|---|---|
| 状態 | `reserved → settled \| released \| expired`。遷移は `WHERE state = 'reserved'` で冪等、totals は同じトランザクションで動き `CHECK (>= 0)` |
| 判定 | 停止は確約（reserved + settled + unmetered hold）と `hard_limit_micros`、alert は消費（settled + hold）と `soft_limit_micros × alert_thresholds_percent`。別の設定 |
| 拒否 | 429 `budget_exhausted`（`Host.BudgetExhausted`）、503 `budget_unavailable`（`Host.BudgetUnknown` / `Host.BudgetStoreUnavailable`）。`reason` は常に `budget`。quota / capacity の拒否とは別 |
| fail closed | 予算の未配信・tombstone・auth lease 切れ・価格表の不一致、finish 済み run の精算の遅れ（`max_unsettled_age_seconds`）、store 停止 → 新規を拒否。開始済みの run は止めない |
| 設定変更 | 上げ下げは以後の受付に効く。確約より下げても実行中は継続、queue の run は grant 時に拒否 |
| 金額の範囲 | PLT-4642 の仮料金（課金区間・要求資源・転送・invocation）だけ。原価・複数 region・決済は対象外。overrun は全額精算し数える |
| 非同期 | PLT-4640 の `run_async` も同じ precheck → reserve → admit を通り、driver に `BudgetRun` を渡す（run id `<invocation>:run-<attempt_base>:<ulid>`）。予算による拒否は dispatcher が attempt を数えずに defer する。`invokeAsync` の受付（202）は予算を見ない |

### 非同期 invoke と outbox（PLT-4639）

決定は `docs/adr/0010-invoke-async-and-outbox.md`、API は `docs/api.md` §5.6.1。

```
client ─POST :invokeAsync─▶ AsyncInvokeService::accept
    1. 設定 cache で解決（revision をここで固定）
    2. JSON・size・digest
    3. Idempotency-Key が結び付いていれば 202（replayed）/ 409
    4. outbox の上限と queue の状態で早期拒否（429 backlog / queue_full、503 queue_unavailable）
    5. 大きな入力は ObjectStore::put（digest を照合）
    6. state.db の 1 トランザクション:
         invocations(mode=async, accepted) + idempotency + object_refs + invocation_inputs + outbox
         （key と outbox の上限を同じ write lock の下で再確認）
    7. COMMIT の後に 202 ──▶ client
                                   │ wake
OutboxPublisher::run_once ◀────────┘（gateway の loop、catch_unwind）
    claim（sent=0・due・claim なし/期限切れ → claimed_by = dispatcher id, claim_ttl）
    → EventQueue::publish(message id = invocation id, payload = envelope)
    → ACK → mark sent（CAS on claim）+ invocation accepted → queued
    → 失敗 → claim を外して backoff（queue 停止なら batch の残りもまとめて外す）
```

| 項目 | 内容 |
|---|---|
| 有効になる条件 | `[queue]` が `none` 以外、かつ台帳が `state.db`。揮発の台帳では 503 `not_configured`。`[objects]` は任意（無ければ inline 上限を超える入力は 413） |
| 台帳 | migration 006: `invocation_inputs`（inline 本文か `(object_id, region)`、size、digest）と `outbox`（event id = invocation id、envelope、`sent`、`publish_attempts`、`next_attempt_at`、`claimed_by` / `claim_expires_at`、`last_error`、`queue_sequence`） |
| envelope | `InvokeEnvelope` v1: invocation / tenant / function / revision の id、event kind、input の digest・size・置き場、`accepted_at`、`queue_deadline`、trace id。**入力本文は載せない**。配送された event は `read_delivery` が台帳と照合し、入力を台帳の tenant scope で読み、digest を検証する |
| 再起動 | 非同期 invocation は dispatcher を持たず（`dispatcher_id = None`）、restart reconcile の対象外。outbox の未送信行は次の pass が送る。claim を持ったまま落ちた publisher の行は `claim_ttl_seconds` 後に取り直す |
| 重複 | ACK 後・mark 前に落ちると再 publish。broker の duplicate window 内なら 1 通、外なら同じ message id の 2 通目が保存される（at-least-once）。consumer は台帳の invocation id で決着する |
| 複数 gateway | 同じ `state.db` の publisher のうち 1 つだけが行を claim する（`BEGIN IMMEDIATE` と CAS）。outbox の上限もトランザクション内で判定するので、gateway の数だけ上限を越えることはない。queue の状態（満杯・停止）はプロセスローカル |

### 非同期 dispatcher・retry・DLQ（PLT-4640）

決定は `docs/adr/0013-async-dispatch-retry-dlq.md`、API は `docs/api.md` §5.6.2・§5.6.3。

```
EventQueue ─fetch(1)─▶ AsyncDispatcher::handle（worker × [async_dispatch] workers）
    1. envelope を decode・台帳と照合 ── 合わない ─▶ dead letter(poison) → term
    2. invocation が terminal ─▶ ACK（重複 / ACK 喪失の再配送。実行しない）
    3. generation が古い・生きた claim がある ─▶ ACK / 期限前 ─▶ NAK(残り)
    4. 期限・回数・削除・固定 Revision・retry budget ─▶ dead letter / 先送り
    5. claim_dispatch（async_dispatch 行の CAS、attempts + 1、claim 期限）
    6. 入力を台帳 / object store から読み digest を検証
    7. InvokeService::run_async（同期 invoke と同じ driver。invocation は terminal にせず結果を返す）
         claim を TTL/3 ごとに延長
    8. settle_dispatch（1 トランザクション、claim で fence）:
         invocations(terminal | queued) + async_dispatch + dead_letters? + outbox(generation + 1)?
    9. COMMIT の後に ACK
OutboxPublisher ─ generation n の event を next_attempt_at に publish（message id = inv_….g<n>）
AsyncDispatcher::reap（reaper_interval_seconds）:
    claim 切れの running → 次の generation を予約 / attempts_exhausted
    期限切れ → expired、broker が失った event → 次の generation を publish
```

| 項目 | 内容 |
|---|---|
| 台帳 | migration 008: `async_dispatch`（state、generation、attempts、deferrals、claim、`next_attempt_at`、`last_error`）、`dead_letters`（理由、状態、試行の要約、入力の参照、`origin_key` unique）、`redrives`（誰が・いつ・なぜ・どの dead letter から・新しい invocation）、`outbox.generation` |
| 所有 | 非同期 invocation の `dispatcher_id` は `None` のまま（guard は変えない）。run の所有は `async_dispatch` の claim。reclaim / restart reconcile は非同期 invocation を terminal にせず、attempt だけを settle する |
| retry の予約 | NAK の delay ではなく、settle と同じトランザクションで outbox に次の generation の event（`created_at` = `next_attempt_at` = 予定時刻）。broker の crash で予定を失わない。古い generation の message は ACK して捨てる |
| 分類 | `retry::classify`: 数える retry / 数えない先送り / dead letter（non_retryable、function_deleted、revision_unavailable）/ cancel |
| 非同期の class | 同期と同じ admission を通る。並列は `workers`（既定 `max_concurrency` の半分）、容量待ちは `admission_wait_ms`（既定 2 s）で、空かなければ先送り |
| dead letter | invocation を terminal にするのと同じトランザクション。入力は複製せず、`open` の間は object GC が参照 object を残す |
| redrive | `invoke` + `redrive` role。新しい invocation（元の固定 Revision、明示したときだけ同じ function の別 Revision）・同じ入力の参照・outbox event・redrive 記録・`redriven` を 1 トランザクション |
| at-least-once | ACK 喪失は実行しない。run 中の停止は次の試行が再実行する（副作用は 2 回起きうる）。台帳は terminal の記録を 1 つに保つ。業務冪等キーの例は `examples/idempotent-async` |
| 停止 | graceful shutdown は worker を止め、実行中の run を `Host.GatewayShutdown`（数えない先送り）で settle してから終わる |
| failpoint | `crates/application/src/failpoints.rs`。unit test と feature `failpoints` の build だけで動き、`TSLS_FAILPOINTS`（例 `outbox.after_publish=kill`）で E2E が SIGKILL を起こす（`scripts/queue/async-e2e.sh`） |
| 範囲外 | queue からの取り出し・実行・retry・DLQ（PLT-4640）。`queued` の先には進まない |

### trigger（PLT-4641）

決定は `docs/adr/0014-cron-and-webhook-triggers.md`、API は `docs/api.md` §5.11。

```
TriggerService::run_scheduler_once（gateway の loop、scheduler_interval_ms と最早の next_fire_at）
    dispatcher が fenced → 何もしない
    trigger_scheduler の lease（owner = dispatcher id、TTL = dispatcher lease）を取る / 更新する
    due な cron trigger ごとに:
        [max(cursor, now - max_catchup), now] の予定時刻を trigger の zone で列挙
        （存在しない local 時刻は skip、2 回ある時刻は早い方 1 回）
        grace 内 = on time、それ以前 = late → missed-run policy で選ぶ
        各時刻 ─▶ fire（下）─▶ accepted / already fired → 次、Inactive → 中断、
                                一時的拒否 → cursor をその時刻に置いて中断、恒久的拒否 → refused を記録
        cursor（next_fire_at）を now の次へ CAS（generation・enabled）

POST /v1/hooks/{trg} ─▶ webhook trigger を引く（404）→ body 上限（413）→ timestamp（401）
    → 定数時間 HMAC（401）→ 有効か（410）→ event id（400）→ event id / 署名の dedup（202 replayed）

fire = AsyncInvokeService::accept_for_trigger（§「非同期 invoke と outbox」の受付と同じ）
    principal = trigger の tenant + invoke。ConfigCache::authorize_tenant、resolve（revision を固定）
    Idempotency-Key = cron:{trg}:{scheduled_at} / webhook:{trg}:{event_id}
    state.db の 1 トランザクション:
        trigger 行を読み直す（削除・無効・generation 違い → Inactive）
        trigger_fires の (trigger, fire_key) / 署名 digest → AlreadyFired
        accept_in（invocation・key・入力・outbox、backlog 上限）
        trigger_fires 行（主キーで一意）
```

| 項目 | 内容 |
|---|---|
| 有効になる条件 | `invokeAsync` が有効（`[queue]` + `state.db`）で `role = "combined"`。webhook は `[triggers] secret_key_file`（または `secret_key_env`）も要る |
| 台帳 | migration 008: `triggers`（spec の JSON、`status`、`generation`、`next_fire_at`、封じた `secret_sealed`）、`trigger_fires`（主キー `(trigger_id, fire_key)`、`(trigger_id, signature_digest)` の一意 index、`outcome`、`invocation_id`）、`trigger_scheduler`（lease 1 行） |
| 重複しない根拠 | fire 行と invocation が同じトランザクション。lease は無駄を減らすだけで、2 つの scheduler が同じ時刻を処理しても主キーで 1 つだけが commit する |
| disable / delete | trigger 行の generation を上げる。fire のトランザクションが trigger 行を読み直すので、それより後に commit する fire は無い。受付済みの invocation は outbox・dispatcher の通常の経路で続く |
| secret | 生成して 1 回だけ返し、AES-256-GCM（AAD = trigger id + tenant id）で `state.db` に封じる。削除で消す |
| 保持 | webhook の fire 行 `webhook_dedup_retention_seconds`（7 日）、cron の fire 行 `fire_retention_seconds`（30 日、`max_catchup_seconds` より長いことを設定検証で強制）。scheduler の pass が 1 時間ごとに消す |
| `[triggers]` の主な設定 | `scheduler_enabled`、`scheduler_interval_ms`（1000）、`scheduler_batch`（100）、`grace_seconds`（30）、`max_catchup_seconds`（86400）、`max_run_all`（100）、`max_triggers_per_function`（20）、`webhook_max_body_bytes`（262144）、`webhook_default_tolerance_seconds`（300）、`webhook_max_tolerance_seconds`（3600）、`webhook_dedup_retention_seconds`（604800）、`fire_retention_seconds`（2592000）、`secret_key_file` / `secret_key_env` |
| 範囲外 | trigger 固有の retry・DLQ・concurrency policy（前の実行中は skip 等）、source 別の webhook 検証 |

## 5. 決め事（実装者が守ること）

1. domain / application は `firecracker` `kube` `axum` を import しない。provider は `ExecutionProvider` だけを実装する。
2. Secret 値は `SecretValue`（Debug は redacted）で運び、HelloAck の env にだけ載せる。ログ・API 応答・台帳（`state.db`）に書かない。
3. guest の自己申告（handler_ms, Ready 時刻）は参考値。課金・timeout・権限の根拠は host 側計測。
4. 結果は `attempt_id` と `epoch` が Lease と一致するものだけ受理する。
5. terminate は冪等。terminate 後に process / socket / tap / drive / workdir が残らない（`TerminateReport.cleaned` に列挙）。
6. process provider は `TACHYON_UNISOLATED=1` を guest env に載せ、`Capabilities.dev_only = true`。
7. すべての ID は domain の型を使い、文字列で持ち回らない。
8. 失敗の分類は `ErrorClass` を正とし、HTTP status は `api-types::ErrorCode::http_status()`。
9. ログは invocation 単位で `Limits` の行数・bytes 上限を守り、超過分は `dropped = true` で観測できるようにする。
10. 「動いた」証跡: 環境の `BootEvidence`（guest_boot_id, host_pid, provider details）を Invocation の attempt に残し、API と CLI で表示する。
11. 再起動で「開始したかもしれない」ものを `Failed` にしない。dispatch 済みは `OutcomeUnknown`、未 dispatch だけ `Failed`（§4「起動時の後始末」）。孤児環境の回収は listener を開ける前に済ませる。
12. 永続化は `<data_dir>/state.db`（埋め込み SQLite、§4「永続化」、`docs/adr/0003-execution-state-persistence.md`）。repository を経由しない読み書きをしない。更新は「読んだ値を条件にした CAS」で、負けたら読み直す（`AliasService::apply`）。新しい列や表は migration を追加して入れ、適用済みの migration を書き換えない。TiDB は将来の adapter で §6 のとおり非対象のまま。
13. invoke の経路（認証・function / route / revision / policy の解決・cold start の可否）は `ConfigCache` / `InvokeGate`（`crates/application/src/control/`）だけを読み、`FunctionRepository` / `AliasRepository` / `RevisionRepository` を直接読まない。設定の有効期限切れで実行中の invocation を止めない（§4「設定配信と認可 lease」、ADR-0007）。
14. queue の ACK を「実行した」「終わった」の根拠にしない。決定は台帳の CAS で行い、ACK はその commit の後に送る。object は必ず `ObjectScope`（tenant, region）と一緒に扱い、非 terminal の invocation が参照する object を消さない（§4「durable queue と object store」、ADR-0008）。
15. pool の環境を終わらせる経路（sweep・drain）は admission の `Grant::try_scale_down` を通し、その後で台帳の CAS を取る。待機者・約束・cooldown・`min_ready` を見ない terminate を足さない（例外は資源不足時の `evict_idle` と shutdown の drain）。環境を起動するのは待機中の invocation か `min_ready` の先行起動（`AdmissionController::try_prestart`）だけ（§4「スケール to zero・min_ready・drain」、ADR-0009）。
16. 実行を始める経路は `UsageMeter::admit` を通す（journal が満杯・停止なら何も記録せずに拒否）。usage の量は host の monotonic clock・gateway が数えた bytes・provider が host で読んだ値だけで作り、guest の申告は `guest_reported` に置いて rating に使わない。測れなかった量は `unknown` にして推測値を入れない。usage event は journal に同期で書いてから先へ進み、ledger は `event_id` で重複を捨てる。usage の表は `function_usage_*` で、build 課金と混ぜない（§4「usage の計測と仮料金」、ADR-0012）。

## 6. 非対象（P1）

snapshot/restore、非同期 invoke、cron、Console UI、TiDB 永続化、OCI image の pull。非同期 invoke のための durable queue と object store は PLT-4638 で検証環境として用意したが（§4「durable queue と object store」）、invoke からはまだ使わない。これらは Capability / API で明示的に Unsupported を返す。egress restricted / public-web は PLT-4622 で Firecracker provider に実装した（tap + nftables、`docs/adr/0005-egress-profiles.md`）。権限の無い host では Capability が理由付きの Unsupported になる。

snapshot/restore に向けた**実験**として、SDK に初期化保存点と復元後 hook の API がある（PLT-4651、X1、`docs/protocol.md` §B-X1）。`tachyon-serverless-sdk` の feature `experimental-restore`（既定 off）の `lifecycle::builder().bootstrap(..).after_restore(..).run(..)` で、同期 bootstrap（Tokio・secret・接続なし）→ checkpoint → continue（`cold` / `restored`）→ after_restore（identity・RNG・時計・認証・接続）→ ready の順に進む。bridge は `continue` に答えるまで Ready を host に送らない。snapshot を取る provider は無いので bridge は常に `cold` と答え、通常起動も同じ経路を通る。host↔bridge frame は変えていない。これは任意のライブラリや multithread runtime を snapshot-safe にするものではない。例は `examples/restore-aware`。P0〜P4 はこれに依存しない。

warm 再利用と idle 休止・再開は実装済み（§4「環境 pool と再利用キー」「idle 休止・再開と計測 gate」）だが、provider が `idle_quiesce` / `idle_resume` を `Supported` と報告しない限り働かない。process は `Unsupported`、firecracker は実機計測を経た `Supported` を返す。`[pool]` の既定が off なので **既定の構成では P1 と同じ destroy-after-invoke** であり、それを `crates/application/tests/pipeline.rs` が検査する。firecracker で再利用が働くのは `[pool] enabled = true` にしたときで、`Unverified` の provider を測るための `[pool] allow_unverified_idle` は別の switch として残る（その構成は API とログで一貫して「未検証」と表示される）。
