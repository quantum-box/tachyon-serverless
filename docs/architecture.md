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
| `crates/api-types` | 管理/Invoke API の DTO, エラー code | application |
| `crates/application` | usecase, repository (in-memory), invoke pipeline, bridge session (host 側), watchdog, log 保持 | axum, hypervisor 固有 API |
| `crates/providers/process` | 子プロセス + unix socket。dev 専用 | — |
| `crates/providers/firecracker` | Firecracker API socket, vsock, drive, cleanup | domain 以外の上位 |
| `crates/providers/fake` | テスト専用。duplex stream 上のスクリプト guest | — |
| `crates/runtime-bridge` | guest 側 agent。vsock/unix で host と接続、Runtime API を HTTP で提供、user process を起動・監視 | — |
| `crates/sdk` | `run(handler)`, `serve_http(router)`, `Context`。実験 feature `experimental-restore` で `lifecycle`（PLT-4651） | bridge 内部 |
| `apps/gateway` | axum。管理 API + Invoke + logs + OpenAPI | — |
| `apps/cli` | `tsls` CLI。deploy / invoke / logs / rollback / dev | application（HTTP 経由のみ） |
| `examples/*` | hello / http-axum / cpu-burn / isolation-probe / restore-aware（実験、PLT-4651） | — |

## 3. 実行の流れ（同期 Invoke, P1）

```
client ─POST /v1/functions/{id}:invoke─▶ gateway
  1 認証: Bearer token → Principal{tenant, roles}. 他 tenant の資源は 404。
  2 Function 取得 (deleted → 409 function_deleted)。alias→Revision 解決 (Ready でなければ 409 revision_not_ready)。
     Revision は受付時に固定される。実行中に alias を変えても版は変わらない。
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
  5 容量: revision.max_concurrency と gateway 全体上限の semaphore。空きがなければ bounded queue
     (config max_queue) で queue_deadline まで待つ。溢れ → 429 capacity_exceeded (ledger にも key にも残らない)。
     queue_deadline 超過 → 504 queue_timeout。
     Invocation の保存と Idempotency-Key の結び付けは 1 回の store 更新で行う (同 key の並行 request は
     1 つだけが受け付けられ、残りは同じ Invocation を返す)。
  6 ExecutionEnvironment(Requested→Provisioning) を作成し、HelloAck(entrypoint, env(+secrets), limits) を組み立てる。
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
max_concurrency = 8            # gateway 全体
max_queue = 32
queue_timeout_seconds = 10

[store]
backend = "sqlite"             # sqlite（<data_dir>/state.db、既定）| memory（再起動で消える）
output_retention_seconds = 604800  # インライン出力の保持期限。過ぎたら digest に置き換える。0 は無期限
idempotency_retention_seconds = 86400  # Idempotency-Key の保持期限（Invocation が terminal になってから）。0 は無期限

[dispatcher]                   # PLT-4631。gateway プロセスごとの所有者 id と lease
# instance = "gateway-a"       # 既定 "gateway@<listen>"。同じ data_dir を共有する gateway 同士で重複させない
lease_ttl_seconds = 30         # dispatcher lease と slot lease の所有期限
heartbeat_interval_seconds = 10  # renew と reclaim の周期（lease_ttl_seconds 未満）
max_clock_skew_ms = 2000       # 他の dispatcher の期限を判定するときに許す時計のずれ

[reconcile]
on_startup = true              # 起動時に provider の孤児環境を回収する（既定 true）

[pool]
enabled = false                # 環境再利用（warm）。既定 off
max_idle_per_revision = 1      # reuse key ごとに idle で残す環境数
idle_ttl_seconds = 60          # これを超えて idle な環境は sweeper が破棄する
max_total_idle = 8             # 全 reuse key 合計の idle 上限
allow_unverified_idle = false  # 計測専用。未計測（Unverified）の idle capability を受け入れる。既定 off
                               # profile = "production" では拒否される

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

1. **台帳（所有者の無い行）**（`crates/application/src/repository/restart.rs` の規則を `SqliteStore::open` が 1 トランザクションで適用する）。対象は **owner を持たない行**（schema 2 以前の gateway が書いた行と `state.json` の import）だけ。`Running` だった Invocation は `Invoke` frame を書き終えており handler が走った可能性があるため `OutcomeUnknown{Host.Restarted}`（自動再実行しない。`docs/threat-model.md` §9）。`Accepted` / `Queued` のままだったものは一度も dispatch していないので `Failed{platform_error, Host.Restarted}`。Attempt は所属する Invocation に従い、Environment は `Lost`、未 release の Lease は release、Invocation の無い Idempotency key は削除。terminal なものは触らない。
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
| 本文 | 入力は digest とサイズだけ。出力は `[invoke] inline_output_max_bytes` 以下のときだけ本文を持ち（store 側でも `limits.max_response_bytes` で拒否）、`[store] output_retention_seconds` を過ぎると digest に置き換える（起動時と 10 分ごと）。secret 値は書かない。log は memory のまま |
| `state.json` からの移行 | `state.json` があり DB が空なら、起動時に 1 度だけ取り込み `state.json.imported-<UTC>` に rename する（削除しない）。行のある DB と `state.json` が同時にあれば両方の path を挙げて起動を拒否する。壊れた `state.json` は従来どおり拒否する。逆方向の変換は無い（戻すには rename された JSON を戻し、`state.db*` を退避する） |
| 権限 | `state.db` は新規作成時に mode `0600`。`-wal` / `-shm` も SQLite が同じ mode で作る |
| port の分割 | control-plane の 8 trait と、cell-local の `SlotStore`（`repository/slot.rs`: dispatcher、slot の acquire / complete / release、lease の renew / reclaim、fencing、pool membership）。両方とも同じ `state.db` に載る（ADR-0003 決定 1・2） |
| 対象外 | 複数 host、ネットワーク FS 上の `data_dir`、TiDB、backup / PITR、保存時暗号化。同じ `data_dir` を同じ host の複数 gateway が同時に開く構成は PLT-4631 で扱えるようになった（次節）。ただし `[dispatcher] instance` と `listen` は gateway ごとに変えること |

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

### 環境 pool と再利用キー（PLT-4632）

invoke 後の環境を破棄せず `Idle` で残し、次の invoke に渡す仕組み（`crates/application/src/services/pool.rs`）。**2 つの gate が両方開いたときだけ**働く。

1. **capability**: provider が `idle_quiesce` と `idle_resume` の両方を `Supported` と報告すること。`Unverified`（コードはあるが実機で測っていない）では足りない。process は `Unsupported` のまま、firecracker は実機計測を経て `Supported`（`docs/evidence/warm-20260916T162532Z/`、`docs/adr/0001` §5）なので、firecracker で pool が働くかどうかは 2 つめの gate（`[pool] enabled`、既定 off）だけで決まる。
2. **設定**: `[pool] enabled = true`。既定は `false`。

どちらかが閉じていれば `EnvironmentPool` は「再利用しない」としか答えず、invoke pipeline は P1 と同じ destroy-after-invoke になる。理由は起動ログと `PoolPolicy::disabled_reason()` / `PoolPolicy::reason()` に出る。

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

**回収**。`idle_ttl_seconds` を過ぎた環境は sweeper（gateway が `idle_ttl/2` 間隔で起動）が terminate する。graceful shutdown では TTL に関係なく全部落とす（pool の session はプロセスと運命を共にするため、跨いで生き残らせない）。terminate に成功した環境だけが terminal になり、そのとき pool が `UsageEvent{EnvironmentStopped}`（id は `<env>:<epoch>:pool-stopped`）を出す。terminate が失敗した環境は `Draining` のまま残し（`list_active` に残るので起動時 reconcile から見えるし、pool からは配られない）、次の sweep で再試行する。claim した環境が使えずに **retire** する場合（session が無い、guest が死んでいた）も同じ経路を通る: 先に `Draining` にしてから terminate し、成功したら `Failed` にして計測、失敗したら `Draining` のまま再試行を queue して**まだ計測しない**。再起動後は台帳上の非 terminal な環境がすべて `Lost` になり、host に残った実体は起動時 reconcile が orphan として回収する（前節）。

**計測**。1 つの環境が生涯に出す `EnvironmentStopped` はちょうど 1 回で、それは誰が終わらせたか（driver / sweeper / drain / retire / 失敗した terminate の再試行）に依らない。`monotonic_duration_ms` も 1 種類だけ:「台帳の `created_at` から終了時刻まで」の host 観測の生存時間である（`crates/application/src/services/pool.rs::environment_lifetime_ms`）。再利用される環境は個々の attempt より長く生きるので、attempt の stopwatch では測れない。`UsageEvent.sequence` は**環境ごとに単調**で、warm 再利用でも続き番号になる（pool が session と一緒に carry し、driver はその続きから採番する）。したがって 1 環境の event は `sequence` で並べられ、`event_id` = `<env>:<epoch>:<sequence>` も衝突しない。

**範囲**。この pool は 1 プロセス内だけのものである。guest への open な stream（`BridgeSession`）は永続化できないため in-process に留まり、複数プロセス間の原子性（pool membership、slot、Lease 期限）は本 store では依然として表現できない。方針は `docs/adr/0003-execution-state-persistence.md`。

Invocation の attempt には `StartKind`（`cold` / `warm` / `restored`）が記録され、API 応答の `attempts[].start_kind` に出る。

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

## 6. 非対象（P1）

snapshot/restore、非同期 invoke、cron、Console UI、TiDB 永続化、OCI image の pull。これらは Capability / API で明示的に Unsupported を返す。egress restricted / public-web は PLT-4622 で Firecracker provider に実装した（tap + nftables、`docs/adr/0005-egress-profiles.md`）。権限の無い host では Capability が理由付きの Unsupported になる。

snapshot/restore に向けた**実験**として、SDK に初期化保存点と復元後 hook の API がある（PLT-4651、X1、`docs/protocol.md` §B-X1）。`tachyon-serverless-sdk` の feature `experimental-restore`（既定 off）の `lifecycle::builder().bootstrap(..).after_restore(..).run(..)` で、同期 bootstrap（Tokio・secret・接続なし）→ checkpoint → continue（`cold` / `restored`）→ after_restore（identity・RNG・時計・認証・接続）→ ready の順に進む。bridge は `continue` に答えるまで Ready を host に送らない。snapshot を取る provider は無いので bridge は常に `cold` と答え、通常起動も同じ経路を通る。host↔bridge frame は変えていない。これは任意のライブラリや multithread runtime を snapshot-safe にするものではない。例は `examples/restore-aware`。P0〜P4 はこれに依存しない。

warm 再利用と idle 休止・再開は実装済み（§4「環境 pool と再利用キー」「idle 休止・再開と計測 gate」）だが、provider が `idle_quiesce` / `idle_resume` を `Supported` と報告しない限り働かない。process は `Unsupported`、firecracker は実機計測を経た `Supported` を返す。`[pool]` の既定が off なので **既定の構成では P1 と同じ destroy-after-invoke** であり、それを `crates/application/tests/pipeline.rs` が検査する。firecracker で再利用が働くのは `[pool] enabled = true` にしたときで、`Unverified` の provider を測るための `[pool] allow_unverified_idle` は別の switch として残る（その構成は API とログで一貫して「未検証」と表示される）。
