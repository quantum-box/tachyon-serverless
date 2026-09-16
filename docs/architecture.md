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
- P1 は 1 環境 1 同時実行、destroy-after-invoke。環境 pool（warm 再利用）は §4「環境 pool と再利用キー」の 2 重 gate の背後にあり、既定では働かない。process は `idle_quiesce` / `idle_resume` を `Unsupported`、firecracker は PLT-4633 で実装済みだが実機未計測のため `Unverified` と報告するので、**既定の設定では同梱のどちらの provider でも destroy-after-invoke のまま**である。snapshot は未対応（Capability に `Unsupported` と明示）。

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
  3 payload 上限 (limits.max_payload_bytes → 413)、trace id ≤ 256 bytes、Idempotency-Key 1..=256 文字 (→ 400)。
     ここまで何も記録しない (拒否された request は key を消費しない)。Idempotency-Key が既存 Invocation に
     結び付いていれば、容量に関係なくそれを返す (同キー・異なる input digest → 409 conflict)。
  4 Invocation(Accepted) + deadlines を組み立てる。queue / init / execution は client_deadline を超えない。
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
  8 Attempt(Dispatched, epoch) + Lease を作成。Invocation(Running)。Invoke frame 送信。
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
 10 Lease release、Attempt/Invocation terminal 更新、UsageEvent(host 観測)、
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

[reconcile]
on_startup = true              # 起動時に provider の孤児環境を回収する（既定 true）

[pool]
enabled = false                # 環境再利用（warm）。既定 off
max_idle_per_revision = 1      # reuse key ごとに idle で残す環境数
idle_ttl_seconds = 60          # これを超えて idle な環境は sweeper が破棄する
max_total_idle = 8             # 全 reuse key 合計の idle 上限
allow_unverified_idle = false  # 計測専用。未計測（Unverified）の idle capability を受け入れる。既定 off

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

前のプロセスが crash / kill で落ちた場合、台帳（`state.json`）も host の資源も中途半端に残る。gateway は次の順で収束させる。

1. **台帳**（`crates/application/src/repository.rs::reconcile_after_restart`、store の読み込み時）。`Running` だった Invocation は `Invoke` frame を書き終えており handler が走った可能性があるため `OutcomeUnknown{Host.Restarted}`（自動再実行しない。`docs/threat-model.md` §9）。`Accepted` / `Queued` のままだったものは一度も dispatch していないので `Failed{platform_error, Host.Restarted}`。Attempt は所属する Invocation に従い、Environment は `Lost`。terminal なものは触らない。
2. **host の資源**（`crates/application/src/services/reconcile.rs::ReconcileService`、`serve()` が listener を accept させる前に呼ぶ）。`ExecutionProvider::list_environments` を呼び、この gateway が active として知らない環境を `terminate_environment(Reconcile)` で回収する（process / socket / drive / workdir。冪等）。台帳には active なのに provider が知らない環境は `Lost` にする。
3. **観測**。結果（found / adopted / terminated / failed / lost）を構造化ログ `startup reconcile finished` に出し、`GET /readyz` の `reconcile` にも載せる。

規則: provider の列挙や terminate が失敗しても起動は止めない（warn を出して続行し、`reconcile.error` に残す）。実行中の invocation の環境は `create_environment` より前に台帳へ記録されるため必ず「知っている」側に入り、reconcile が terminate することはない。`[reconcile] on_startup = false` で 2 と 3 だけを止められる（1 は常に走る）。

### 環境 pool と再利用キー（PLT-4632）

invoke 後の環境を破棄せず `Idle` で残し、次の invoke に渡す仕組み（`crates/application/src/services/pool.rs`）。**2 つの gate が両方開いたときだけ**働く。

1. **capability**: provider が `idle_quiesce` と `idle_resume` の両方を `Supported` と報告すること。`Unverified`（コードはあるが実機で測っていない）では足りない。process は `Unsupported`、firecracker は `Unverified` なので、既定では同梱のどちらでも何も pool されない（`docs/adr/0001` §5）。
2. **設定**: `[pool] enabled = true`。既定は `false`。

どちらかが閉じていれば `EnvironmentPool` は「再利用しない」としか答えず、invoke pipeline は P1 と同じ destroy-after-invoke になる。理由は起動ログと `PoolPolicy::disabled_reason()` / `PoolPolicy::reason()` に出る。

### idle 休止・再開と計測 gate（PLT-4633）

gate が開いているとき、pool は環境の**休止と再開そのもの**も持つ。

- **pool に入るとき（`release`）**: 台帳の行を `Idle` にする**前**に `ExecutionProvider::idle_quiesce` を呼ぶ。行が `Idle` になった瞬間から claim できてしまい、claim 側は必ず resume するので、公開時点で休止済みでなければならないからである。休止に失敗した環境は **pool に入れない**。session を呼び出し側に返し、呼び出し側は今までどおり terminate する（＝ P1 と同じ destroy-after-invoke）。
- **pool から出すとき（`claim`）**: `idle_resume` → **readiness 検査**（`BridgeSession::drain_stale` と使用可否の確認）の順で行う。休止中の guest は自分について何も答えられないので、検査は再開の後でなければならない。**再開が確認できなかった環境には決して dispatch しない**: 既存の「guest が死んでいた」経路と同じく retire（`Draining` → terminate → `Failed`、計測は 1 回）し、cold start に落ちる。
- **timing**: warm start は boot も init もしないので `environment_boot_ms` / `runtime_init_ms` は 0 で正しい。代わりに実際に掛かった `resume_ms` と `readiness_ms` を `AttemptTimings` に記録し、API（`attempts[].timings`）と CLI に出す。0 で埋めて「warm は無料」に見せることはしない。値はミリ秒に**切り上げ**る（起きた仕事を 0 と報告しないため）。
- Firecracker 側の実装は `PATCH /vm {"state": "Paused"|"Resumed"}`（`docs/protocol.md` §C）。冪等で、VMM プロセスが死んでいる / API socket が無い / 環境が無い場合はそれぞれ別の error になる。

**計測 gate（`allow_unverified_idle`）。** capability を `Supported` にするには実機の計測が要るが、計測するには再利用が動いていなければならない。この鶏と卵を解くのが `[pool] allow_unverified_idle`（既定 `false`）で、これを立てたときだけ `Unverified` が capability gate を通る。`Unsupported`（コードが無い）は通らない。

この switch は「未検証の構成を検証済みにする」ものでは**ない**。したがって:

- `GET /v1/provider` は `reuse` を返す（`enabled` / `verified` / `reason` / `idle_quiesce` / `idle_resume`）。switch で動いている間は `enabled = true` かつ **`verified = false`**、`reason` は「計測のための実行であって検証済みの warm 構成ではない」と述べる。
- 起動ログは `environment_reuse` / `reuse_verified` / `reuse_reason` を必ず出し、switch で動いている場合はさらに `warn` を 1 行出す。
- 計測は `scripts/kvm/measure-warm.sh`（`docs/kvm.md` §3.7）で取り、証跡は `docs/evidence/warm-<UTC>/` に残す。`Unverified` → `Supported` への昇格は、その証跡を引用した別の変更である（`docs/adr/0001` §「決定」5）。

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
2. Secret 値は `SecretValue`（Debug は redacted）で運び、HelloAck の env にだけ載せる。ログ・API 応答・state.json に書かない。
3. guest の自己申告（handler_ms, Ready 時刻）は参考値。課金・timeout・権限の根拠は host 側計測。
4. 結果は `attempt_id` と `epoch` が Lease と一致するものだけ受理する。
5. terminate は冪等。terminate 後に process / socket / tap / drive / workdir が残らない（`TerminateReport.cleaned` に列挙）。
6. process provider は `TACHYON_UNISOLATED=1` を guest env に載せ、`Capabilities.dev_only = true`。
7. すべての ID は domain の型を使い、文字列で持ち回らない。
8. 失敗の分類は `ErrorClass` を正とし、HTTP status は `api-types::ErrorCode::http_status()`。
9. ログは invocation 単位で `Limits` の行数・bytes 上限を守り、超過分は `dropped = true` で観測できるようにする。
10. 「動いた」証跡: 環境の `BootEvidence`（guest_boot_id, host_pid, provider details）を Invocation の attempt に残し、API と CLI で表示する。
11. 再起動で「開始したかもしれない」ものを `Failed` にしない。dispatch 済みは `OutcomeUnknown`、未 dispatch だけ `Failed`（§4「起動時の後始末」）。孤児環境の回収は listener を開ける前に済ませる。
12. 永続化は P1 では in-memory + `state.json` の write-through（`crates/application/src/repository.rs`）で、調整（coordination）には使っていない。環境再利用・autoscaling・scale-to-zero が要求する複数プロセス間の原子性（slot 取得、Lease の期限、pool membership、reuse key 検索、Idempotency binding）は現在の store では表現できない。方針は `docs/adr/0003-execution-state-persistence.md`（control-plane と cell 局所状態を port で分け、プロトタイプは 1 file の埋め込み SQLite に載せる。TiDB は将来の adapter で §6 のとおり非対象のまま）。

## 6. 非対象（P1）

snapshot/restore、非同期 invoke、cron、Console UI、TiDB 永続化、egress restricted/public-web、OCI image の pull。これらは Capability / API で明示的に Unsupported を返す。

warm 再利用と idle 休止・再開は実装済み（§4「環境 pool と再利用キー」「idle 休止・再開と計測 gate」）だが、provider が `idle_quiesce` / `idle_resume` を `Supported` と報告しない限り働かない。process は `Unsupported`、firecracker は実機未計測の `Unverified` を返すため、**既定の構成では P1 と同じ destroy-after-invoke** であり、それを `crates/application/tests/pipeline.rs` が検査する。firecracker で再利用を動かせるのは計測用の `[pool] allow_unverified_idle` を明示的に立てたときだけで、その構成は API とログで一貫して「未検証」と表示される。
