# 脅威モデルと API / 実行契約ノート（PLT-4614）

- 対象: Tachyon Serverless プロトタイプ P1（`docs/architecture.md`、`docs/protocol.md`）
- 正本: 失敗の分類は `crates/domain/src/invocation.rs::ErrorClass`、HTTP status は `crates/api-types/src/lib.rs::ErrorCode::http_status()`、上限は `crates/domain/src/limits.rs::Limits`、役割は `crates/provider-port/src/identity.rs::Role`。本書はそれらと矛盾しないように書き、矛盾があればコードを正とし本書を直す。
- 状態: プロトタイプの契約。SLA・料金・本番運用の約束ではない。

## 1. 目的と範囲

本書は次を固定する。

1. 信頼境界と、その境界を越えて何を信じるか（§3〜§6）。
2. 2 tenant + operator のアクセス制御マトリクスと期待する HTTP 結果（§7）。
3. deadline モデル、`OutcomeUnknown`、Idempotency-Key、payload 上限の意味（§8〜§11）。
4. 脅威と対策の対応（§12）、process provider が守らないもの（§13）、残存リスク（§14）、非目標（§15）。
5. 実装者が守る実行契約の要点（§16）。

範囲外: gateway の前段（TLS 終端、WAF、rate limit）、host OS の hardening、供給網（依存 crate の監査）。これらは deployment の責務として §5 の前提に置く。

## 2. 用語

| 用語 | 意味 | 正本 |
|---|---|---|
| tenant | `TenantId`（`tn_` + ULID）。本リポジトリは tenant を発行しない | `crates/domain/src/ids.rs` |
| Principal | 認証済み呼び出し元。`{subject, tenant_id, roles}` | `crates/provider-port/src/identity.rs` |
| Role | `Deploy`（function / revision / alias の作成・更新）、`Invoke`（invoke と invocation / logs の読み取り）、`Operator`（`/v1/provider` と、自 tenant の function / revision / alias の読み取りだけ。§7） | 同上 |
| environment | 1 回の invoke のために作られ、終了後に破棄される microVM / process | `crates/domain/src/environment.rs` |
| bridge | guest 側 PID 1 / 子プロセスとして user code を起動し、host と frame を交換する agent | `crates/runtime-bridge`、`docs/protocol.md` |
| Lease | `(attempt_id, epoch)` を environment に結び付ける fencing token | `ExecutionLease::accepts` |
| ErrorClass | `UserError` / `Crash` / `InitError` / `Timeout` / `QueueTimeout` / `PlatformError` / `Cancelled` / `OutcomeUnknown` | `crates/domain/src/invocation.rs` |

## 3. 信頼境界

```
┌────────────┐  B1   ┌──────────────────────────────────────────┐
│  client    │──────▶│ gateway                                   │
│ (tenant)   │◀──────│  control plane: 管理 API, artifact store, │
└────────────┘       │                 repositories, state.db    │
                     │  data plane:    invoke pipeline,          │
                     │                 bridge session, watchdog  │
                     └───────────────┬──────────────────────────┘
                                     │ B2 (ExecutionProvider trait)
                     ┌───────────────▼──────────────────────────┐
                     │ provider host                             │
                     │  firecracker process / process provider   │
                     │  vsock uds, drives, workdir               │
                     └───────────────┬──────────────────────────┘
                                     │ B3 (vsock / unix socket, frames)
                     ┌───────────────▼──────────────────────────┐
                     │ guest bridge (PID 1 in microVM)           │
                     │  Runtime API http://127.0.0.1:<port>      │
                     └───────────────┬──────────────────────────┘
                                     │ B4 (loopback HTTP, Runtime API)
                     ┌───────────────▼──────────────────────────┐
                     │ user code (artifact, tenant が供給)       │
                     └──────────────────────────────────────────┘
```

| 境界 | 越えるもの | 信じるもの | 信じないもの |
|---|---|---|---|
| B1 client ↔ gateway | HTTP、`Authorization: Bearer`、任意の `X-Tachyon-Tenant-Id`、payload | 管理 API は `IdentityProvider::authenticate` が返した `Principal`、invoke 系は有効期限内に配信された grant から作った `Principal`（PLT-4636、B5） | header の tenant 主張（token と一致しなければ 403）、client が申告する timeout（`x-tachyon-client-timeout-ms` は上限を縮める方向にしか効かない）、client の接続維持（切断は完了を意味しない） |
| B2 gateway ↔ provider host | `EnvironmentSpec`、`EnvironmentHandle`、`TerminateReport`、`EnvironmentObservation` | provider の観測（host pid、exit code、cleaned の列挙） | provider が「作った」と言うだけの環境（`BootEvidence` と bridge の `Hello` が揃って初めて Initializing にする） |
| B3 provider host ↔ guest bridge | 長さ付き JSON frame（≤ 8 MiB） | `Hello.environment_id` が spec と一致すること、`(attempt_id, epoch)` が Lease と一致する `Response` / `Error` | `Hello.guest_boot_id` / `Ready.init_ms` / `Response.handler_ms` / `Heartbeat.ts_ms` / `Log.ts_ms`（参考値）、frame の到着順が実行順であること |
| B4 guest bridge ↔ user code | Runtime API（long-poll、response / error / ready / init error） | 何も信じない。bridge は `attempt_id` を配ったものだけ受理し、1 環境 1 in-flight を守る | user code が bridge を乗っ取れないこと（同一 guest 内なので前提にしない。§6-1） |
| B5 control plane ↔ data plane（PLT-4636） | `GET /v1/internal/config`（bearer = `internal_token`）、generation 付きの function / route / revision / grant / tenant / policy と tombstone | 内部 credential を持つ相手からの配信で、cache より大きい generation の entry | cache より小さい generation の配信（丸ごと無視し、何も延命しない）、同じか小さい generation の entry、期限（`valid_until`）を過ぎた entry。配信に token そのものと secret の値は含まれない |
| B6 gateway ↔ durable queue / object store（PLT-4638） | NATS client protocol（loopback、平文）上の JetStream API と ACK、`<data_dir>/queue.db`、`<data_dir>/objects` の file | server が認証した接続（user + password / nkey）、publish の ACK（file store に fsync 済み）、復号と SHA-256 が一致した object の bytes、台帳の `object_refs` | queue の ACK と dedup（配送の事実であって実行・完了・冪等性の根拠ではない）、delivery の `delivery_count` と redelivery の有無、object の metadata の `expires_at`（改竄されうる。GC の判断は台帳の参照と合わせて行う）、file の置き場所（scope は metadata と AAD で再確認する） |

B3 と B4 の間には権限境界が無い。user code が guest 内で権限昇格すれば bridge も乗っ取られる。したがって **guest から来る情報はすべて「tenant のコード」が言ったものとして扱う**。これが §6 の原則の根拠である。

## 4. 資産

| 資産 | 所在 | 損なわれ方 |
|---|---|---|
| A1 artifact（tenant の実行バイナリ） | `data_dir/artifacts`（digest 名） | 他 tenant による取得・実行、改竄 |
| A2 secret 値 | `config/gateway.{dev,firecracker}.toml`（P1）、`HelloAck.env`（転送中）、guest プロセス環境 | ログ・API 応答・台帳（`state.db`）・`BootEvidence.details` への混入、他 environment への配送 |
| A3 invocation の入出力 | request body、`Invocation.output`（inline ≤ 上限 / digest）、guest メモリ | 他 tenant による読み取り、改竄、上限を超える蓄積 |
| A4 ログ | `LogRecord`（invocation 単位、上限付き） | 他 tenant による読み取り、洪水による retention 破壊、secret の混入 |
| A5 ledger（Function / Revision / Alias / Invocation / Attempt / Environment / Lease / UsageEvent） | `<data_dir>/state.db`（埋め込み SQLite。UsageEvent と log は memory） | 偽の結果による上書き、guest 申告に基づく計測 |
| A6 API token | `config/gateway.{dev,firecracker}.toml` | 漏洩、他 tenant への流用 |
| A7 host 資源（KVM、CPU、memory、disk、fd） | provider host | orphan 環境、無制限の同時実行、暴走 handler |
| A8 起動の証跡（`BootEvidence`、`AttemptTimings`） | Attempt | guest 申告での偽装、process provider の結果を microVM の結果と誤認 |
| A9 queue の message（非同期 event、PLT-4638） | nats-server の JetStream store（1 host の local disk）/ `<data_dir>/queue.db` | 認証なしの publish / 読み取り、満杯時の黙った削除、再起動での消失、他 tenant の event として配送 |
| A10 大きな入出力の object（PLT-4638） | `<data_dir>/objects/<region>/<tenant>/`（AES-256-GCM） | 他 tenant / 他 region からの参照、改竄、未完了 invocation の入力の早すぎる削除、quota を超える蓄積 |
| A11 queue credential と object 鍵（PLT-4638） | `[queue.nats] password_file` / `nkey_seed_file`、`<state>/auth.conf`、`[objects] key_file` / `key_env` | 漏洩による queue の読み書き・object の復号、group / other に読める mode |

## 5. 前提（assumptions）

1. gateway と provider host は P1 では同一 Linux host で、gateway プロセスのユーザーが `/dev/kvm` と `data_dir` にアクセスできる。`config/gateway.firecracker.toml`（production）は jailer と host cgroup（`mode = "required"`）を有効にするので gateway は root で動き、VMM は jailer が uid 64000・chroot・新しい PID / mount namespace に降格させてから exec する（§14-2）。jailer を外した dev 構成では VMM は gateway と同じユーザーで走る。
2. B1 の transport 保護（TLS）は deployment の責務。P1 の既定 `listen = "127.0.0.1:8080"` は平文であり、ループバック外に露出しない前提。
3. `config/gateway.{dev,firecracker}.toml` は operator だけが読める。token・secret 値の保管はファイル権限に依存する。
4. `IdentityProvider` は token を `Principal` に解決する以上のことをしない。IAM / policy は将来の adapter（`docs/inventory-tachyon-apps.md` §3.7）。
5. KVM と Firecracker の隔離境界は upstream の設計を信頼する（`docs/adr/0001-execution-provider-firecracker-first.md`）。本リポジトリで hypervisor の脆弱性は扱わない。
6. 1 環境は 1 tenant の 1 revision にしか使われない（`ExecutionPolicy.concurrency_per_environment = 1`。`min_ready`（PLT-4635、既定 0）で先行起動した環境も 1 revision の reuse key に属する）。warm 再利用は `[pool] enabled` と provider の `idle_quiesce` / `idle_resume` = `Supported` が両方揃ったときだけ働き（`docs/architecture.md` §4「環境 pool と再利用キー」）、process は `Unsupported`、firecracker は実機計測を経た `Supported` を返すが、`[pool]` の既定が off なので既定ではどちらも destroy-after-invoke のまま。`Unverified` の provider を受け入れるのは計測用の `[pool] allow_unverified_idle` を明示的に立てたときだけで、その構成は API とログで「未検証」と表示される。再利用する場合も ReuseKey の 8 field 完全一致が条件で、tenant / revision / 設定 / secret generation をまたいで 1 環境が共有されることはない。
7. clock は host のもの（`Clock` trait）。guest の時刻は信頼しない。

## 6. 原則

1. **guest の自己申告は authz・metering・termination の根拠にしない。** `Ready.init_ms`、`Response.handler_ms`、`Heartbeat`、`Log.ts_ms`、`guest_boot_id` は表示・診断のための参考値。課金相当の事実は `UsageEvent{evidence_quality: HostObserved}` と `AttemptTimings`（host 計測）だけから作る。timeout の判定は host の watchdog が `execution_deadline` で行い、guest の申告や `Heartbeat` の有無で延長も短縮もしない。terminate は provider に対して host が発行し、guest の同意を要しない。
2. **他 tenant の資源は存在しない扱い（404）。** 403 で存在を漏らさない。`Function::ensure_owned_by` / `Invocation::ensure_owned_by` が `TenantMismatch` を返したら、gateway は `NotFound` に写像する。
3. **結果は Lease と一致するものだけ受理する。** `(attempt_id, epoch)` が一致しない `Response` / `Error` は捨てる。deadline 判定後に届いた結果も捨てる。session での判定に加え、台帳への書き込みも `SlotStore::complete` が「未 release の同じ Lease、同じ epoch の環境」を 1 トランザクションで確認してからしか行わない（PLT-4631）。別の dispatcher が reclaim した slot の結果は、届いた gateway の中で正しくても台帳に入らない。
8. **lease の失効は環境を空きにしない（PLT-4631）。** lease を失った dispatcher の環境は fence（`Draining`、epoch + 1）され、pool にも容量にも数えられず、provider の terminate が成功したことを確認してから `Lost` にする。生きている dispatcher の仕事は、その lease が期限 + 許容する時計のずれを過ぎるまで、別の gateway から settle も terminate もしない。
4. **secret 値は `HelloAck.env` にだけ載せる。** `SecretValue` の `Debug` は redact。ログ・API 応答・台帳（`state.db`）・`BootEvidence` に書かない。`HelloAck` frame そのものをログに出さない。
5. **Revision は受付時に固定され、以後変わらない。** `spec_digest` で不変性を検証する。alias は generation で CAS 更新する。
6. **終了は冪等で、後始末は列挙する。** `terminate_environment` は 2 回目に `was_running = false` を返し、`TerminateReport.cleaned` に消したものを列挙する。
7. **process provider の成功は microVM の成功ではない。** `Capabilities.dev_only = true` と `TACHYON_UNISOLATED=1` を必ず露出し、`profile = "production"` では拒否する（`docs/adr/0002-process-provider-dev-only.md`）。

## 7. アクセス制御マトリクス（2 tenant + operator）

前提: tenant A の principal は `roles = [deploy, invoke]`、tenant B も自 tenant に対して同じ、operator は `roles = [operator]` で A 以外の tenant に属する（token は必ず 1 つの tenant に紐付く）。対象資源はすべて **tenant A が所有**する。未認証は token なし / 無効 token。

| 資源・操作 | tenant A（所有者） | tenant B | operator | 未認証 |
|---|---|---|---|---|
| `POST /v1/functions`（作成） | 201（A の tenant に作成） | 201（**B の** tenant に作成。A の資源には触れない） | 403 `forbidden`（operator は読み取り専用） | 401 `unauthorized` |
| `GET /v1/functions`（一覧） | A の分だけ | B の分だけ（A の function は含まれない） | operator 自身の tenant の分だけ（A の function は含まれない） | 401 |
| `GET /v1/functions/{A}` | 200 | 404 `not_found` | 404 | 401 |
| `DELETE /v1/functions/{A}` | 200（以後 invoke は 409 `function_deleted`） | 404 | 403 | 401 |
| `POST /v1/functions/{A}/revisions` | 202（validation は非同期） | 404 | 403 | 401 |
| `GET /v1/functions/{A}/revisions[/{rev}]` | 200（spec には secret の `binding_ref` と `env_name` だけ。値は無い） | 404 | 404 | 401 |
| `PUT /v1/functions/{A}/aliases/{alias}` | 200 / 409 `conflict`（generation 不一致）/ 409 `revision_not_ready` | 404 | 403 | 401 |
| `GET /v1/functions/{A}/aliases[/{alias}]` | 200 | 404 | 404 | 401 |
| `POST /v1/functions/{A}:invoke`、`ANY /v1/functions/{A}/http/{*path}` | 200 または invocation の失敗 status（§8） | 404 | 403（operator は invoke できない） | 401 |
| `GET /v1/functions/{A}/invocations`、`GET /v1/invocations/{inv of A}` | 200 | 404 | 403（invocation 履歴は tenant データ。`invoke` role が必要） | 401 |
| `POST /v1/invocations/{inv of A}:cancel` | 200 | 404 | 403 | 401 |
| `GET /v1/invocations/{inv of A}/logs` | 200 | 404 | 403（ログ本文は tenant データ） | 401 |
| secret 値 | API 無し（読めない） | API 無し | API 無し | — |
| `POST /v1/artifacts`（upload） | 200（digest。A が所有者として記録される） | 200（B 自身の upload。同一 bytes なら同一 digest で、B も所有者として記録される。B が upload していない digest を revision で参照すると、存在しない digest と同じ理由で `failed`。§14-1） | 403 | 401 |
| artifact 本体の取得 | API 無し | API 無し | API 無し | — |
| `GET /v1/functions/{A}/usage` | 200 | 404 | 403 | 401 |
| `GET /v1/provider` | 200 | 200 | 200 | 401 |
| `GET /healthz`、`GET /readyz` | 200 / 503 | 同左 | 同左 | 200 / 503（認証不要） |
| `X-Tachyon-Tenant-Id` が token の tenant と不一致 | 403 `forbidden` | 403 | 403 | 401 |

注:

- B が A の ID（`fn_...`、`inv_...`）を推測しても 404。ID は秘密ではなく、authz の代わりにしない。
- operator は P1 では **自 tenant の function / revision / alias の metadata と `/v1/provider` だけ** を読める（`require_read` と tenant 一致）。他 tenant の資源は tenant B と同じく 404、invocation 履歴・usage・logs は role 不足で 403（`invoke` が必要。自 tenant でも 403）、mutation / invoke / cancel / upload は 403。全 tenant 横断の読み取り（`tenant_id` 付き一覧、`output` を省いた invocation 閲覧）は未実装で、実装するまで付与しない（fail-safe）。テスト: `apps/gateway/tests/gateway_integration.rs::operator_role_is_own_tenant_read_only`。
- role が足りない場合（例: `invoke` だけの principal が `POST /v1/functions`）は 403 `forbidden`。自 tenant の資源に対する権限不足は存在を隠す必要がないため 404 にしない。

## 8. deadline モデル

すべて絶対時刻（host clock）。受付時に固定し、guest には `Invoke.deadline_ms` と `tachyon-deadline-ms` header で「観測のため」渡す。guest はこれを短縮も延長もできない。

| deadline | 定義 | 超過時の分類 | HTTP | handler の副作用 |
|---|---|---|---|---|
| `queue_deadline` | `min(accepted_at + queue_timeout（config、既定 10 s）, client_deadline)` | `QueueTimeout` | 504 `queue_timeout` | 無し（環境未作成） |
| `init_deadline` | `min(環境作成開始 + initialization_timeout_seconds（revision、既定 30 s、上限 120 s）, client_deadline)`。`create_environment` の `connect_timeout` と handshake / Ready の待ちもこれで打ち切る | `InitError` | 502 `init_error` | handler は未実行。ただし user process の初期化コードは走った可能性がある |
| `execution_deadline` | `min(Invoke 送信時刻 + timeout_seconds（revision、既定 30 s、上限 15 min）, client_deadline)`。guest の `Invoke.deadline_ms` はこの値 | `Timeout` | 504 `timeout` | あり得る。再実行しない |
| `client_deadline` | `accepted_at + min(x-tachyon-client-timeout-ms, timeout + init + queue)` | 他の 3 つはこれを超えて設定されない（host が clamp する）。到達時はその時点の phase で分類する: Queued → `QueueTimeout`。環境作成〜Ready 待ち、または Ready 後で handler 未送信 → `Timeout`（`Host.ClientDeadline`。handler は起動せず、環境は stop + `terminate(Cancelled)`、Attempt は作らない）。Running → `Timeout`（`Host.ClientDeadline`。`Cancel` → `terminate(Timeout)`） | 同左 | phase による（handler 起動前なら無し） |
| 容量超過（queue が満杯） | `max_queue` / `max_queue_bytes` / tenant の `max_queue` 超え、または 1 環境が node に収まらない | — | 429 `capacity_exceeded`（`reason` = `queue_full` / `quota` / `capacity`） | 無し |
| 配置制約・起動 breaker（PLT-4634） | `required_region` と node の `region` の不一致、revision の breaker が open | — | 503 `provider_unavailable`（`reason` = `placement` / `circuit_open`） | 無し |

補足:

- `execution_deadline` 到達時の手順: `Cancel{grace_ms: 1000}` → grace 後 `provider.terminate_environment(Timeout)` → `Failed{Timeout}`。terminate は guest の応答を待たない。
- `POST :cancel` は同じ手順で `Cancelled`（invoke 応答は 499 `cancelled`、cancel 自体は 200）。
- client が切断しても invoke タスクは `execution_deadline` まで追跡し、結果を ledger に残す（`docs/architecture.md` §3-11）。切断は cancel ではない。
- timeout / cancel / init 失敗した環境は再利用しない（P1 はそもそも再利用しない）。
- Ready 後、Attempt / Lease / `Running` を記録する前に `client_deadline` を再確認し、過ぎていれば handler を起動しない。

## 9. `OutcomeUnknown` の意味

`OutcomeUnknown` は「handler が実行された可能性があるが、結果を host が確認できなかった」terminal 状態。`Invocation::mark_outcome_unknown` は `Running` からしか遷移できない（`crates/domain/src/invocation.rs`、test `outcome_unknown_only_after_running`）。

入る条件:

1. `Invoke` frame の書き込みが成功した後、`Response` / `Error` を受け取る前に bridge との stream が閉じた（EOF / IO error）。
2. `observe_environment` が `Exited` / `NotFound` を返し、結果 frame が無い。
3. gateway が再起動し、ledger に `Running` の invocation が残っている（`Invoke` frame は書き終えているので handler が走った可能性がある）。対象は owner の無い行（起動時 reconcile）と、stopped / 前の incarnation と証明された dispatcher の行（reclaim、`Host.Restarted`）。Attempt も同じ分類にする。
4. invocation を実行していた dispatcher の lease が期限 + 許容する時計のずれを過ぎ、別の dispatcher が reclaim した（`Host.LeaseExpired`、PLT-4631）。元の dispatcher がまだ生きていて後から結果を得ても、台帳は上書きされない（§6-3）。

入らない条件:

- `execution_deadline` 到達 → `Timeout`（host が判定済み）。
- bridge が `Error{kind: crash}` を送ってから閉じた → `Failed{Crash}`（結果は「crash」として確定）。
- frame の protocol 違反 → `Failed{PlatformError}`（`docs/architecture.md` §3-9 の分類に従う）。
- 再起動時に `Accepted` / `Queued` のまま残っていた → 一度も dispatch していない＝ handler は開始していないので `Failed{PlatformError}` / `Host.Restarted`（`crates/application/src/repository.rs::reconcile_after_restart`、`docs/architecture.md` §4）。
- `Invoke` frame が guest に届かなかった（handler は開始していない）→ `Failed`。encode できない（`FrameTooLarge`、何も書いていない）→ `PlatformError` / `Host.InvokeTooLarge`（環境は健全なので `Stopped`）。書き込み失敗（接続断）→ guest が閉じる前に送った frame を短時間読み、`Exited` なら `Crash` / `Runtime.Exited`、無ければ `Crash` / `Host.BridgeDisconnectedBeforeInvoke`。同じ guest の挙動が書き込みの競合で分類を変えないようにするため。
- **warm（pool から取り出した再利用環境）への書き込み失敗も同じ規則**。drain してから同じ 2 分類のどちらかを attempt に記録する。環境が再利用だったことは分類を変えない。warm だけが違うのは、その attempt を失敗として残したうえで **cold で 1 回だけ dispatch をやり直す**ことである（`docs/architecture.md` §4。やり直しは cold 固定なので再帰しない）。やり直す根拠は分類ではなく guest が死んだ場所にある: warm の guest は前の invocation で `Ready` を報告して実際に動いた後、idle の間に死んだので、新しい環境なら同じ request を実行できる見込みが高い。cold の guest は **この invocation のための初期化中**に死んでおり、やり直しても同じ失敗を繰り返すだけなので再実行しない。どちらの場合も `Invoke` は届いておらず handler は開始していないので、やり直しても at-most-once は破れない（`OutcomeUnknown` の「自動再実行しない」は handler が走ったかもしれない場合の話であり、ここには当たらない）。

契約:

- HTTP 502 `outcome_unknown`、`error_type = "Host.OutcomeUnknown"`（起動時 reconcile で確定したものは `Host.Restarted`、lease の失効で reclaim されたものは `Host.LeaseExpired`）。応答には `invocation_id` を含める。
- **自動再実行しない。** 再実行の判断は client の責務。client は「実行されたかもしれない」として自身の冪等性で扱う。
- 同じ Idempotency-Key での再送は `OutcomeUnknown` の記録を返し、再実行しない（§10）。
- 環境は必ず terminate する。後から届く `Response` は Lease 解放済みのため捨てる。
- `UsageEvent` は `HandlerStarted` まで host 観測で記録し、`HandlerFinished` は `evidence_quality = Unknown` で記録するか省く。

## 10. Idempotency-Key の意味

- header: `Idempotency-Key`（`crates/api-types::headers::IDEMPOTENCY_KEY`）。1..=256 文字（`Invocation::accept` が検証）。
- scope: `(tenant_id, function_id, key)`。tenant B が同じ key を送っても A の invocation には触れない（B の scope で新規作成）。
- 一致（同 key、同 `input_digest`）: 既存 Invocation を返し、**再実行しない**。terminal でなければ現在の状態（`accepted` / `queued` / `running`）を返し、client は `GET /v1/invocations/{id}` で追跡する。
- 不一致（同 key、異なる `input_digest`）: 409 `conflict`。本文の `invocation_id` は key が結び付いている invocation、`error_type` は `Host.IdempotencyKeyReused`（key は呼び出し元自身の tenant・function の scope なので、id を返しても他者の情報は漏れない）。
- 別の gateway（同じ `state.db` を開く別プロセス）が実行中の invocation に一致した場合、応答はその invocation が terminal になるか、その invocation の `client_deadline` まで台帳を追ってから返す。どちらの gateway でも 2 回目の実行はしない（PLT-4631、`crates/application/tests/leases.rs::a_key_replayed_on_another_gateway_returns_the_same_invocation_and_never_runs_twice`）。
- 一意性: `(tenant_id, function_id, key)` は `idempotency` 表の主キーで、結び付けは `BEGIN IMMEDIATE` の中で行う。複数プロセスが同時に同じ key を送っても結び付くのは 1 つだけ（`crates/application/src/repository/sqlite/tests.rs::{separate_processes_racing_for_one_slot_or_one_key_have_one_winner, reclaim_and_key_binding_are_exactly_once_across_connections}`）。
- key は Invocation の ledger 行と **同じ store 更新** で結び付ける。受付前に拒否された request（400 / 413 / 429 など）は key を消費せず、同じ key での再送は新規として受け付けられる。key が既存 Invocation に結び付いていれば、容量が満杯でも 429 ではなくその記録を返す。並行した同 key の request は 1 つだけが受け付けられ、残りは同じ Invocation を返す。
- 旧版の台帳（`state.json` の import を含む）に残った「Invocation の無い key」は起動時の reconcile で捨てる（404 を返し続けない）。
- alias / revision の違いは key の一致判定に含めない（同 key なら最初に受け付けた revision の結果が返る）。
- 保持期間（PLT-4631）: key は invocation が terminal になった時点から `[store] idempotency_retention_seconds`（既定 24 時間、0 は無期限）だけ応答する。実行中の invocation の key は失効しない。失効後の同じ key は新しい invocation として受け付け、失効した結び付きは起動時と 10 分ごとに削除する（invocation の行は残る）。インライン出力は `[store] output_retention_seconds`（既定 7 日）を過ぎると digest に置き換わるので、その後の同 key の再送（key の保持期間を 7 日より長くした場合）は記録（状態と digest）を返すが出力本文は返さない。これは SLA ではない。
- 非同期 invoke（PLT-4639）も同じ key 表を使う。同 key・同 input は同じ非同期 invocation を 202 で返し（完了前でも）、同 key・異なる input は 409。同期と非同期の間で key を使い回すと 409（同期の replay は結果を待つが、非同期の invocation は client deadline より長く queue に居うる）。key の結び付きは invocation・入力・outbox event と同じトランザクションで行うので、202 を受け取れなかった client が同じ key で再送すれば、commit 済みなら同じ invocation、未 commit なら新しい受付になる。
- Idempotency-Key は副作用の exactly-once を保証しない。`OutcomeUnknown` / `Timeout` の後の再送は、同 key なら記録を返すだけで、副作用が起きたかどうかを確定させない。

## 11. payload と資源の上限

値は `Limits::default()`（`crates/domain/src/limits.rs`）と `crates/protocol`、`docs/architecture.md` §4。SLA ではなく、超過時の挙動を固定するためのもの。

| 項目 | 上限 | 超過時 |
|---|---|---|
| request payload | 1 MiB（`max_payload_bytes`）。`max_payload_bytes + 64 KiB`（envelope）は `MAX_FRAME_BYTES` 以下でなければ設定を拒否 | 413 `payload_too_large`、invocation を作らない |
| response payload | 6 MiB（`max_response_bytes`、`HelloAck.max_response_bytes`）。`max_response_bytes + 64 KiB` は `MAX_FRAME_BYTES` 以下でなければ設定を拒否 | bridge が `Error{response_too_large}` → `Failed{PlatformError}`（user code の責任だが分類は platform 側の制約超過） |
| frame | 8 MiB（`MAX_FRAME_BYTES`） | 受信: `ProtocolError::FrameTooLarge`、session を閉じる → `PlatformError`。host が送る `Invoke` が encode できない: 何も書かずに `Failed{PlatformError}`（`Host.InvokeTooLarge`、§9）。bridge が送る frame が encode できない: session は閉じず、`Response` は `Error{response_too_large}` に置き換え、それ以外の frame は破棄して記録する |
| inline output | config `inline_output_max` | `PayloadRef::Digest` に切り替え。`InvocationResponse.output` は `null` |
| log 行 | 16 KiB（`max_log_line_bytes`） | char boundary で切り `truncated = true` |
| log / invocation | 2000 行、1 MiB | 超過分を捨て `LogsResponse.dropped = true` |
| artifact | 256 MiB | `ArtifactError::TooLarge` → 413 |
| description | 1024 bytes | 400 `invalid_request` |
| Idempotency-Key | 256 文字 | 400（key は消費しない） |
| trace id（`x-tachyon-trace-id` request header） | 256 bytes | 400 |
| env 変数名 | 128 文字、`TACHYON_` prefix 禁止、重複禁止 | 400（`validate_env_name`） |
| timeout | 1..=900 s（execution）、1..=120 s（init） | 400 |
| memory / cpu / ephemeral | 128..=4096 MiB / 250..=2000 m / 32..=2048 MiB（既定 256 / 500 / 256） | 400（deploy 時の `RevisionSpec::validate`） |
| guest の `/tmp`（Firecracker） | `ephemeral_storage_mib` ちょうどの scratch drive（ext4、環境作成時に host 側で確保）。`/` と `/function` は read-only drive | guest 内で `ENOSPC` / `EROFS`。host の空きは減らない（PLT-4622、`docs/evidence/isolation-20260917T011555Z/`） |
| 環境ごとの host 側ログ（Firecracker） | `console.log` 4 MiB + marker、`fc.log` 4 MiB（1 秒ごとの watchdog） | 超過分を捨てる / truncate（`docs/kvm.md` §3.6） |
| 環境作成時の host の空き | 環境の budget（artifact + drive 2 本 + ログ上限）+ 512 MiB | 何も書かずに `Unavailable` → invoke は `Failed{PlatformError}`（`Host.ProviderError`） |
| egress（Firecracker） | `none`: NIC なし。`restricted`: revision の `egress_allow`（IPv4 CIDR × tcp/udp × port、1..=16 規則・各 1..=16 port）だけ。`public-web`: 公開 IPv4 unicast と設定した resolver への DNS だけ。どの profile でも管理網・node・metadata（169.254.0.0/16）・link-local・RFC1918・CGNAT・loopback・他の special-purpose 範囲・IPv6・他 tenant の guest には届かない | 400（special-purpose 範囲や IPv6 を許可しようとした deploy）。実行時は host の nftables が drop（guest からは timeout / unreachable）。host が強制できなければ環境作成が `Unavailable`（`docs/adr/0005-egress-profiles.md`） |
| `max_concurrency`（revision） | 1..=1000。加えて node 全体の `capacity.max_concurrency`、tenant quota、node の資源（revision の resources + VMM / bridge の overhead）、`max_queue`・`max_queue_bytes`・tenant の `max_queue`（PLT-4634、ADR-0006） | 待機のまま queue deadline → 504、待ち行列の上限 → 429 |
| Heartbeat | 5 s ごと。host は無視してよい | 無視 |
| queue の stream（PLT-4638） | `[queue.limits]`: 既定 100 000 件、256 MiB、1 message 256 KiB、7 日（`max_age = 0` は設定検証で拒否）。JetStream は `discard: new` | publish を `queue_full` で拒否（既存 message は消さない）、1 message の超過は `message_too_large`。`max_age` を過ぎた message は ACK の有無によらず削除 |
| object（PLT-4638） | `[objects] max_object_bytes`（既定 8 MiB）、`tenant_quota_bytes`（既定 1 GiB、全 region 合計） | `TooLarge` / `QuotaExceeded`（何も書かない） |

## 12. 脅威と対策

| ID | 脅威 | 対策（コード上の根拠） | 残存 |
|---|---|---|---|
| T01 | tenant B が A の function / invocation を ID 推測で読む | `ensure_owned_by` → 404（§6-2） | — |
| T02 | tenant B が A の名前で invoke する | `Principal.tenant_id` は token 由来。header 不一致は 403 | token 漏洩（A6） |
| T03 | secret 値がログ・応答・state に混入 | `SecretValue` の `Debug` redact、`HelloAck.env` にだけ載せる、`HelloAck` を log しない、`BootEvidence.details` に secret 禁止 | 実装ミス。host 側は `crates/application/tests/pipeline.rs::secret_values_never_reach_host_logs`（TRACE で捕捉した host log と invocation log に値が無いこと）で確認。bridge 側の同等テストは PLT-4623 |
| T04 | secret が別 environment に配られる | `SecretDeliveryContext{tenant_id, revision_id, environment_id, epoch}` で解決。environment 割当後にしか解決しない | — |
| T05 | 古い / 偽の結果で ledger を上書き | `ExecutionLease::accepts(attempt_id, epoch)`（test `lease_fencing`）。deadline 判定後の frame は捨てる。台帳は `SlotStore::complete` が Lease と epoch を CAS で確認してからしか書かない（`crates/application/src/repository/contract_tests.rs::a_completion_with_a_stale_epoch_never_overwrites_state`、`crates/application/tests/leases.rs::a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced`） | — |
| T06 | guest が `handler_ms` / `Ready` を偽って課金・timeout を操作 | §6-1。host の `AttemptTimings`、`UsageEvent{HostObserved}`、watchdog | — |
| T07 | 暴走 handler（cpu-burn、無限ループ） | host watchdog → `Cancel` → `terminate`（SIGKILL）→ `Failed{Timeout}`。環境は再利用しない | terminate の実測（`docs/adr/0001` の残る測定） |
| T08 | 巨大 payload / response / frame による memory 枯渇 | §11 の各上限。frame 上限は codec で decode 前に拒否 | — |
| T09 | log 洪水による retention 破壊・disk 枯渇 | invocation ごとの行数 / bytes 上限、`dropped` で観測 | — |
| T10 | orphan 環境（process / socket / tap / drive / workdir）が残る | `terminate_environment` は冪等、`TerminateReport.cleaned` を列挙。起動時に application（`ReconcileService`）が `list_environments` を呼び、active として知らない環境を `terminate(Reconcile)` で回収する（listener を開ける前。`docs/architecture.md` §4）。egress `restricted` / `public-web` の tap・nftables chain・map 要素は terminate で消して読み戻しで確認し、`list_environments` が環境ディレクトリの無いものを掃除する（PLT-4622） | KVM 実機での実測（PLT-4627 の orphan テスト） |
| T11 | artifact の差し替え・改竄・他 tenant の artifact の実行 | Revision は digest 固定、`spec_digest` で `verify_integrity`、alias は generation CAS、artifact store は content-addressed。upload した tenant を所有者として記録し、revision は自 tenant が upload した digest しか解決しない（§14-1） | — |
| T12 | dev_only provider が production で使われる | `profile = "production"` は `Capabilities.dev_only` を拒否。`/v1/provider` が `dev_only` を露出 | 設定ミス |
| T13 | `TACHYON_*` を revision の env で上書きし、bridge の挙動を変える | `validate_env_name` が `TACHYON_` prefix と重複を拒否（test `invalid_specs_are_rejected`） | — |
| T14 | user code が bridge を乗っ取り、他 attempt の結果を送る | bridge は 1 in-flight、host は Lease 一致だけ受理。乗っ取られても host 側の判定は変わらない（§3） | guest 内の情報（自 tenant の secret / payload）は守れない。設計上受容 |
| T15 | 別 environment の guest が他の vsock に接続する | Firecracker は VM ごとに uds path（`<uds_path>_5000`）を持ち、host は `InstanceStart` 前に listen。`Hello.environment_id` 不一致は `HelloReject` | — |
| T16 | client 切断で invoke が放置され、資源が残る | invoke タスクは spawn され deadline まで追跡、必ず terminate。driver が panic しても環境を `terminate(Crashed)` し、Lease 解放・Attempt `Failed`・環境 `Failed`・`EnvironmentStopped` を記録する | — |
| T17 | 同時実行数の無制限化 | revision の `max_concurrency`、node の `max_concurrency` と資源の予約（起動中を含めて 1 回だけ数える）、bounded queue → 429 / 504（ADR-0006、`crates/application/src/services/admission/tests.rs::reservations_are_counted_exactly_once_under_random_operations`） | node の資源は設定値で、host の実測ではない。overhead の既定値（24 MiB）は推定 |
| T25 | 1 tenant の大量 / 長時間 invoke・起動の暴走・巨大 payload の待機で他 tenant を待たせる（DoS、容量の独占、PLT-4634） | tenant ごとの公平 queue（in-flight / weight の小さい tenant から）、tenant の同時数 quota と待ち行列の持ち分、待ち行列の件数・payload bytes の上限、各待機者の queue deadline、node 全体の start-rate token bucket、revision ごとの起動失敗 circuit breaker（open 中は即時 503）、起動は desired（合流）を超えない。長短 2 tenant の fake clock シミュレーションで短い tenant の待ちは 3 s 以内（`admission::tests::a_long_running_tenant_does_not_starve_a_short_one`、`tests/admission.rs::a_flooding_tenant_does_not_starve_another_one`） | admission は preemptive ではない: tenant quota を node の `max_concurrency` 未満にしないと、先に来た長時間の tenant が全枠を取り、その invoke が終わるまで他 tenant は待つ。1 tenant が多数の revision を持っても breaker は revision ごと。状態はプロセスのメモリだけで、再起動で到着率・breaker は消える。gateway の前段の rate limit（接続数、HTTP request 数）は無い（§15） |
| T18 | operator の越権（他 tenant の出力 / ログ / secret 参照、invoke） | §7: operator は自 tenant の function / revision / alias metadata のみ。他 tenant は 404、invocation / usage / logs 403、mutation / invoke 403 | — |
| T19 | `readyz` が provider 不能を隠す | `preflight` 失敗で `readyz` 503、invoke は 503 `provider_unavailable` | — |
| T20 | OCI artifact を「実行できる」と誤認 | `ArtifactRef::OciImage` は受理するが validation で理由付き `Failed`（`Support::Unsupported`） | — |
| T21 | guest が書き込みで host のディスクを使い切る（`/tmp` の fill、rootfs の remount、serial console の洪水、VMM ログ） | rootfs と function drive は Firecracker に `is_read_only` で渡す。書ける drive は `ephemeral_storage_mib` の scratch drive だけで、作成時に `fallocate` で確保。`console.log` は pipe 経由で上限付き、`fc.log` は watchdog。作成前に budget + 512 MiB の空きを確認（`crates/providers/firecracker/src/host_guard.rs`）。KVM 実測: 64 MiB に 256 MiB 書こうとして 58 MiB で `ENOSPC`、`/` `/function` は `EROFS`、host の空きの減少は drive の確保分（`docs/evidence/isolation-20260917T011555Z/`） | console 洪水と `fc.log` の上限は fake VMM と unit test だけで、KVM 上では未計測。`fallocate` 非対応の fs では sparse になり確保されない（warn と `scratch_drive_reserved=false`）。drive の IO 帯域（`rate_limiter`）は未設定で、隣の環境の IO を遅くできる |
| T22 | network policy が効く前に user code が動く（egress の race） | egress none は NIC を付けない。restricted / public-web は tap と nftables chain を作って `nft -j list table` で読み戻し、一致したときだけ NIC を計画に入れ、egress gate が「検証済み tap を指す eth0 1 本だけ・MMDS なし」を計画と `GET /vm/config` で確認し、`InstanceStart` 直前にもう一度読み戻す（`crates/providers/firecracker/src/network.rs`、ADR-0005）。tap に map 要素が無い間は table の既定 drop に落ちる。以下は egress none の従来の検査: `InstanceStart` の前に、送る API に `/network-interfaces` `/mmds` が無いことと、`GET /vm/config` の `network-interfaces` が空で `mmds-config` が null であることを確認し、違えば起動しない（`crates/providers/firecracker/src/egress_gate.rs`、`tests/fake_vmm.rs::egress_gate_refuses_to_start_a_vm_with_a_network_interface`）。guest の init と user code はその後にしか動かない。M8 実測で guest は `lo` だけ。restricted / public-web の KVM 実測では policy を付けた 4 回の起動すべてで policy の検証が `InstanceStart` より前（`docs/evidence/isolation-20260917T031126Z/net-race.json`） | 実測は aarch64 の nested virtualization 1 host。host の別の firewall が本 table より先に drop / accept する構成は未検証 |
| T24 | 同じ `state.db` を開く 2 つ目の gateway、または止まっていた古い gateway が、他の gateway の実行中の invocation を失敗扱いにする / 同じ環境に重ねて dispatch する / 古い結果を書き込む（PLT-4631） | dispatcher ごとの owner と lease。reclaim は期限 + `max_clock_skew_ms` を過ぎた・stopped・前の incarnation と証明された dispatcher だけ。環境の owner は変わらず、pool の claim / sweep は owner の環境だけ。slot の acquire は `(state, epoch)` の CAS と未 release lease の不在が条件。reclaim された dispatcher は heartbeat も acquire もできず、結果は store が拒否する。別 gateway が駆動中の invocation の cancel は 409（`crates/application/tests/leases.rs::two_gateways_on_one_data_dir_never_settle_each_others_work`、`crates/application/src/repository/sqlite/tests.rs::a_lease_left_by_an_exited_process_is_reclaimed_once_and_only_after_expiry`） | 時計のずれが `max_clock_skew_ms` を超える、または heartbeat が lease_ttl + skew より長く止まると、生きている gateway の仕事が reclaim され、その handler は terminate で途中終了しうる（結果は上書きされない）。同一 host の同一 file だけ。2 つの gateway プロセスを HTTP で並べた E2E は未実施 |
| T25 | revoke した token・削除した function・古い revision が、control plane に届かない data plane で使われ続ける / 古い配信や restore した control plane が新しい設定を巻き戻す（PLT-4636） | data plane は期限付きの cache だけで判断し、grant は `auth_lease_seconds`、他は `config_ttl_seconds` を過ぎれば新規受付を拒否（`Host.AuthLeaseExpired` / `Host.ConfigExpired`）。entry は保持より大きい generation でしか置き換えず、cache より小さい generation の配信は丸ごと無視して延命もしない。generation は control plane の `state.db` で読みと刻印を 1 トランザクションで行い、値の版が下がる観測は無視（`crates/application/tests/config_cache.rs::{new_work_is_refused_exactly_at_valid_until_with_the_matching_reason, older_generations_never_roll_back_a_newer_configuration, a_revoked_token_stops_working_within_one_refresh_or_one_auth_lease}`） | revoke の遅延は最大 `auth_lease_seconds`（§14-9）。実行中の invocation は期限切れでも止めない。restore した control plane の generation が data plane より小さいと、data plane は期限まで古い cache で動き、その後は拒否し続ける（data plane の再起動で解消） |
| T26 | `GET /v1/internal/config` から tenant の資格情報を得る、tenant の token で配信を取る（PLT-4636） | endpoint は `internal_token`（16 bytes 以上、定数時間比較）でだけ応答し、tenant の token は 401、未設定の gateway は 404。grant の key は bearer token の HMAC-SHA256（鍵 = `internal_token`）で、token そのものは配信しない。secret は binding の参照だけ（`apps/gateway/tests/gateway_integration.rs::a_data_plane_gateway_serves_invokes_from_delivered_configuration`） | 通信路は平文 HTTP（§14-3）。`internal_token` が漏れれば配信（function / revision の spec、tenant と role の対応、token の keyed digest）が読める。低 entropy の token は `internal_token` を知る者なら digest から総当たりできる |
| T31 | 削除した function の仕事が続く・始まる（待機中だった invocation の起動、`Idempotency-Key` の再送、cold のやり直し）/ alias を移した後も旧 revision の環境が再利用される / 回転した secret の旧世代の環境が残る / idle sweep が使用中・約束済みの環境を消す / 先行起動や再作成の往復で容量を食い潰す（PLT-4635） | 削除は受付を 409 `Host.FunctionDeleted` で止め、reconciler が待機中の invocation を起動前に同じ理由で終え、admission は再キューも拒否し、driver は環境の用意の前と dispatch の直前に削除を再確認する。実行中の invocation は完了させ、その環境は pool に戻さない。drain timeout を過ぎたものは `Host.DrainTimeout` で止める。旧 revision・旧 reuse key の環境は pool に戻さず、idle は次の reconcile で終える。idle 環境を消す判定は admission の lock の下で待機者・約束・TTL・cooldown・`min_ready` を見て予約を `Draining` にし、台帳の CAS が claim との競合を 1 つに決める。先行起動は待機者がいれば行わず、全 cap の内側で、失敗は backoff。設定 cache が期限切れの間は route の観測を保持する（`crates/application/tests/scaling.rs::{deleting_a_function_refuses_new_queued_and_retried_work_and_lets_running_work_finish, sweeps_racing_invocations_never_terminate_an_environment_in_use, an_outage_holds_routes_and_a_reconnect_storm_does_not_flap}`、`admission::tests::{an_arrival_between_the_sweep_decision_and_the_terminate_aborts_the_scale_down, a_function_deletion_refuses_waiters_and_arrivals_and_drains_at_once}`） | 削除・alias 切替の反映は data plane の設定 cache の遅延（最大 1 refresh / `config_ttl_seconds`）と reconcile 間隔だけ遅れる。drain timeout で止めた handler の外部副作用は不明（§9 と同じ扱い）。secret の rotate は invocation か先行起動が新しい値を解決するまで検出されない。`min_ready` の環境は invocation が無くても node の容量を持つ（tenant quota に数えるのは起動中だけ） |
| T23 | guest が管理網・node・metadata・他 tenant・private 範囲・IPv6 に届く（SSRF、横移動）、DNS や redirect で回り込む | 環境ごとの chain が送信元偽装・`BLOCKED_IPV4`・IPv4 以外を profile の規則より先に drop、`input` / `output` で node と tap の間を遮断、`forward` で tap 宛ては応答だけ通す（tenant 間なし）。DNS は public-web でも設定 resolver だけ、restricted では allowlist に無ければ無し。DNS 応答や HTTP redirect が denied な宛先を指しても IP 層で drop。guest は `ipv6.disable=1`、host 側でも IPv6 を drop。KVM 実測（`docs/evidence/isolation-20260917T031126Z/`）: public-web 16/16・restricted 11/11 の拒否対象がすべて失敗し、許可対象は成功。2 tenant 同時起動で A から B の guest / tap に届かず B の受け付けは 0。`169.254.169.254.nip.io` と httpbin の 302 → metadata も接続できない | `public-web` は公開 IPv4 すべてに出られるので、DNS over HTTPS や外部 relay 経由の exfiltration は防がない。hostname の allowlist は無い。帯域・接続数の上限（`rate_limiter`）が無い。gateway は root（`CAP_NET_ADMIN`）が必要。x86_64・bare metal は未測定 |
| T27 | credential の無い client、または誤った credential で queue に publish / subscribe する、gateway が匿名接続で動く（PLT-4638） | server 設定に `no_auth_user` が無く、user は account `TACHYON` の `gateway` だけ（permission は event subject・`$JS.API.>`・`$JS.ACK.>` への publish と `_INBOX.>` の subscribe）。listen は `127.0.0.1`。gateway は `[queue.nats]` に user + password_file か nkey_seed_file が無ければ設定検証で起動しない。credential file が group / other に読めれば拒否。実測: 匿名と誤 password は `Authorization Violation`（`scripts/queue/verify.sh` §1、`crates/adapters/queue-nats/src/tests.rs::nats_refuses_unauthenticated_and_wrong_credentials`、`crates/application/src/durable/tests.rs::nats_config_refuses_anonymous_connections`） | TLS なし（loopback 前提）。password は `auth.conf` に平文で置く（0600）。permission は `$JS.API.>` 全体で、stream 単位に絞っていない。同じ host の同じ user は file を読める |
| T28 | 他 tenant の object を id で読む / 消す、ciphertext を別 tenant の directory にコピーして読ませる、metadata の digest を書き換えて改竄を隠す（PLT-4638） | object は `(tenant, region, obj_id)` でしか引けず、別 scope では `NotFound`（存在しない id と同じ）。metadata に記録した scope とも照合。AES-256-GCM の AAD に id・tenant・region・key id・digest・size を入れ、平文 SHA-256 を読むたびに照合（`crates/application/src/durable/tests.rs::{objects_are_invisible_across_tenants, tampered_objects_fail_verification}`）。台帳の参照も invocation と object の tenant が違えば拒否（`repository::object_contract_tests::*::an_object_reference_never_crosses_a_tenant`） | 鍵は 1 つで全 tenant 共通（tenant ごとの鍵・KMS は無い）。鍵 file を読める者はすべての object を復号できる。`expires_at` は AAD に入っていないので、書き込み権限のある者は保持期限を延ばせる（削除は元々できる） |
| T29 | GC や retention が、まだ実行していない / 実行中の invocation の入力 object を消す。put と invocation insert の間に GC が走る（PLT-4638） | GC は台帳の `claim_for_collection` を 1 トランザクションで呼び、非 terminal の invocation（または台帳に無い invocation）が参照していれば TTL を過ぎても残す。orphan は grace（既定 1 時間）を過ぎてから。tombstone を commit した後の attach は拒否し、tombstone は file 削除後も 30 日残す（それより古い id の attach は常に拒否）（`crates/application/src/durable/tests.rs::{gc_never_collects_objects_of_unfinished_invocations, gc_and_attach_race_never_leave_a_dangling_reference}`、スレッド競合の契約テスト） | attach は invocation insert と別トランザクション（PLT-4639 の outbox で同じトランザクションにする）。grace より遅い client は object を put し直す必要がある |
| T30 | queue の ACK や dedup を根拠に「実行した / していない」を決め、二重実行や取りこぼしが起きる。queue の再起動で event が消える（PLT-4638） | 決定は台帳の CAS、ACK は commit の後（ADR-0008 §1）。JetStream は file store + `sync_interval: always`、work-queue retention。実測: 100 件 publish・10 件 ACK・10 件 in-flight の状態で kill -9 → 再起動し、未 ACK の 90 件がすべて配送された（10 件は再配送）（`scripts/queue/verify.sh` §2）。dedup は kill -9 の後、削除済み（ACK 済み）message の id を忘れる（実測、ADR-0008） | 単一 node・複製なし: disk / host の喪失で event を失い、server 停止中は publish も配送もできない。HA ではない |
| T32 | 非同期 invoke の入力（本文）が他 tenant に読まれる / 配送される、outbox の envelope から本文や他 tenant の情報が漏れる、偽造した message で他 tenant の invocation を動かす（PLT-4639） | 入力は台帳（`invocation_inputs`、`state.db` 0600）か object store（T28 の暗号化・scope）にだけ置き、queue の envelope には id・digest・size だけを載せる。object の参照は invocation insert と同じトランザクションで attach し、tenant が違えば拒否。配送された event は `read_delivery` が message id・routing tenant・台帳の tenant / revision / function / digest を照合し、入力は台帳に記録した tenant scope で読んで digest を検証する。一致しなければ 404 相当（`services::invoke_async::tests::async_invocations_and_their_inputs_never_cross_a_tenant`、`apps/gateway/tests/gateway_integration.rs::invoke_async_status_and_acceptance_never_cross_a_tenant`） | inline の入力は台帳に平文で残り、invocation が terminal になっても消えない（retention は PLT-4640）。台帳の file を読める者は inline 入力を読める（object store と違い暗号化していない）。queue の credential を持つ者は envelope（tenant id、function / revision id、digest）を読める |
| T33 | 202 を返した後に gateway が止まり、受け付けた処理が失われる / 2 回実行される。queue 停止や大量受付で台帳が際限なく膨らむ（PLT-4639） | 202 は台帳の COMMIT の後だけ。publish は outbox から行い、request の中では broker に書かない。claim の lease と CAS で 1 publisher、ACK 後・mark 前の crash は再 publish（dedup window 内なら broker が捨てる）で、consumer は台帳の invocation id で決着する。outbox は件数と滞留時間で有界で、超えれば 429 `backlog`（queue 停止中は 503 `queue_unavailable`）。failpoint で 4 つの crash 窓それぞれの SIGKILL → 再起動後の収束を確認（ADR-0010、`scripts/queue/async-e2e.sh`） | 配送は at-least-once で、dedup window の外の再 publish は 2 通になる。単一 host の `state.db` と単一 node の queue（T30、§14-11）。outbox の上限は全 tenant 共通で、1 tenant の大量受付が他 tenant の受付を 429 にしうる（tenant ごとの持ち分は無い） |

## 13. process provider が守らないもの

process provider（`crates/providers/process`）は隔離境界を持たない。以下は **守られない** ことを明示する。`docs/adr/0002-process-provider-dev-only.md` も参照。

| 守られないもの | 説明 |
|---|---|
| filesystem | user code は gateway プロセスと同じユーザーで走り、host の filesystem（`data_dir`、`config/gateway.{dev,firecracker}.toml` を含む）を読める |
| network | host の network namespace をそのまま使う。`egress_none` は成立しない |
| memory / cpu / pids | cgroup を設定しない。timeout で kill するまで資源を消費できる |
| ephemeral storage | `/tmp` は host の `/tmp` |
| 他 tenant の environment | 同一ユーザーの別プロセスとして見える（`/proc/<pid>/environ` に **secret 値が載る**） |
| kernel attack surface | host kernel に直接 syscall する |
| 起動の証跡 | `BootEvidence.guest_boot_id` は host の boot_id で、microVM の起動を示さない |
| timing | boot / init の時間は microVM の測定にならない |

守るもの（限定的）: `(attempt_id, epoch)` fencing、deadline による kill、1 in-flight、log 上限、protocol の検証。これらは「pipeline の正しさ」であって「隔離」ではない。

## 14. 残存リスクと未解決事項

1. **artifact の tenant 境界（解消済み）。** `ArtifactStore`（`crates/provider-port/src/artifact.rs`）は content-addressed で tenant を持たないが、`POST /v1/artifacts` は `ArtifactService::upload` を通り、`(tenant_id, digest)` の所有を台帳（`state.db` の `artifact_owners`）に記録する。revision の作成と validation は revision の tenant が所有する digest しか解決せず、他 tenant だけが upload した digest は存在しない digest と同じ結果（`size_bytes = 0`、`failed` の理由 `artifact unavailable: artifact not found: <digest>`）になるので、digest の存在も漏れない。同じ bytes を自分で upload すれば参照できる。port の変更は無い。残り: 修正前の版で作られた Ready revision は再検証しない。テスト: `crates/application/tests/pipeline.rs::revisions_cannot_reference_another_tenants_artifact`、`apps/gateway/tests/gateway_integration.rs::foreign_artifact_digest_is_indistinguishable_from_a_missing_one`。
2. **VMM の閉じ込め（jailer と host cgroup、PLT-4622）。** PLT-4622 は Kata / seccomp / non-root / capabilities / ServiceAccount token を挙げていたが、ADR-0001 で Firecracker を選んだので次のように読み替え、実装した。
   - Kata の sandbox 境界 → Firecracker の microVM（KVM）。1 環境 = 1 VM = 1 tenant × 1 revision。
   - non-root / capabilities → `[provider.firecracker.jailer]`。jailer（root）が `<chroot_base>/<exec>/<instance id>/root` を作って `pivot_root` し、新しい mount / PID namespace で uid / gid 64000・capability なし（`CapEff` 0）に降格してから Firecracker を exec する。chroot に入るのは Firecracker の複製、`/dev/{kvm,net/tun,urandom,userfaultfd}`、hard link の kernel / rootfs / function drive（read-only）、scratch drive と `fc.log`（jail の uid 所有 0600）、API / vsock socket だけ。
   - seccomp → Firecracker が既定で VMM の各 thread に入れる seccomp filter（provider は `--no-seccomp` を渡さない）。実測で `firecracker` / `fc_api` / `fc_vcpu*` の `Seccomp` が 2（filter）。
   - 資源の上限 → 環境ごとの cgroup v2（`cpu.max` = `cpu_millis`、`memory.max` = guest + 64 MiB、`pids.max` 64）。VMM は exec 前にその cgroup に入り、所属を確認してから構成する。`profile = "production"` では必須（委譲が無ければ環境を作らない）。
   - ServiceAccount token → guest に platform の資格情報を置かない。rootfs は bridge だけ、function drive は artifact だけで、guest に渡るのは revision の env と Secret binding（PLT-4623）の値だけ。metadata service（MMDS）は構成せず、egress gate で拒否する。
   実測は `docs/evidence/isolation-20260917T041930Z/`（HOST: 起動した全 VMM が自分の cgroup にいて上限付き、jail 化、実行後の残留 0）。
   残るもの: gateway 自体は root で動く（jailer・cgroup・tap / nftables のため）。gateway の脆弱性は host の root になる。VMM は全環境で同じ uid 64000 を使う（chroot と PID namespace で互いの `/proc` とファイルは見えないが、uid ごとの分離は無い）。network namespace は使わず、tap は host namespace に jail の uid 所有で置く（ADR-0005）。`--daemonize` と `--resource-limit` は使っていない。dev 構成（jailer 無効）では従来どおり VMM が gateway と同じユーザーで走る。
3. **平文 HTTP と静的 token。** B1 の保護は deployment 依存（§5-2, 5-3）。
4. **台帳 file の権限。** `state.db`（と `-wal` / `-shm`）は invocation の inline 出力、入力の digest、idempotency key、boot evidence を含む（secret 値と log は含まない）。gateway は `state.db` を新規作成するときに mode `0600` で作り、SQLite は `-wal` / `-shm` を同じ mode で作る（`crates/application/src/repository/sqlite/tests.rs::the_database_file_is_private_to_its_owner`）。既存 file の mode は変えないので、P1 から移行した `data_dir` や手で作った file は運用者が `config/gateway.{dev,firecracker}.toml` と同じ扱いにする。取り込み後に残る `state.json.imported-*` も同じ内容を含む。インライン出力は `[store] output_retention_seconds` を過ぎると digest に置き換わる。`secure_delete = ON` で解放された領域は上書きされるが、置き換え前の page は WAL（`-wal`）に checkpoint まで残る（gateway は終了時に `wal_checkpoint(TRUNCATE)` を行う。`inline_output_is_replaced_by_its_digest_after_retention`）。`state.json.imported-*` と file system の snapshot / backup には残る。保存時暗号化は無い。
5. **hypervisor 側の DoS（fork bomb、大量 fd）。** microVM の vCPU / memory 上限は Firecracker の `machine-config` で与え、guest から見える値と超過 alloc の `crash` 分類は実測した（ADR-0001 M9、`docs/evidence/isolation-20260917T011555Z/`）。PLT-4622 で VMM ごとの host cgroup（`cpu.max` / `memory.max` / `pids.max`）を加え、2 tenant を並べた計測をした（`docs/evidence/isolation-20260917T041930Z/` の NOISY）: 4 thread で CPU を回した 500 m の環境は cgroup で 0.517 core（5 秒窓の最大、平均 0.501、guest の steal 50.7%）に抑えられ、同時に scratch drive を ENOSPC まで埋めて fdatasync で書き続ける環境と 512 MiB を確保して OOM する環境（7 回とも `crash`、host 側の `oom_kill` 0）が動いている間、隣の tenant の固定の仕事は 13/13 成功し、CPU 部分の中央値が 1.245 倍（上限 1.30 倍）、fsync 書き込み p50 が 1.051 倍（上限 3.00 倍）、handler が 1.279 倍に遅くなった。残るもの: IO の帯域は制限していない（`io.max`・drive の `rate_limiter` なし。この 1 回では書き込みの干渉は小さかったが、共有デバイスの性能に依存する）、fork bomb は guest kernel の中で閉じ（VMM の `pids.max` は host 側の thread 数だけを縛る）、計測は aarch64 nested virtualization の 4 vCPU host 1 台・1 回だけ、CPU 遅延 1.245 倍は上限に近い。
6. **時刻の単調性。** deadline は wall-clock。host の時刻が飛ぶと deadline 判定がずれる。P1 では `AttemptTimings` に `Instant` を使い、deadline だけ wall-clock とする。
7. **`OutcomeUnknown` 後の副作用の可視化。** ledger は「不明」としか言えない。
8. **lease と時計（PLT-4631）。** lease の期限は wall-clock で判定する。gateway 間の時計のずれは `[dispatcher] max_clock_skew_ms`（既定 2 s）までしか許さず、それを超える時刻の飛びや、heartbeat が `lease_ttl_seconds + max_clock_skew_ms` より長く止まる停止（SIGSTOP、過負荷、VM の pause）では、生きている dispatcher の仕事が reclaim される。その場合も fencing で台帳は守られるが、handler は terminate されて途中で止まり、外部副作用は不明のまま残る。`dispatchers` 表には retention が無く、起動のたびに 1 行増える。
9. **revoke の遅延と設定の有効期限（PLT-4636）。** token は control plane の設定（`[[identity.tokens]]`）にしか無く、revoke は control plane の再起動で行う。data plane への反映は、届く限り次の refresh（`refresh_interval_ms` + fetch）、届かない間は最後に確認した refresh の開始から最大 `auth_lease_seconds`（既定 60 s）。function の削除・alias の変更も同様に最大 `config_ttl_seconds` 遅れうる。期限の判定は data plane の wall-clock で行い、時計が遅れると lease が長く効く（`max_clock_skew_ms` のような補正は無い）。期限切れでも実行中の invocation は止めないので、revoke した tenant の実行は最長で revision の timeout まで続く。`internal_token` の rotation 手順は無い（両側の設定を変えて再起動）。予算 token との接続は P3（PLT-4643）。
10. **admission の前提（PLT-4634）。** 容量・予約・公平 queue は 1 gateway プロセスの中にだけあり、同じ host（`data_dir`）で 2 つ目の gateway を動かすとそれぞれが自分の分しか数えない。node の容量と per-environment overhead は設定値で、host の実測や cgroup による強制ではない（overhead の既定値は推定）。KVM 上での burst は未計測。
11. **durable queue と object store は単一 node（PLT-4638）。** JetStream は 1 process・`num_replicas = 1`、object は gateway の host の `data_dir`、どちらも複製・backup が無い。host や disk を失えば未処理の event と object を失い、nats-server が止まっている間は publish も配送もできない。region label は置き場所の境界で、region 障害への耐性を意味しない（ADR-0008 §6）。object の暗号化鍵は 1 つで rotation が無く、鍵を失えば全 object を読めない。queue の通信路は loopback 前提の平文。queue の容量上限は stream 全体で、tenant ごとの上限（公平性）は無い: 1 tenant が stream を満杯にすれば他 tenant の publish も `queue_full` になる。
12. **scale to zero と drain（PLT-4635）。** 環境数 0 は host 費用 0 ではない（gateway・`state.db`・node は動き続ける）。route の観測・drain・scale event・先行起動の backoff は 1 gateway プロセスのメモリにだけあり、再起動で消える（そのとき環境も in-flight も残らない）。alias 切替の drain は「前回の有効な観測との差」で始めるので、起動直後の最初の観測より前の切替は drain しない。削除の確定（`drained_at`）は、この gateway の in-flight・admission・台帳の最新 1000 件の invocation・active な環境だけを見る。drain timeout の既定は revision の最大 timeout + cancel grace + 60 s なので、既定では drain が自分の timeout の内側にいる handler を止めることはない。これより短い値は `allow_short_drain = true` を明示したときだけ受け付け、その場合は alias を移すと長い handler が途中で止まりうる（外部副作用は不明、§9）。Firecracker（実 microVM、pool 有効）での zero-scale の E2E は未実施。
13. **非同期 invoke の入力と outbox（PLT-4639）。** inline の入力本文（既定 64 KiB 以下）は台帳に暗号化せずに置き、invocation が terminal になっても保持期限で消さない（PLT-4640 で扱う）。outbox の上限と queue の状態による拒否は tenant ごとの持ち分を持たない。queue の状態（満杯・停止）はプロセスローカルに覚えるので、複数 gateway では観測までの遅れが gateway ごとにある。配送は at-least-once で、exactly-once の根拠は台帳の CAS だけ（ADR-0010 §5）。failpoint は release build では動かない（feature `failpoints` の build を本番に使わない）。

## 15. 非目標（P1）

- 有償サービスとしての提供、SLA、料金（`UsageSummaryResponse.not_billable = true`）。
- AWS Lambda / API Gateway との互換（イベント形式、`X-Amz-*` header、Lambda Runtime API）。`tachyon.invoke.v1` / `tachyon.http.v1` は独自。
- 副作用の exactly-once。at-most-once の pipeline 実行と、Idempotency-Key による記録の再利用まで。dispatch 後の失敗は再実行しない（lease の reclaim でも自動再実行しない）ので、handler の外部副作用は client の再送による at-least-once か、`OutcomeUnknown` の「不明」になる。
- warm 再利用、idle 休止、snapshot / restore、非同期 invoke、cron（`docs/architecture.md` §6）。
- 複数 host へのスケジューリング、Kubernetes 連携。
- DDoS 耐性、rate limit、TLS 終端、WAF。
- tenant の self-service（token 発行、secret 管理 UI）。
- コンプライアンス要件（監査ログの保全、データ所在）。

## 16. 実行契約ノート（実装者向けの要約）

`docs/architecture.md` §5 を補う。矛盾があれば architecture.md とコードを正とする。

1. Revision は受付時に固定。alias を途中で変えても走っている invocation の版は変わらない。
2. 1 environment = 1 tenant × 1 revision × 1 attempt。終了後に必ず `terminate_environment`。`EnvironmentHandle` の drop は終了ではない。
3. `Hello` → 検証 → `HelloAck` → `Ready` の順。`Ready` の前に user process が死んだら `InitError`。`Ready` は 1 回だけ。
4. 結果の受理条件は `(attempt_id, epoch)` の一致のみ。`Response` の後に `Error` が来ても無視。
5. deadline は host が持つ。guest の `Heartbeat` は生存確認にすら使わない（無視してよい）。
6. `ErrorClass` → `ErrorCode` の写像は `crates/api-types::ErrorCode::http_status()`。新しい失敗を追加するときは `ErrorClass` に variant を足し、`ErrorCode` と本書 §8 を同時に更新する。
7. すべての ID は domain の型で持ち回る。文字列比較で authz をしない。
8. `LogRecord` の `phase`（boot / init / handler / shutdown）と `stream`（stdout / stderr / platform）を落とさない。`platform` 行は bridge / host が出したもので、user code の出力と混ぜない。
9. `BootEvidence` は `Hello` 受信時に `record_guest_boot_id`、provider 由来（host pid、VMM version、kernel digest）は `mark_initializing` で記録。secret を入れない。
10. `Capabilities` は測定していない能力を `Supported` と言わない（`Unverified{note}`）。fake / process provider の結果で `Supported` を主張しない。
11. 再起動は完了ではない。listener を開ける前に `ReconcileService` を走らせ、provider の孤児を `terminate(Reconcile)` で回収する。dispatch 済みの invocation は `OutcomeUnknown`、未 dispatch は `Failed{PlatformError}`（§9）。列挙に失敗しても起動は止めず、結果は `GET /readyz` の `reconcile` に出す。
