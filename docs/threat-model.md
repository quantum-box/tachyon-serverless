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
└────────────┘       │                 repositories, state.json  │
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
| B1 client ↔ gateway | HTTP、`Authorization: Bearer`、任意の `X-Tachyon-Tenant-Id`、payload | `IdentityProvider::authenticate` が返した `Principal` | header の tenant 主張（token と一致しなければ 403）、client が申告する timeout（`x-tachyon-client-timeout-ms` は上限を縮める方向にしか効かない）、client の接続維持（切断は完了を意味しない） |
| B2 gateway ↔ provider host | `EnvironmentSpec`、`EnvironmentHandle`、`TerminateReport`、`EnvironmentObservation` | provider の観測（host pid、exit code、cleaned の列挙） | provider が「作った」と言うだけの環境（`BootEvidence` と bridge の `Hello` が揃って初めて Initializing にする） |
| B3 provider host ↔ guest bridge | 長さ付き JSON frame（≤ 8 MiB） | `Hello.environment_id` が spec と一致すること、`(attempt_id, epoch)` が Lease と一致する `Response` / `Error` | `Hello.guest_boot_id` / `Ready.init_ms` / `Response.handler_ms` / `Heartbeat.ts_ms` / `Log.ts_ms`（参考値）、frame の到着順が実行順であること |
| B4 guest bridge ↔ user code | Runtime API（long-poll、response / error / ready / init error） | 何も信じない。bridge は `attempt_id` を配ったものだけ受理し、1 環境 1 in-flight を守る | user code が bridge を乗っ取れないこと（同一 guest 内なので前提にしない。§6-1） |

B3 と B4 の間には権限境界が無い。user code が guest 内で権限昇格すれば bridge も乗っ取られる。したがって **guest から来る情報はすべて「tenant のコード」が言ったものとして扱う**。これが §6 の原則の根拠である。

## 4. 資産

| 資産 | 所在 | 損なわれ方 |
|---|---|---|
| A1 artifact（tenant の実行バイナリ） | `data_dir/artifacts`（digest 名） | 他 tenant による取得・実行、改竄 |
| A2 secret 値 | `config/gateway.{dev,firecracker}.toml`（P1）、`HelloAck.env`（転送中）、guest プロセス環境 | ログ・API 応答・`state.json`・`BootEvidence.details` への混入、他 environment への配送 |
| A3 invocation の入出力 | request body、`Invocation.output`（inline ≤ 上限 / digest）、guest メモリ | 他 tenant による読み取り、改竄、上限を超える蓄積 |
| A4 ログ | `LogRecord`（invocation 単位、上限付き） | 他 tenant による読み取り、洪水による retention 破壊、secret の混入 |
| A5 ledger（Function / Revision / Alias / Invocation / Attempt / Environment / UsageEvent） | in-memory + `state.json` | 偽の結果による上書き、guest 申告に基づく計測 |
| A6 API token | `config/gateway.{dev,firecracker}.toml` | 漏洩、他 tenant への流用 |
| A7 host 資源（KVM、CPU、memory、disk、fd） | provider host | orphan 環境、無制限の同時実行、暴走 handler |
| A8 起動の証跡（`BootEvidence`、`AttemptTimings`） | Attempt | guest 申告での偽装、process provider の結果を microVM の結果と誤認 |

## 5. 前提（assumptions）

1. gateway と provider host は P1 では同一 Linux host で、gateway プロセスのユーザーが `/dev/kvm` と `data_dir` にアクセスできる。jailer は P1 では使わない（§14）。
2. B1 の transport 保護（TLS）は deployment の責務。P1 の既定 `listen = "127.0.0.1:8080"` は平文であり、ループバック外に露出しない前提。
3. `config/gateway.{dev,firecracker}.toml` は operator だけが読める。token・secret 値の保管はファイル権限に依存する。
4. `IdentityProvider` は token を `Principal` に解決する以上のことをしない。IAM / policy は将来の adapter（`docs/inventory-tachyon-apps.md` §3.7）。
5. KVM と Firecracker の隔離境界は upstream の設計を信頼する（`docs/adr/0001-execution-provider-firecracker-first.md`）。本リポジトリで hypervisor の脆弱性は扱わない。
6. 1 環境は 1 tenant の 1 revision にしか使われない（`ExecutionPolicy.concurrency_per_environment = 1`、`min_ready = 0`）。warm 再利用は `[pool] enabled` と provider の `idle_quiesce` / `idle_resume` = `Supported` が両方揃ったときだけ働き（`docs/architecture.md` §4「環境 pool と再利用キー」）、process は `Unsupported`、firecracker は実装済みだが未計測の `Unverified` を返すので、既定ではどちらも destroy-after-invoke のまま。`Unverified` を受け入れるのは計測用の `[pool] allow_unverified_idle` を明示的に立てたときだけで、その構成は API とログで「未検証」と表示される。再利用する場合も ReuseKey の 8 field 完全一致が条件で、tenant / revision / 設定 / secret generation をまたいで 1 環境が共有されることはない。
7. clock は host のもの（`Clock` trait）。guest の時刻は信頼しない。

## 6. 原則

1. **guest の自己申告は authz・metering・termination の根拠にしない。** `Ready.init_ms`、`Response.handler_ms`、`Heartbeat`、`Log.ts_ms`、`guest_boot_id` は表示・診断のための参考値。課金相当の事実は `UsageEvent{evidence_quality: HostObserved}` と `AttemptTimings`（host 計測）だけから作る。timeout の判定は host の watchdog が `execution_deadline` で行い、guest の申告や `Heartbeat` の有無で延長も短縮もしない。terminate は provider に対して host が発行し、guest の同意を要しない。
2. **他 tenant の資源は存在しない扱い（404）。** 403 で存在を漏らさない。`Function::ensure_owned_by` / `Invocation::ensure_owned_by` が `TenantMismatch` を返したら、gateway は `NotFound` に写像する。
3. **結果は Lease と一致するものだけ受理する。** `(attempt_id, epoch)` が一致しない `Response` / `Error` は捨てる。deadline 判定後に届いた結果も捨てる。
4. **secret 値は `HelloAck.env` にだけ載せる。** `SecretValue` の `Debug` は redact。ログ・API 応答・`state.json`・`BootEvidence` に書かない。`HelloAck` frame そのものをログに出さない。
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
| 容量超過（queue が満杯） | `max_queue` 超え | — | 429 `capacity_exceeded` | 無し |

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
3. gateway が再起動し、ledger に `Running` の invocation が残っている（起動時 reconcile。`Invoke` frame は書き終えているので handler が走った可能性がある）。Attempt も同じ分類にする。

入らない条件:

- `execution_deadline` 到達 → `Timeout`（host が判定済み）。
- bridge が `Error{kind: crash}` を送ってから閉じた → `Failed{Crash}`（結果は「crash」として確定）。
- frame の protocol 違反 → `Failed{PlatformError}`（`docs/architecture.md` §3-9 の分類に従う）。
- 再起動時に `Accepted` / `Queued` のまま残っていた → 一度も dispatch していない＝ handler は開始していないので `Failed{PlatformError}` / `Host.Restarted`（`crates/application/src/repository.rs::reconcile_after_restart`、`docs/architecture.md` §4）。
- `Invoke` frame が guest に届かなかった（handler は開始していない）→ `Failed`。encode できない（`FrameTooLarge`、何も書いていない）→ `PlatformError` / `Host.InvokeTooLarge`（環境は健全なので `Stopped`）。書き込み失敗（接続断）→ guest が閉じる前に送った frame を短時間読み、`Exited` なら `Crash` / `Runtime.Exited`、無ければ `Crash` / `Host.BridgeDisconnectedBeforeInvoke`。同じ guest の挙動が書き込みの競合で分類を変えないようにするため。
- **warm（pool から取り出した再利用環境）への書き込み失敗も同じ規則**。drain してから同じ 2 分類のどちらかを attempt に記録する。環境が再利用だったことは分類を変えない。warm だけが違うのは、その attempt を失敗として残したうえで **cold で 1 回だけ dispatch をやり直す**ことである（`docs/architecture.md` §4。やり直しは cold 固定なので再帰しない）。やり直す根拠は分類ではなく guest が死んだ場所にある: warm の guest は前の invocation で `Ready` を報告して実際に動いた後、idle の間に死んだので、新しい環境なら同じ request を実行できる見込みが高い。cold の guest は **この invocation のための初期化中**に死んでおり、やり直しても同じ失敗を繰り返すだけなので再実行しない。どちらの場合も `Invoke` は届いておらず handler は開始していないので、やり直しても at-most-once は破れない（`OutcomeUnknown` の「自動再実行しない」は handler が走ったかもしれない場合の話であり、ここには当たらない）。

契約:

- HTTP 502 `outcome_unknown`、`error_type = "Host.OutcomeUnknown"`（起動時 reconcile で確定したものは `Host.Restarted`）。応答には `invocation_id` を含める。
- **自動再実行しない。** 再実行の判断は client の責務。client は「実行されたかもしれない」として自身の冪等性で扱う。
- 同じ Idempotency-Key での再送は `OutcomeUnknown` の記録を返し、再実行しない（§10）。
- 環境は必ず terminate する。後から届く `Response` は Lease 解放済みのため捨てる。
- `UsageEvent` は `HandlerStarted` まで host 観測で記録し、`HandlerFinished` は `evidence_quality = Unknown` で記録するか省く。

## 10. Idempotency-Key の意味

- header: `Idempotency-Key`（`crates/api-types::headers::IDEMPOTENCY_KEY`）。1..=256 文字（`Invocation::accept` が検証）。
- scope: `(tenant_id, function_id, key)`。tenant B が同じ key を送っても A の invocation には触れない（B の scope で新規作成）。
- 一致（同 key、同 `input_digest`）: 既存 Invocation を返し、**再実行しない**。terminal でなければ現在の状態（`accepted` / `queued` / `running`）を返し、client は `GET /v1/invocations/{id}` で追跡する。
- 不一致（同 key、異なる `input_digest`）: 409 `conflict`。
- key は Invocation の ledger 行と **同じ store 更新** で結び付ける。受付前に拒否された request（400 / 413 / 429 など）は key を消費せず、同じ key での再送は新規として受け付けられる。key が既存 Invocation に結び付いていれば、容量が満杯でも 429 ではなくその記録を返す。並行した同 key の request は 1 つだけが受け付けられ、残りは同じ Invocation を返す。
- 旧版の `state.json` に残った「Invocation の無い key」は起動時の reconcile で捨てる（404 を返し続けない）。
- alias / revision の違いは key の一致判定に含めない（同 key なら最初に受け付けた revision の結果が返る）。
- 保持期間: P1 は gateway プロセスの生存期間（`state.json` に保存されていればその間）。期限切れ後の再送は新規 invocation になる。これは SLA ではない。
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
| memory / cpu / ephemeral | 128..=4096 MiB / 250..=2000 m / ≤ 2048 MiB | 400 |
| `max_concurrency`（revision） | 1..=1000。加えて gateway 全体 `capacity.max_concurrency` と `max_queue` | 429 |
| Heartbeat | 5 s ごと。host は無視してよい | 無視 |

## 12. 脅威と対策

| ID | 脅威 | 対策（コード上の根拠） | 残存 |
|---|---|---|---|
| T01 | tenant B が A の function / invocation を ID 推測で読む | `ensure_owned_by` → 404（§6-2） | — |
| T02 | tenant B が A の名前で invoke する | `Principal.tenant_id` は token 由来。header 不一致は 403 | token 漏洩（A6） |
| T03 | secret 値がログ・応答・state に混入 | `SecretValue` の `Debug` redact、`HelloAck.env` にだけ載せる、`HelloAck` を log しない、`BootEvidence.details` に secret 禁止 | 実装ミス。host 側は `crates/application/tests/pipeline.rs::secret_values_never_reach_host_logs`（TRACE で捕捉した host log と invocation log に値が無いこと）で確認。bridge 側の同等テストは PLT-4623 |
| T04 | secret が別 environment に配られる | `SecretDeliveryContext{tenant_id, revision_id, environment_id, epoch}` で解決。environment 割当後にしか解決しない | — |
| T05 | 古い / 偽の結果で ledger を上書き | `ExecutionLease::accepts(attempt_id, epoch)`（test `lease_fencing`）。deadline 判定後の frame は捨てる | — |
| T06 | guest が `handler_ms` / `Ready` を偽って課金・timeout を操作 | §6-1。host の `AttemptTimings`、`UsageEvent{HostObserved}`、watchdog | — |
| T07 | 暴走 handler（cpu-burn、無限ループ） | host watchdog → `Cancel` → `terminate`（SIGKILL）→ `Failed{Timeout}`。環境は再利用しない | terminate の実測（`docs/adr/0001` の残る測定） |
| T08 | 巨大 payload / response / frame による memory 枯渇 | §11 の各上限。frame 上限は codec で decode 前に拒否 | — |
| T09 | log 洪水による retention 破壊・disk 枯渇 | invocation ごとの行数 / bytes 上限、`dropped` で観測 | — |
| T10 | orphan 環境（process / socket / tap / drive / workdir）が残る | `terminate_environment` は冪等、`TerminateReport.cleaned` を列挙。起動時に application（`ReconcileService`）が `list_environments` を呼び、active として知らない環境を `terminate(Reconcile)` で回収する（listener を開ける前。`docs/architecture.md` §4）。P1 は tap を作らない | KVM 実機での実測（PLT-4627 の orphan テスト） |
| T11 | artifact の差し替え・改竄・他 tenant の artifact の実行 | Revision は digest 固定、`spec_digest` で `verify_integrity`、alias は generation CAS、artifact store は content-addressed。upload した tenant を所有者として記録し、revision は自 tenant が upload した digest しか解決しない（§14-1） | — |
| T12 | dev_only provider が production で使われる | `profile = "production"` は `Capabilities.dev_only` を拒否。`/v1/provider` が `dev_only` を露出 | 設定ミス |
| T13 | `TACHYON_*` を revision の env で上書きし、bridge の挙動を変える | `validate_env_name` が `TACHYON_` prefix と重複を拒否（test `invalid_specs_are_rejected`） | — |
| T14 | user code が bridge を乗っ取り、他 attempt の結果を送る | bridge は 1 in-flight、host は Lease 一致だけ受理。乗っ取られても host 側の判定は変わらない（§3） | guest 内の情報（自 tenant の secret / payload）は守れない。設計上受容 |
| T15 | 別 environment の guest が他の vsock に接続する | Firecracker は VM ごとに uds path（`<uds_path>_5000`）を持ち、host は `InstanceStart` 前に listen。`Hello.environment_id` 不一致は `HelloReject` | — |
| T16 | client 切断で invoke が放置され、資源が残る | invoke タスクは spawn され deadline まで追跡、必ず terminate。driver が panic しても環境を `terminate(Crashed)` し、Lease 解放・Attempt `Failed`・環境 `Failed`・`EnvironmentStopped` を記録する | — |
| T17 | 同時実行数の無制限化 | revision の `max_concurrency` と gateway 全体の semaphore、bounded queue → 429 / 504 | 単一 host の容量は実測前 |
| T18 | operator の越権（他 tenant の出力 / ログ / secret 参照、invoke） | §7: operator は自 tenant の function / revision / alias metadata のみ。他 tenant は 404、invocation / usage / logs 403、mutation / invoke 403 | — |
| T19 | `readyz` が provider 不能を隠す | `preflight` 失敗で `readyz` 503、invoke は 503 `provider_unavailable` | — |
| T20 | OCI artifact を「実行できる」と誤認 | `ArtifactRef::OciImage` は受理するが validation で理由付き `Failed`（`Support::Unsupported`） | — |

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

1. **artifact の tenant 境界（解消済み）。** `ArtifactStore`（`crates/provider-port/src/artifact.rs`）は content-addressed で tenant を持たないが、`POST /v1/artifacts` は `ArtifactService::upload` を通り、`(tenant_id, digest)` の所有を `state.json`（`artifact_owners`）に記録する。revision の作成と validation は revision の tenant が所有する digest しか解決せず、他 tenant だけが upload した digest は存在しない digest と同じ結果（`size_bytes = 0`、`failed` の理由 `artifact unavailable: artifact not found: <digest>`）になるので、digest の存在も漏れない。同じ bytes を自分で upload すれば参照できる。port の変更は無い。残り: 修正前の版で作られた Ready revision は再検証しない。テスト: `crates/application/tests/pipeline.rs::revisions_cannot_reference_another_tenants_artifact`、`apps/gateway/tests/gateway_integration.rs::foreign_artifact_digest_is_indistinguishable_from_a_missing_one`。
2. **jailer 未使用。** P1 の firecracker プロセスは gateway と同じユーザー・同じ mount namespace で走る。VMM 脱出時の影響範囲を狭めていない。PLT-4622 以降で jailer / 専用ユーザー / seccomp を検討する。
3. **平文 HTTP と静的 token。** B1 の保護は deployment 依存（§5-2, 5-3）。
4. **`state.json` の権限。** invocation の入出力（inline）と log を含むため、ファイル権限は `config/gateway.{dev,firecracker}.toml` と同じ扱いにする。
5. **hypervisor 側の DoS（fork bomb、大量 fd）。** microVM の vCPU / memory 上限は Firecracker の `machine-config` で与えるが、実効性は未測定（`docs/inventory-tachyon-apps.md` §5）。
6. **時刻の単調性。** deadline は wall-clock。host の時刻が飛ぶと deadline 判定がずれる。P1 では `AttemptTimings` に `Instant` を使い、deadline だけ wall-clock とする。
7. **`OutcomeUnknown` 後の副作用の可視化。** ledger は「不明」としか言えない。

## 15. 非目標（P1）

- 有償サービスとしての提供、SLA、料金（`UsageSummaryResponse.not_billable = true`）。
- AWS Lambda / API Gateway との互換（イベント形式、`X-Amz-*` header、Lambda Runtime API）。`tachyon.invoke.v1` / `tachyon.http.v1` は独自。
- 副作用の exactly-once。at-most-once の pipeline 実行と、Idempotency-Key による記録の再利用まで。
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
