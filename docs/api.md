# 管理 / Invoke API リファレンス（プロトタイプ）

- 正本: `crates/api-types/src/lib.rs`（DTO・ヘッダ名・エラーコード）。gateway は `GET /openapi.json` で OpenAPI を返す。
- 状態: P1 プロトタイプ。互換性の約束はない。
- クライアント: `tsls` CLI（`docs/cli.md`）はこの文書の形だけに依存する。

## 1. 認証とテナント

| 項目 | 内容 |
|---|---|
| 認証 | `Authorization: Bearer <token>`。管理 API（functions / artifacts / revisions / aliases / invocations 一覧 / usage）は control plane の `[[identity.tokens]]` で、invoke・HTTP アダプタ・`/v1/invocations/*`・`/v1/provider` は gateway の設定 cache に配信された grant（認可 lease 付き）で tenant / subject / roles に解決される（PLT-4636、§7）。 |
| テナント | 省略可の `x-tachyon-tenant-id` ヘッダ。token のテナントと一致しなければ 401/403。 |
| 他テナントの資源 | 常に **404**（403 は返さない。存在を漏らさないため）。 |
| roles | `deploy`（functions / artifacts / revisions / aliases の書き込み）、`invoke`（invoke / http / cancel と invocation / logs / usage の読み取り）。function / revision / alias の読み取りは `deploy` / `invoke` / `operator` のどれでもよい。`operator` は自 tenant の function / revision / alias の読み取りと `/v1/provider` だけで、他 tenant の資源は 404、invocation / logs / usage と書き込み・invoke は 403（`docs/threat-model.md` §7）。 |

`/healthz` `/readyz` `/openapi.json` は認証不要。`/v1/internal/config` は tenant の token ではなく内部 credential（`[control_plane] internal_token`）を bearer で要求する（§7）。

## 2. 共通ヘッダ

| ヘッダ | 方向 | 意味 |
|---|---|---|
| `x-tachyon-tenant-id` | req | テナント id（任意、token と一致必須） |
| `idempotency-key` | req (invoke / invokeAsync) | 1..=256 文字。scope は `(tenant, function, key)`（同期と非同期で共通。mode をまたぐ再利用は 409、§5.6.1）。同キー・同 input digest なら既存 Invocation を返す（容量が満杯でも、実行中でも、完了後でも。§5.6「Idempotency-Key」）。同キー・異なる digest → 409 `conflict`（本文は §4）。受付前に拒否された request（400 / 413 / 429）は key を消費しない。Invocation が terminal になってから `[store] idempotency_retention_seconds`（既定 24 時間）で失効する |
| `x-tachyon-client-timeout-ms` | req (invoke) | クライアント側の全体 deadline（相対 ms）。revision の timeout + init + queue で上限が掛かる。queue / init / execution の各 deadline はこれを超えず、handler の起動前に過ぎれば handler を起動しない（504 `timeout`、`Host.ClientDeadline`） |
| `x-request-id` | req/res | リクエスト id（省略時は gateway が採番） |
| `x-tachyon-invocation-id` | res (invoke/http) | 受け付けた Invocation の id |
| `x-tachyon-trace-id` | req/res (invoke/http) | trace id（request では任意、256 bytes 以下。超過は 400） |

## 3. エンドポイント一覧

| Method | Path | 役割 | 200 系 | 主なエラー |
|---|---|---|---|---|
| GET | `/healthz` | liveness | 200 | — |
| GET | `/readyz` | readiness（provider preflight OK、dispatcher lease が有効、かつ新しい invocation を受け付ける。本文に `dispatcher: {id, instance, fenced}` と `control_plane`（§7）、`usage`（usage journal・collector・ledger の運用状態、PLT-4642、§5.10.1）、`budget`（予算の強制の運用状態、PLT-4643、§5.10.2）。usage journal が満杯・停止、または予算 store の停止・collector の停止で受付を拒否している間も 503） | 200 | 503 |
| GET | `/metrics` | Prometheus text exposition（PLT-4637、`docs/metrics.md`）。全 tenant の revision・待ちを含むので `[metrics] bearer_token` の operator credential だけを受け付ける（tenant の token は operator role でも 401）。未設定の gateway には無い | 200 `text/plain; version=0.0.4` | 401, 404 |
| GET | `/v1/provider` | provider 種別 / isolation / capability 表 / preflight | 200 `ProviderInfo` | — |
| GET | `/v1/capacity` | node の容量と予約・状態別の環境数・待ち行列・start rate・拒否数・自 tenant の revision（PLT-4634、§5.1.1）、scale policy・route 状態・最後の scale event と `scaling`（PLT-4635、§8） | 200 `CapacityInfo` | 401 |
| POST | `/v1/artifacts` | 実行ファイルの生バイト (`application/octet-stream`) を upload → digest | 200 `ArtifactUploadResponse` | 401, 413 `payload_too_large` |
| POST | `/v1/functions` | Function 作成 | 201 `FunctionResponse` | 400 `invalid_request`, 409 `conflict`（同名） |
| GET | `/v1/functions` | Function 一覧（テナント内） | 200 `ListResponse<FunctionResponse>` | — |
| GET | `/v1/functions/{function_id}` | 取得 | 200 `FunctionResponse` | 404 |
| DELETE | `/v1/functions/{function_id}` | 削除。新規 invoke と待機中の invoke を 409 で止め、実行中は完了（または drain timeout）まで待ち、環境が残らなくなると `deletion_state = deleted`（PLT-4635、§8） | 200 `FunctionResponse`（`deletion_state = deleting`） | 404 |
| POST | `/v1/functions/{function_id}/revisions` | Revision 作成（非同期 validation） | 202 `RevisionResponse` (`pending`) | 400, 404 |
| GET | `/v1/functions/{function_id}/revisions` | 一覧 | 200 `ListResponse<RevisionResponse>` | 404 |
| GET | `/v1/functions/{function_id}/revisions/{revision_id}` | 取得（`ready` / `failed` を poll する） | 200 `RevisionResponse` | 404 |
| PUT | `/v1/functions/{function_id}/aliases/{alias}` | alias を CAS 更新 | 200 `AliasResponse` | 404, 409 `conflict`（generation 不一致）, 409 `revision_not_ready` |
| GET | `/v1/functions/{function_id}/aliases/{alias}` | 取得 | 200 `AliasResponse` | 404 |
| GET | `/v1/functions/{function_id}/aliases` | 一覧 | 200 `ListResponse<AliasResponse>` | 404 |
| POST | `/v1/functions/{function_id}:invoke`<br>`/v1/functions/{function_id}/invoke` | 同期 JSON invoke（query: `alias`, `revision_id`） | 200 handler の出力 JSON | 下記 §6 |
| POST | `/v1/functions/{function_id}:invokeAsync`<br>`/v1/functions/{function_id}/invokeAsync` | 非同期 JSON invoke（PLT-4639、§5.6.1）。入力・Invocation・outbox event を台帳の 1 トランザクションで確定した後に 202（query: `alias`, `revision_id`） | 202 `InvokeAsyncResponse`（`Location: /v1/invocations/{id}`） | 409 `conflict`（key 衝突）, 413 `payload_too_large`（`reason = input_too_large`）, 429 `capacity_exceeded`（`reason = backlog` / `queue_full` / `object_quota`）, 503 `async_unavailable`（`reason = queue_unavailable` / `object_store_unavailable` / `not_configured`）, 503 `control_plane_unavailable`（`Host.StoreUnavailable`） |
| ANY | `/v1/functions/{function_id}/http/{*path}` | HTTP アダプタ。リクエストを `tachyon.http.v1` event に包んで実行し、関数の status / headers / body をそのまま返す | 関数が返した status | 下記 §6（本文が `ApiErrorBody` の時だけ platform エラー） |
| GET | `/v1/functions/{function_id}/invocations` | 履歴（query: `limit`, `cursor`） | 200 `ListResponse<InvocationResponse>` | 404 |
| GET | `/v1/invocations/{invocation_id}` | Invocation 詳細（attempts, timings, boot_evidence 含む） | 200 `InvocationResponse` | 404 |
| POST | `/v1/invocations/{invocation_id}:cancel`<br>`/v1/invocations/{invocation_id}/cancel` | 実行中の Invocation を cancel | 200 `InvocationResponse` (`cancelled`) | 404, 409（既に terminal） |
| GET | `/v1/invocations/{invocation_id}/logs` | ログ（invocation 単位、行数 / bytes 上限あり） | 200 `LogsResponse` | 404 |
| GET | `/v1/functions/{function_id}/usage` | 使用量集計（課金ではない） | 200 `UsageSummaryResponse` | 404 |
| GET | `/v1/budget` | token の tenant の予算（PLT-4643、§5.10.2。仮料金の単位、決済はしない）。query: `period`（`YYYY-MM`、既定は今月 UTC） | 200 `BudgetReportResponse` | 400（`period`）, 401, 403（`invoke` role が無い）, 503 `budget_unavailable`（store 停止） |
| GET | `/v1/usage` | token の tenant の**仮**利用量・仮料金の報告（PLT-4642、§5.10.1。請求書ではない）。query: `from`, `to`（RFC 3339 か `YYYY-MM-DD`）、`group_by`（`function` / `day` / `function,day` / `none`）、`function_id` | 200 `UsageReportResponse` | 400（範囲・`group_by`）, 401, 403（`invoke` role が無い） |
| POST | `/v1/functions/{function_id}/triggers` | cron / webhook trigger の作成（PLT-4641、§5.11、`deploy` role）。webhook の `secret` はこの応答にだけ入る | 201 `TriggerResponse` | 400, 403, 404, 409（function 削除済み・`max_triggers_per_function`）, 413（cron payload）, 503 `async_unavailable`（`not_configured`） |
| GET | `/v1/functions/{function_id}/triggers`<br>`/v1/functions/{function_id}/triggers/{trigger_id}` | 一覧・取得（削除済みは含まない。secret は返さない） | 200 `ListResponse<TriggerResponse>` / `TriggerResponse` | 404 |
| PATCH | `/v1/functions/{function_id}/triggers/{trigger_id}` | 更新・有効 / 無効・secret の rotate（`expected_generation` で CAS） | 200 `TriggerResponse` | 400, 404, 409 `conflict` |
| DELETE | `/v1/functions/{function_id}/triggers/{trigger_id}` | 削除。以後の fire は commit しない。受付済みの fire は通常の非同期 invocation として続く | 200 `TriggerResponse`（`status = deleted`） | 404, 409 |
| GET | `/v1/functions/{function_id}/triggers/{trigger_id}/fires` | fire の記録（予定時刻・event id・invocation id・refused の理由。query: `limit`） | 200 `ListResponse<TriggerFireResponse>` | 404 |
| POST | `/v1/functions/{function_id}/snapshots` | **実験（X1、PLT-4653、§5.12）**: revision（body `{"revision_id"?}`、省略時は `prod`）の snapshot を作る（`deploy` role）。source を checkpoint で保持して保存し、暗号化・署名する | 201 `SnapshotResponse` | 400（synthetic でない・secret binding あり・egress が none でない・lifecycle を使わない）, 404, 409 `revision_not_ready`, 503（`[snapshots]` 無効・capability が使えない） |
| GET | `/v1/functions/{function_id}/snapshots` | 実験: snapshot 一覧（状態 `active` / `revoked` / `quarantined` / `expired`、manifest digest、restore 回数） | 200 `ListResponse<SnapshotResponse>` | 404, 503 |
| POST | `/v1/functions/{function_id}/snapshots/{snapshot_id}/revoke` | 実験: 失効（body `{"reason"}`、`deploy` role）。以後その snapshot は load されない | 200 `SnapshotResponse` | 404, 503 |
| POST | `/v1/hooks/{trigger_id}` | 署名付き webhook の配信（bearer token なし、HMAC 署名で認証、§5.11.2） | 202 `WebhookAcceptedResponse` | 400（event id）, 401（署名・timestamp）, 404, 410（無効）, 413, 429, 503 |
| GET | `/v1/functions/{function_id}/dead-letters` | 非同期 invocation の dead letter 一覧（PLT-4640、§5.6.2。新しい順、query: `limit`） | 200 `ListResponse<DeadLetterResponse>`（他 tenant の function は空） | 403, 503 `async_unavailable`（`not_configured`） |
| GET | `/v1/dead-letters/{dead_letter_id}` | dead letter と redrive の記録（§5.6.2） | 200 `DeadLetterResponse` | 403, 404（他 tenant を含む） |
| POST | `/v1/dead-letters/{dead_letter_id}:redrive`<br>`/v1/dead-letters/{dead_letter_id}/redrive` | 新しい非同期 invocation として再投入（`invoke` と `redrive` の両 role、§5.6.2）。本文 `{revision_id?, reason?}` | 202 `RedriveAcceptedResponse`（`Location: /v1/invocations/{id}`） | 400, 403, 404, 409（redrive 済み・poison・function 削除）, 429 / 503（outbox、§5.6.1 と同じ） |
| GET | `/openapi.json` | OpenAPI 3 | 200 | — |
| GET | `/v1/internal/config?since=<generation>` | data plane 向けの設定配信（`combined` で `internal_token` を設定した gateway だけ。§7） | 200 `ConfigDelivery` | 401（内部 credential でない）, 404（提供しない gateway）, 503 `control_plane_unavailable`（store） |

invoke / cancel は `:invoke` `/invoke` の両形式を受け付ける。CLI は既定で `/invoke` `/cancel` を使い、`--colon-routes` で切り替える。

## 4. エラー本文

すべてのエラーは次の形。`code` は安定した機械可読値（`ErrorCode`, snake_case）。

```json
{
  "error": {
    "code": "user_error",
    "message": "handler returned an error: validation failed",
    "request_id": "req_8f2b0c",
    "invocation_id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
    "error_type": "Handler.Error"
  }
}
```

`invocation_id` と `error_type` は invoke 系の失敗でのみ入る（履歴・ログを引くために使う）。

admission（PLT-4634、`docs/adr/0006-autoscaling-and-admission.md`）が拒否した、または待機のまま打ち切った invoke には `reason` が入る:

| `reason` | 意味 | HTTP / `code` |
|---|---|---|
| `queue_full` | 待ち行列の件数（`max_queue`）または payload bytes 合計（`max_queue_bytes`）の上限 | 429 `capacity_exceeded` |
| `quota` | tenant の待ち行列の持ち分を超えた（429）、または tenant / revision の同時数 quota で待ったまま queue deadline（504、`error_type = Host.QuotaWaitTimeout`） | 429 `capacity_exceeded` / 504 `queue_timeout` |
| `capacity` | 1 環境が node の容量に収まらない（429）、または node が満杯のまま queue deadline（504、`Host.CapacityWaitTimeout`） | 429 / 504 |
| `queue_deadline` | 順番・start rate・起動の合流を待ったまま queue deadline（`Host.QueueTimeout`） | 504 `queue_timeout` |
| `circuit_open` | revision の起動が連続して失敗し、起動を止めている（待機中だった invocation は `Host.StartCircuitOpen`） | 503 `provider_unavailable` |
| `placement` | tenant / revision の `required_region` をこの node が満たさない（負荷に関係なく緩めない） | 503 `provider_unavailable` |
| `function_deleted` | 削除より前に受け付けて待機中だった invoke を、起動せずに終えた（`error_type = Host.FunctionDeleted`、attempt なし。PLT-4635） | 409 `function_deleted` |

```json
{
  "error": {
    "code": "capacity_exceeded",
    "message": "queue_full: no capacity available and the wait queue (4 slots) is full",
    "request_id": "req_8f2b0c",
    "reason": "queue_full"
  }
}
```

429 と、`placement` / `circuit_open` の 503 は invocation を作らず、`Idempotency-Key` も消費しない。例外は `Idempotency-Key` の衝突で、409 `conflict` に key が結び付いている Invocation の id と `error_type = "Host.IdempotencyKeyReused"` が入る:

```json
{
  "error": {
    "code": "conflict",
    "message": "conflict: idempotency key `order-42` is bound to invocation inv_01j7z2k3m4n5p6q7r8s9t0v1w2 with a different input",
    "request_id": "req_8f2b0c",
    "invocation_id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
    "error_type": "Host.IdempotencyKeyReused"
  }
}
```

| code | HTTP | 意味 | CLI exit |
|---|---|---|---|
| `unauthorized` | 401 | token 無し / 無効 | 2 |
| `forbidden` | 403 | role 不足 | 2 |
| `not_found` | 404 | 資源が無い（他テナントを含む） | 2 |
| `conflict` | 409 | 同名 / generation 不一致 / idempotency 衝突（`Host.IdempotencyKeyReused`）/ 別の gateway が実行中の invocation の cancel | 2 |
| `invalid_request` | 400 | 検証エラー | 2 |
| `payload_too_large` | 413 | payload / artifact 上限超過 | 2 |
| `capacity_exceeded` | 429 | 待ち行列（件数 / bytes / tenant の持ち分）が満杯、または 1 環境が node に収まらない（`reason`） | 2 |
| `revision_not_ready` | 409 | alias / revision が `ready` でない | 2 |
| `function_deleted` | 409 | 削除済み（`deleting` / `deleted`）Function への invoke と `Idempotency-Key` の再送、および削除時に待機中だった invoke（`error_type = Host.FunctionDeleted`） | 2 |
| `user_error` | 502 | handler が `Err` を返した（`Handler.Error`） | 3 |
| `crash` | 502 | panic / プロセス異常終了（`Runtime.Panic`, `Runtime.Crash`） | 3 |
| `init_error` | 502 | Ready 前に失敗 / init timeout | 3 |
| `timeout` | 504 | 実行 deadline 超過（host が環境を終了）。drain（alias 切替・削除）の開始から `[scaling] drain_timeout_seconds` を過ぎても実行中だった invocation は `Host.DrainTimeout` | 4 |
| `queue_timeout` | 504 | queue deadline まで grant されなかった（`reason` = `capacity` / `quota` / `queue_deadline`） | 4 |
| `cancelled` | 499 | cancel API による中断 | 3 |
| `outcome_unknown` | 502 | 結果を確認できない（自動再実行しない） | 5 |
| `platform_error` | 500 | provider / bridge / 内部エラー | 6 |
| `provider_unavailable` | 503 | provider が使えない（例: `/dev/kvm` 無し）、shutdown 中、dispatcher lease を失った gateway（別の gateway に送り直す）、または provider の制御 API が preflight に失敗していて新しい環境を起動できない（`Host.ProviderControlUnavailable`。実行中と warm 環境は継続）、配置制約を満たさない node（`reason = placement`）、revision の起動 breaker が open（`reason = circuit_open`） | 6 |
| `config_unavailable` | 503 | 設定 cache が新しい仕事を保証できない（PLT-4636、§7）。`error_type`: `Host.ConfigNotDelivered`（未配信）、`Host.ConfigExpired`（TTL 切れ）、`Host.AuthLeaseExpired`（認可 lease 切れ）、`Host.ColdStartRestricted`（control plane 到達不能で cold start を制限中） | 6 |
| `control_plane_unavailable` | 503 | 管理 API を提供できない。`Host.ControlPlaneUnavailable`（data plane の gateway）、`Host.StoreUnavailable`（台帳 store が応答しない） | 6 |

| `async_unavailable` | 503 | 非同期 invoke を今は durable に受け付けられない（PLT-4639、§5.6.1）。`reason`: `queue_unavailable`（queue に届かず outbox も上限）、`object_store_unavailable`、`not_configured`（`[queue]` が無い、または台帳が揮発）。何も記録しない | 6 |
| `usage_journal_full` | 503 | 利用量を計測できないので新しい invoke を受け付けない（PLT-4642、fail closed）。`reason`: `usage_journal_full`（`error_type = Host.UsageJournalFull`、未回収 event が上限の headroom に達した）/ `usage_journal_unavailable`（`Host.UsageJournalUnavailable`、journal を開けない・書けない）。invocation を作らず、`Idempotency-Key` も消費しない（結び付いた key の replay は答える） | 6 |
| `budget_exhausted` | 429 | tenant / function の hard limit が、この invocation の最大料金を認めない（PLT-4643、§5.10.2）。`reason = budget`、`error_type = Host.BudgetExhausted`。quota（`capacity_exceeded` / `reason = quota`）とは別。invocation を作らず capacity の grant も取らない。queue で待った後、grant の時点の再確認で拒否された invocation は `Failed{platform_error, Host.BudgetExhausted}` で同じ code | 2 |
| `budget_unavailable` | 503 | 予算を強制できないので新しい invoke を受け付けない（PLT-4643、fail closed）。`reason = budget`。`error_type`: `Host.BudgetUnknown`（予算が未配信・削除・auth lease 切れ・価格表の不一致、または終わった run の精算が `max_unsettled_age_seconds` より遅れている = collector 停止）/ `Host.BudgetStoreUnavailable`（予算 store を開けない・書けない）。実行中の invocation は止めない | 6 |

`forbidden`（403）には PLT-4636 で `Host.UnknownTenant`（grant はあるが tenant が配信されていない / 削除された）と `Host.PolicyDenied`（revision の egress profile が配信された policy で許可されていない）が加わった。

## 5. DTO とフィクスチャ

ID は `<prefix>_<26 文字 lowercase ULID>`（`fn_` `rev_` `inv_` `att_` `env_` `tn_`）。時刻は RFC 3339 UTC。

### 5.1 `GET /v1/provider` → `ProviderInfo`

```json
{
  "kind": "process",
  "dev_only": true,
  "isolation": "process",
  "capabilities": {
    "isolation": "process",
    "create_terminate": { "status": "supported" },
    "observe": { "status": "supported" },
    "enforce_deadline": { "status": "supported" },
    "enforce_resource_limits": { "status": "unsupported", "reason": "process provider has no cgroup/VM limits" },
    "egress_none": { "status": "unverified", "note": "host network is reachable from a plain process" },
    "egress_restricted": { "status": "unsupported", "reason": "P1" },
    "egress_public_web": { "status": "unsupported", "reason": "P1" },
    "host_metering": { "status": "supported" },
    "idle_quiesce": { "status": "unsupported", "reason": "P1: destroy-after-invoke" },
    "idle_resume": { "status": "unsupported", "reason": "P1" },
    "snapshot_create": { "status": "unsupported", "reason": "P1" },
    "snapshot_clone": { "status": "unsupported", "reason": "P1" },
    "dev_only": true
  },
  "preflight": {
    "provider": "process",
    "ok": true,
    "checks": [
      { "name": "bridge_binary", "ok": true, "detail": "target/debug/tachyon-serverless-runtime-bridge" }
    ]
  },
  "reuse": {
    "enabled": false,
    "verified": false,
    "reason": "the provider does not support idle quiesce and resume",
    "idle_quiesce": "unsupported",
    "idle_resume": "unsupported"
  }
}
```

`start_kind = warm` の attempt では `timings` に `resume_ms` と `readiness_ms` が入り、`environment_boot_ms` / `runtime_init_ms` は 0 になる（起動も初期化もしていないため）。`readiness_ms` は再開後の drain と guest への liveness 検査（`Ping` / `Pong`）の往復を含む。cold の attempt にはこの 2 つは出ない（`null` ではなく省略される）。

`isolation` は `micro_vm` | `container` | `process`。`capabilities.*.status` は `supported` | `unsupported`(`reason`) | `unverified`(`note`)。`unverified` は実機で計測していないという意味で、supported として扱ってはならない。

`reuse` は「この gateway が環境を再利用するか」と「その構成が計測済みか」を分けて返す（`docs/architecture.md` §4）。

| field | 意味 |
|---|---|
| `enabled` | 実際に再利用するか（capability gate と `[pool] enabled` の両方が開いているか） |
| `verified` | **provider** が `idle_quiesce` / `idle_resume` を両方 `supported`（＝実機で計測済み）と申告しているか。`enabled` とは独立で、再利用が off の gateway でも `true` になりうる |
| `reason` | そう決まった理由。有効なときも無効なときも必ず入る |
| `idle_quiesce` / `idle_resume` | provider の申告（`supported` / `unsupported` / `unverified`） |

`enabled = true` かつ `verified = false` は「計測のために `[pool] allow_unverified_idle` で動かしている」という意味であり、warm が動く構成である**という主張ではない**。この組み合わせを「warm 成功」として扱ってはならない（PLT-4633）。

### 5.1.1 `GET /v1/capacity` → `CapacityInfo`（PLT-4634）

node（物理 host）と、その上の環境を分けて返す。`tenant` と `revisions` は呼び出し元 tenant の分だけで、他 tenant の id・queue・revision は出さない。

```json
{
  "node": {
    "name": "kvm-node", "region": "jp", "hosts": 1, "host_scale_out": "not_supported",
    "capacity": {"cpu_millis": 4000, "memory_mib": 6144},
    "per_environment_overhead": {"cpu_millis": 0, "memory_mib": 24, "ephemeral_storage_mib": 0},
    "max_concurrency": 8
  },
  "reserved": {"cpu_millis": 1500, "memory_mib": 840, "ephemeral_storage_mib": 768},
  "environments": {"starting": 1, "busy": 2, "promised": 0, "parking": 0, "idle": 0, "draining": 0},
  "in_flight": 3,
  "queue": {"length": 4, "bytes": 64, "max_length": 32, "max_bytes": 33554432, "timeout_seconds": 10, "oldest_age_ms": 350},
  "start_rate": {"per_second": 20, "burst": 20, "tokens": 17},
  "rejections": {"queue_full": 7, "placement": 1},
  "tenant": {"tenant_id": "tn_...", "in_flight": 3, "queued": 4, "queued_bytes": 64, "oldest_age_ms": 350, "max_concurrency": 6, "weight": 1},
  "revisions": [
    {"revision_id": "rev_...", "desired": 7, "max_environments": 8,
     "environments": {"starting": 1, "busy": 2, "promised": 0, "parking": 0, "idle": 0, "draining": 0},
     "queued": 4, "arrival_rate_per_second": 1.2, "avg_duration_ms": 410, "circuit_breaker": "closed",
     "min_ready": 0, "idle_ttl_seconds": 60, "scale_down_cooldown_seconds": 30, "route_state": "routed",
     "last_scale_event": {"kind": "activation", "reason": "backlog", "at": "2026-09-17T06:07:36.592Z"}}
  ],
  "scaling": {
    "reconcile_interval_ms": 1000, "default_idle_ttl_seconds": 60, "default_scale_down_cooldown_seconds": 30,
    "drain_timeout_seconds": 961, "warm_pool": true,
    "at_zero": "zero environments is not zero host cost: the gateway, its store and the node keep running"
  }
}
```

- `node.capacity` で省略された次元は上限なし。`reserved` は `Starting` / `Busy` / `Parking` / `Idle` / `Draining` の予約（revision の resources + overhead）の合計。
- `hosts` は常に 1、`host_scale_out` は `not_supported`（host の追加はこの prototype の範囲外）。admission が増減するのはこの node の上の環境だけ。
- `desired` は autoscaler の目標（`docs/architecture.md` §4「admission・autoscaler」）。起動はこれを超えないが、待機中の invocation の無い先行起動はしない。
- 状態は gateway プロセスのメモリにあり、再起動で `rejections`・到着率・breaker は初期化される。
- `reuse`（PLT-4637、node 全体・tenant の情報なし）: `{provider, mode, reason, first_boots, same_boot_reuses, boot_id_changed, boot_id_unreported}`。`mode` は `warm_reuse`（pool の環境を再利用する）か `every_invocation_boots`（warm の段階が無く毎回起動。process provider は常にこれ）。後ろ 4 つは attempt の guest boot id をその環境の最初の boot id と比べた数で、`boot_id_changed` は 0 のままであるべき値（`docs/metrics.md` §4）。プロセスのメモリにあり再起動で 0 に戻る。
- PLT-4635 の欄（§8）: revision の `min_ready` / `idle_ttl_seconds` / `scale_down_cooldown_seconds`（既定値を解決した値）、`route_state`、`last_scale_event`。環境が 0 になり admission が revision を忘れた後も、最後の event を持つ revision は環境数 0 で載る。「ready」の環境は `idle`（pool にあってすぐ使える）に当たる。

### 5.2 `POST /v1/artifacts`

- Request: `Content-Type: application/octet-stream`、本文は実行ファイルそのもの。firecracker provider では static Linux (musl) バイナリであること。
- upload した tenant が digest の所有者として記録される。revision の `artifact.digest` は自 tenant が upload した digest でなければならない。他 tenant だけが upload した digest は、存在しない digest と同じく revision が `failed`（`artifact unavailable: artifact not found: <digest>`）になる。同じ bytes を自分で upload すれば参照できる。
- Response 201:

```json
{ "digest": "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", "size_bytes": 1048576 }
```

### 5.3 Function

`POST /v1/functions` request:

```json
{ "name": "hello", "description": "demo" }
```

`name` は `[a-z0-9-]{1,63}`、先頭末尾は英数字。Response 201 / `GET` 200 → `FunctionResponse`:

```json
{
  "id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "tenant_id": "tn_01hzzzzzzzzzzzzzzzzzzzzzza",
  "name": "hello",
  "description": "demo",
  "created_at": "2026-09-15T01:00:00Z",
  "updated_at": "2026-09-15T01:00:00Z"
}
```

`deletion_state` は `live` / `deleting`（`deleted_at` が付き、新規と待機中の invoke は 409。実行中の invocation と環境が残っている）/ `deleted`（何も残っていない。`drained_at` が付く）。一覧は `{"items": [...], "next_cursor": "..."}`（`next_cursor` は続きがある時だけ）。

### 5.4 Revision

`POST /v1/functions/{function_id}/revisions` request（省略した項目は既定値）:

```json
{
  "artifact": { "kind": "binary", "digest": "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" },
  "architecture": "aarch64",
  "resources": { "memory_mib": 256, "cpu_millis": 500, "ephemeral_storage_mib": 256 },
  "execution": { "timeout_seconds": 30, "initialization_timeout_seconds": 30, "max_concurrency": 4 },
  "egress": null,
  "egress_allow": [],
  "env_vars": [["GREETING", "v1"]],
  "secrets": [{ "env_name": "DEMO_SECRET", "binding_ref": "demo-secret" }],
  "description": "v1",
  "publish_to_prod": true
}
```

- `artifact.kind` は `binary`（`digest` は `/v1/artifacts` の戻り値）または `oci_image`（`reference`。受け付けるが P1 provider では実行できず `failed` になる）。
- `env_vars` は `[name, value]` の配列。`TACHYON_` で始まる名前は予約。secret の値は API を通らず、`binding_ref` が gateway 設定 `[[secrets.bindings]]` で解決され HelloAck の env にだけ載る。
- `required_region`（任意）: この revision を動かしてよい region（例 `jp`）。node の `[capacity.node] region` が一致しなければ invoke は 503 `placement`。spec には `placement.region` として入り、未指定なら省略される（既存 revision の digest は変わらない）。
- `egress`: `none`（既定。NIC なし）/ `restricted` / `public-web`。`egress_allow` は `restricted` のときだけ必須（1..=16 件）で、各要素は `{"cidr": "1.1.1.1/32", "protocol": "tcp", "ports": [443]}`（`protocol` は `tcp` 既定 / `udp`、`ports` は 1..=16 件）。IPv4 CIDR のみで、0/8・10/8・100.64/10・127/8・169.254/16・172.16/12・192.168/16 などの special-purpose 範囲と重なるものは 400。どの profile でも管理網・node・metadata・private 範囲・IPv6 には届かない（`docs/adr/0005-egress-profiles.md`）。spec の `egress_allow` は空なら省略される。
- `publish_to_prod`（既定 true）: `ready` になった時点で alias `prod` を向ける。
- `restore`（任意、**実験 X1、PLT-4653**）: `{"policy": "disabled" | "prefer" | "require", "synthetic_init_sample": bool}`。省略時は `disabled`（何も変わらず、spec にも出ないので既存 revision の digest は変わらない）。`prefer` / `require` は `synthetic_init_sample = true`・secret binding なし・egress `none` の revision だけ作れる（違えば 400）。`prefer`: 互換で検証済みの snapshot があれば clone で起動し、無ければ cold（attempt は `start_kind = cold`、`boot_evidence.details.restore_fallback` に理由）。`require`: clone できなければ invocation は `init_error` / `Host.RestoreRequiredUnavailable`（message に理由の code）で、cold にはならない。restore された attempt は `start_kind = restored` で、`boot_evidence.details` に `snapshot_id`、`snapshot_manifest_digest`、`restore_instance_id`、`restore_generation`、`restore_{verify,load,doorbell,reconnect,ready}_ms`。gateway の `[snapshots] enabled` が無ければ、`require` は常に失敗し `prefer` は常に cold（`restore_fallback = not_configured`）。詳細は §5.12 と `docs/adr/0017-snapshot-manifest-and-clone.md`。
- scale policy（PLT-4635、§8）: `execution.min_ready`（既定 0 = 無負荷なら環境 0。`0..=16` かつ `max_concurrency` 以下。環境再利用が有効な gateway でだけ満たされる）、`execution.idle_ttl_seconds`（省略時 gateway の `[pool] idle_ttl_seconds`、`1..=86400`）、`execution.scale_down_cooldown_seconds`（省略時 `[scaling] scale_down_cooldown_seconds`、`0..=3600`）。環境数の上限は `max_concurrency`。後の 2 つは未指定なら spec に出ない（既存 revision の digest は変わらない）。

Response 202 → `RevisionResponse`（`status` は `pending` → `preparing` → `validating` → `ready` | `failed`）:

```json
{
  "id": "rev_01j7z0m1n2p3q4r5s6t7v8w9x0",
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "number": 1,
  "status": "ready",
  "spec": {
    "artifact": { "kind": "binary", "digest": "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", "size_bytes": 1048576 },
    "runtime": { "protocol": "tachyon-invoke-v1", "architecture": "aarch64" },
    "resources": { "memory_mib": 256, "cpu_millis": 500, "ephemeral_storage_mib": 256 },
    "execution": { "timeout_seconds": 30, "initialization_timeout_seconds": 30, "concurrency_per_environment": 1, "max_concurrency": 4, "min_ready": 0 },
    "egress": "none",
    "env_vars": [["GREETING", "v1"]],
    "secrets": [{ "env_name": "DEMO_SECRET", "binding_ref": "demo-secret" }],
    "description": "v1"
  },
  "spec_digest": "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
  "created_at": "2026-09-15T01:00:01Z",
  "updated_at": "2026-09-15T01:00:02Z"
}
```

`failed` の時は `"failure_reason": "..."` が付く。

### 5.5 Alias

`PUT /v1/functions/{function_id}/aliases/prod` request:

```json
{ "revision_id": "rev_01j7z0m1n2p3q4r5s6t7v8w9x0", "expected_generation": 3 }
```

`expected_generation` を渡すと、現在の generation と一致する時だけ更新される（不一致は 409 `conflict`）。Response → `AliasResponse`:

```json
{
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "name": "prod",
  "revision_id": "rev_01j7z0m1n2p3q4r5s6t7v8w9x0",
  "generation": 4,
  "previous_revision_id": "rev_01j7z0b1c2d3e4f5g6h7j8k9m0",
  "updated_at": "2026-09-15T01:05:00Z"
}
```

rollback は「`GET` → `previous_revision_id` を `expected_generation = generation` で `PUT`」で行う（CLI の `functions rollback`）。

### 5.6 Invoke

`POST /v1/functions/{function_id}/invoke?alias=prod`（`revision_id=rev_...` で版を固定可）。本文は任意の JSON（`Content-Type: application/json`、上限 `limits.max_payload_bytes`）。

```json
{ "name": "demo" }
```

成功時は 200 で **handler の出力 JSON をそのまま** 返し、`x-tachyon-invocation-id` / `x-tachyon-trace-id` ヘッダが付く（CLI はこの形を前提にしている）。

```json
{ "message": "hello, demo", "greeting": "v1", "secret_present": true }
```

失敗時は §4 のエラー本文（`invocation_id` 付き）。同期 invoke の deadline は受付時に固定され、クライアントが切断しても実行は deadline まで追跡・記録される。

**再実行しない。** handler に dispatch した後の失敗（接続断、timeout、lease を失った gateway の reclaim）は `Failed` / `OutcomeUnknown` のまま返し、gateway が自動で再実行することはない。handler の外部副作用は exactly-once ではなく、client が再送すれば at-least-once、`outcome_unknown` なら不明である。副作用を 1 回にまとめたい handler は、`Idempotency-Key` とは別に自分の副作用先で冪等にする。

**Idempotency-Key**（PLT-4631、`docs/threat-model.md` §10）:

| 状況 | 応答 |
|---|---|
| 新しい key | 通常どおり実行し、key を Invocation と同じ store 更新で結び付ける |
| 同 key・同 input digest、Invocation が終わっている（保持期間内） | 記録を返す（成功なら出力、失敗なら §4 の本文）。handler は動かない |
| 同 key・同 input digest、Invocation が実行中（同じ gateway） | その実行の終了を待って同じ結果を返す |
| 同 key・同 input digest、Invocation が実行中（同じ `state.db` を使う別の gateway） | 台帳を追い、terminal になるか元の Invocation の client deadline まで待って返す。deadline までに終わらなければ現在の状態を返す（`platform_error` / `Host.Incomplete`、`invocation_id` 付き）ので `GET /v1/invocations/{id}` で追う |
| 同 key・異なる input digest（完了・実行中を問わない） | 409 `conflict`、`invocation_id` と `error_type = "Host.IdempotencyKeyReused"`（§4）。何も実行しない |
| 保持期間を過ぎた key | 新しい key として扱う |
| 同 key の並行 request（別プロセスを含む） | 1 つだけが受け付けられ、残りは同じ Invocation を返す（key は store の主キーで一意） |

input digest は request 本文を JSON として解釈し、直列化し直した bytes の SHA-256（空白の違いは同じ digest になる）。alias / revision の違いは一致判定に含めない。

### 5.6.1 invokeAsync（PLT-4639）

`POST /v1/functions/{function_id}:invokeAsync?alias=prod`（`/invokeAsync` も可、`revision_id=rev_...` で版を固定可）。本文は任意の JSON（上限 `limits.max_payload_bytes`）。決定の詳細は `docs/adr/0010-invoke-async-and-outbox.md`。

**202 は台帳の COMMIT の後にだけ返す。** その時点で invocation（`mode = async`、`status = accepted`、revision は受付時に固定）、入力（`[invoke_async] inline_input_max_bytes` 以下は台帳、超えるものは object store に put してから参照）、Idempotency-Key の結び付き、outbox event が 1 トランザクションで確定している。gateway が 202 の直後に止まっても、再起動後に配送が続く。request の中で queue には送らない（outbox publisher が後で送る）。

```http
HTTP/1.1 202 Accepted
Location: /v1/invocations/inv_01j7z2k3m4n5p6q7r8s9t0v1w2
x-tachyon-invocation-id: inv_01j7z2k3m4n5p6q7r8s9t0v1w2
```

```json
{
  "invocation_id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "revision_id": "rev_01j7z0m1n2p3q4r5s6t7v8w9x0",
  "alias": "prod",
  "status": "accepted",
  "status_url": "/v1/invocations/inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
  "input_digest": "sha256:5f3d...",
  "input_size_bytes": 15,
  "input_storage": "inline",
  "replayed": false,
  "trace_id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
  "accepted_at": "2026-09-17T06:21:15.000Z"
}
```

`GET /v1/invocations/{id}`（§5.8）は `mode = "async"` で、`status` は `accepted`（台帳に確定、queue にはまだ無い）→ `queued`（outbox publisher が queue の ACK を得た）→ `running`（dispatcher が実行中、§5.6.2）→ terminal。retry を待つ間は再び `queued`。`deadlines.queue_deadline` は受付時刻 + `[invoke_async] queue_deadline_seconds`（既定 24 時間）で、これと `[async_dispatch] max_event_age_seconds` の早い方を過ぎると実行を始めない（dead letter `expired`）。

| 状況 | 応答 | 記録 |
|---|---|---|
| 受付 | 202 | invocation・入力・key・outbox event |
| 同 key・同 input（完了・未完了を問わない） | 202、同じ `invocation_id`、`replayed = true`、その時点の `status` | 何も増えない |
| 同 key・異なる input | 409 `conflict`（`Host.IdempotencyKeyReused`、`invocation_id` 付き） | なし |
| 同期 invoke の invocation に結び付いた key、またはその逆 | 409 `conflict` | なし |
| payload が `limits.max_payload_bytes` 超過 | 413 `payload_too_large` | なし |
| inline 上限を超え、object store が無い / object の size 上限超過 | 413 `payload_too_large`、`reason = input_too_large` | なし |
| tenant の object quota 超過 | 429 `capacity_exceeded`、`reason = object_quota` | なし |
| object store が応答しない | 503 `async_unavailable`、`reason = object_store_unavailable` | なし |
| outbox の未送信が `max_pending_events` 件以上、または最古が `max_pending_age_seconds` より古い（queue は健全） | 429 `capacity_exceeded`、`reason = backlog` | なし |
| queue が直前の publish を満杯で拒否した（未送信がある） | 429 `capacity_exceeded`、`reason = queue_full` | なし |
| queue に届かない | outbox に余裕がある間は **202**。上限に達したら 503 `async_unavailable`、`reason = queue_unavailable` | 202 の分は queue の回復後に配送 |
| 台帳が失敗（COMMIT 前） | 503 `control_plane_unavailable`（`Host.StoreUnavailable`） | なし（put 済みの object は GC が回収） |
| `[queue]` が無い、または台帳が揮発（`[store] backend = "memory"`） | 503 `async_unavailable`、`reason = not_configured` | なし |
| function 削除中・削除済み（PLT-4635） / revision が ready でない / 他 tenant の function | 409 `function_deleted`（`Host.FunctionDeleted`） / 409 `revision_not_ready` / 404 | なし |

配送は at-least-once。publisher が queue の ACK を得た後、台帳に送信済みを記録する前に止まると、同じ event（message id = invocation id）がもう一度 publish される。broker の duplicate window（既定 120 s）内なら 1 通にまとまるが、外なら 2 通届きうる。consumer は message ではなく invocation id で台帳に照らして決着する。

### 5.6.2 非同期の実行・retry・dead letter・redrive（PLT-4640）

決定の詳細は `docs/adr/0013-async-dispatch-retry-dlq.md`。gateway は `[async_dispatch] workers` 本の worker で queue から 1 件ずつ取り出し、台帳の dispatch 行を claim してから同期 invoke と同じ経路（admission、環境、attempt と lease、後始末、usage）で**受付時に固定した Revision と保存した入力**を実行する。結果（terminal、次の試行の予約、dead letter）を台帳に 1 トランザクションで確定した**後に** queue の message を ACK する。

**状態の遷移**

| `status` | `dispatch.state` | 意味 |
|---|---|---|
| `accepted` / `queued` | `pending`（行なし）/ `scheduled` | 実行待ち。retry 待ちは `scheduled` と `next_attempt_at` |
| `running` | `running` | ある gateway が claim して実行中 |
| `succeeded` | `done` | 成功 |
| `cancelled` | `done` | 利用者の cancel |
| `failed` / `outcome_unknown` | `dead` | dead letter になった（`dispatch.dead_letter_id`）。`error` は最後の試行のエラー |

`GET /v1/invocations/{id}` の非同期 invocation には `dispatch` が付く:

```json
"dispatch": {
  "state": "scheduled",
  "attempts": 1,
  "deferrals": 0,
  "generation": 1,
  "next_attempt_at": "2026-09-17T07:36:37.260Z",
  "last_error": {"class": "user_error", "error_type": "Downstream.Unavailable", "message": "..."}
}
```

`dead_letter_id`（dead letter になったとき）と `redriven_from`（redrive で作られた invocation のとき、redrive の記録）は該当するときだけ付く。

**retry policy**（`[async_dispatch]`、既定値。`[[async_dispatch.function]]` で function ごとに `max_attempts` / `max_event_age_seconds` を上書き）

| 項目 | 既定 | 内容 |
|---|---|---|
| `max_attempts` | 3 | 数える試行の上限。超えたら dead letter `attempts_exhausted` |
| `max_event_age_seconds` | 21600 | 受付からこの時間（と `queue_deadline` の早い方）を過ぎたら試行を始めない。dead letter `expired` |
| `backoff_initial_ms` / `backoff_max_ms` / `backoff_floor_ms` | 1000 / 300000 / 100 | full jitter: n 回目の後の遅延は `[0, min(max, initial × 2^(n-1))]` の一様乱数（下限 floor） |
| `retry_budget` / `retry_budget_window_seconds` | 100 / 60 | function ごと・window ごとの retry の上限（gateway ごと）。超えた retry は次の window へ先送り（数えない） |
| `workers` | `[capacity] max_concurrency` の半分 | 非同期の同時実行数。同期 invoke の分を常に残す |
| `admission_wait_ms` | 2000 | 非同期の試行が容量を待つ時間。空かなければ先送り（数えない） |
| `claim_ttl_seconds` | 60 | 実行中の claim の期限（実行中は 1/3 ごとに延長）。gateway が消えると、この後に別の gateway が続ける |
| `ack_wait_seconds` / `max_deliver` | 300 / 1000 | broker の再配送。retry の回数は broker ではなく台帳が数える |

**エラーの分類**

| 結果 | 扱い |
|---|---|
| handler のエラー（`user_error`）、`crash`、`init_error`、`timeout`、`outcome_unknown`、その他の platform エラー | retry（数える） |
| 容量・quota・breaker・gateway 停止・設定 cache の期限切れ・入力 object の一時的な読めなさ・retry budget・usage journal の拒否（`Host.UsageJournalFull` / `Host.UsageJournalUnavailable`、PLT-4642） | 先送り（`deferrals`、数えない。期限は効く） |
| 入力の digest 不一致（`Host.InputCorrupt`）、JSON でない入力、応答の上限超過（`Runtime.ResponseTooLarge` / `Host.ResponseTooLarge`）、secret binding の認可（`Host.SecretBindingUnavailable`）、policy 拒否、未対応 artifact | dead letter `non_retryable`（retry しない） |
| function の削除（`Host.FunctionDeleted`） | dead letter `function_deleted` |
| 固定 Revision が解決できない | dead letter `revision_unavailable` |
| 利用者の cancel | `cancelled`（dead letter にしない） |
| decode できない、台帳と合わない event | dead letter `poison`（invocation なし）。message は term し、再配送しない |

**dead letter**

`GET /v1/functions/{function_id}/dead-letters` → `ListResponse<DeadLetterResponse>`、`GET /v1/dead-letters/{id}` → `DeadLetterResponse`（`invoke` role、tenant 内だけ）:

```json
{
  "id": "dlq_01m2q4s1pkr0cwp631behj5734",
  "reason": "attempts_exhausted",
  "status": "open",
  "function_id": "fn_01m2q4qfry2dqnx7rer8ftg3hz",
  "invocation_id": "inv_01m2q4qg9tw3mzxpmgjvc8xwds",
  "revision_id": "rev_01m2q4qg2kgpb1cjnjgfrgd4n4",
  "attempts": 3,
  "deferrals": 0,
  "last_error": {"class": "user_error", "error_type": "Downstream.Unavailable", "message": "..."},
  "accepted_at": "2026-09-17T07:36:35.4Z",
  "first_attempt_at": "2026-09-17T07:36:36.7Z",
  "last_attempt_at": "2026-09-17T07:36:38.1Z",
  "created_at": "2026-09-17T07:36:38.2Z",
  "input_digest": "sha256:...",
  "input_size_bytes": 37,
  "input_storage": "inline",
  "redrive_count": 0,
  "redrives": []
}
```

入力は複製しない（台帳の inline 本文か object store の同じ object を指す）。`open` の dead letter が参照する object は GC で消さない。poison は `invocation_id` / `function_id` が無く、`message_id` と `detail` だけを持ち、function の一覧には出ない（`GET /v1/dead-letters/{id}` で読める）。

**redrive**

`POST /v1/dead-letters/{id}:redrive`、本文（任意）:

```json
{"revision_id": "rev_01m2q4rz9v50trnqcrrb2nmvqt", "reason": "downstream fixed"}
```

| 状況 | 応答 |
|---|---|
| `invoke` と `redrive` の両 role（token の `roles = ["invoke", "redrive"]`）、自 tenant の `open` の dead letter | 202 `RedriveAcceptedResponse`（`redrive`: 記録、`invocation`: 新しい invocation の 202 本文）。`Location: /v1/invocations/{新しい id}` |
| `redrive` role が無い | 403 `forbidden` |
| 他 tenant の dead letter、存在しない id | 404 `not_found` |
| `revision_id` が他 function / 他 tenant / 存在しない | 404（設定 cache で解決できない）/ 400 |
| redrive 済み、poison | 409 `conflict` |
| function 削除済み / revision が ready でない | 409 `function_deleted` / 409 `revision_not_ready` |
| outbox の上限・queue 停止 | §5.6.1 と同じ 429 / 503 |

redrive は**新しい非同期 invocation** を作る（元の invocation は terminal のまま）。Revision は既定で元の固定 Revision、`revision_id` を明示したときだけ同じ function の別 Revision。入力は元と同じもの（同じ tenant の同じ本文 / object）。invocation・入力の参照・outbox event・redrive 記録（`requested_by` = principal の subject、`reason`、時刻、元の invocation、dead letter）・dead letter の `redriven` を 1 トランザクションで書く。新しい invocation の `dispatch.redriven_from` に記録が付く。`Idempotency-Key` は引き継がない。

### 5.6.3 非同期 invoke の at-least-once 契約（PLT-4640）

非同期 invocation の**配送も実行も at-least-once** です。

- queue の ACK が失われると同じ event が再配送されますが、invocation が既に terminal なら何も実行しません。
- 実行中に gateway が止まると、その試行の handler が外部に何をしたか platform には分かりません。claim の期限（`claim_ttl_seconds`）の後、次の試行が**もう一度 handler を実行します**。handler の外部副作用（DB 書込み、外部 API 呼び出し、メール送信）の**後**、結果を台帳に確定する**前**に止まった場合、その副作用は 2 回起きえます。
- 台帳が保証するのは、1 invocation に terminal の記録が 1 つ、dead letter が 1 件、ということだけです。exactly-once の実行は約束しません。
- `Idempotency-Key`（§5.6.1）は受付の重複をまとめるもので、実行の重複は防ぎません。

handler は**業務の冪等キー**で副作用を 1 回にしてください: 一意制約付きの insert（`INSERT ... ON CONFLICT DO NOTHING`）、条件付き書込み、処理済みキーの記録。`examples/idempotent-async` は `order_id` を冪等キーにして `effects/<order_id>.json` を排他的に作り（`create_new`）、2 回目の実行は副作用を行わずに `{"applied": false}` を返します。`scripts/queue/async-dispatch-e2e.sh` は、副作用の後・確定の前に gateway を SIGKILL し、再起動後に handler が 2 回実行され、副作用は 1 回、terminal の記録は 1 つであることを確かめます。

### 5.11 trigger（PLT-4641）

cron と署名付き webhook。決定の詳細は `docs/adr/0014-cron-and-webhook-triggers.md`。**trigger は何も実行しない**: すべての fire は §5.6.1 の非同期 invocation として受け付けられ（設定 cache での tenant の認可と解決、revision の固定、outbox、受付の上限）、実行・retry・DLQ・利用量の計測（PLT-4642）は非同期 invocation の共通経路（PLT-4640）で、fire の受付では計測しない。fire の記録（`trigger_fires`）は invocation と同じトランザクションで書かれ、`(trigger_id, fire_key)` で一意。

trigger を使えるのは、`invokeAsync` が有効（`[queue]` と `state.db`）な `combined` gateway だけ。data plane では CRUD が 503 `control_plane_unavailable`、webhook が 503 `async_unavailable`（`not_configured`）。

#### 5.11.1 CRUD

`POST /v1/functions/{function_id}/triggers`:

```json
{
  "name": "nightly-report",
  "kind": "cron",
  "enabled": true,
  "target": { "alias": "prod" },
  "cron": {
    "expression": "30 2 * * *",
    "timezone": "Asia/Tokyo",
    "payload": { "report": "daily" },
    "missed_run_policy": { "kind": "run_all", "max_runs": 3 }
  }
}
```

```json
{ "name": "orders", "kind": "webhook", "webhook": { "tolerance_seconds": 300, "max_body_bytes": 65536, "event_id_header": "x-tachyon-webhook-id" } }
```

| field | 内容 |
|---|---|
| `name` | 1..=64 文字の label（一意でない） |
| `kind` | `cron` \| `webhook`。対応する `cron` / `webhook` だけを持つ |
| `enabled` | 既定 `true` |
| `target` | `alias`（既定 `prod`、fire ごとに解決して固定）か `revision_id`（その revision に固定）。両方は 400 |
| `cron.expression` | 5 field（`minute hour day-of-month month day-of-week`）か、先頭に秒を足した 6 field。`*` `n` `a-b` `*/s` `a-b/s` `a/s` `,`、`JAN`-`DEC`、`SUN`-`SAT`（0 と 7 は日曜）、`@yearly` `@monthly` `@weekly` `@daily` `@hourly`。day-of-month と day-of-week の両方を制限すると **どちらか** に一致した日 |
| `cron.timezone` | IANA 名（既定 `UTC`）。式は wall-clock 時刻で評価し、DST で **存在しない時刻は発火しない**、**2 回ある時刻は早い方で 1 回** |
| `cron.payload` | 静的 JSON（上限 `limits.max_payload_bytes` − 1 KiB） |
| `cron.missed_run_policy` | `{"kind":"skip"}`（既定） \| `{"kind":"run_once"}` \| `{"kind":"run_all","max_runs":N}`（1..=`[triggers] max_run_all`）。§5.11.3 |
| `webhook.source` | `generic-hmac`（唯一） |
| `webhook.tolerance_seconds` | 既定 `[triggers] webhook_default_tolerance_seconds`（300）、上限 `webhook_max_tolerance_seconds`（3600） |
| `webhook.max_body_bytes` | 既定・上限 `[triggers] webhook_max_body_bytes`（256 KiB、`limits.max_payload_bytes` で頭打ち） |
| `webhook.event_id_header` | 既定 `x-tachyon-webhook-id`。小文字の header 名で、timestamp / signature / 標準 header 以外 |

`201` の `TriggerResponse`（webhook）。`secret` は **この応答（と rotate の応答）にだけ** 入り、`Cache-Control: no-store`。以後の GET / list は `secret_fingerprint` だけを返す:

```json
{
  "id": "trg_01j7z3a4b5c6d7e8f9g0h1j2k3",
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "name": "orders",
  "kind": "webhook",
  "enabled": true,
  "status": "enabled",
  "generation": 1,
  "target": {},
  "webhook": {
    "source": "generic-hmac",
    "tolerance_seconds": 300,
    "max_body_bytes": 65536,
    "event_id_header": "x-tachyon-webhook-id",
    "timestamp_header": "x-tachyon-webhook-timestamp",
    "signature_header": "x-tachyon-webhook-signature",
    "url": "/v1/hooks/trg_01j7z3a4b5c6d7e8f9g0h1j2k3",
    "secret_fingerprint": "sha256:5a0532de6876"
  },
  "secret": "whsec_6f1c...(64 hex)",
  "created_at": "2026-09-17T07:00:00Z",
  "updated_at": "2026-09-17T07:00:00Z"
}
```

cron の `TriggerResponse` は `cron: {expression, timezone, payload, missed_run_policy, next_fire_at, last_scheduled_at}` を持つ。`next_fire_at` は scheduler が次に処理する予定時刻（UTC、無効・削除済みは無し）。

`PATCH`（`UpdateTriggerRequest`）: 指定した field だけを変える。`expected_generation` が違えば 409。`enabled`、`name`、`target`、cron の `expression` / `timezone` / `payload` / `missed_run_policy`、webhook の `tolerance_seconds` / `max_body_bytes` / `event_id_header` / `rotate_secret: true`（新しい secret をこの応答で 1 回だけ返し、旧 secret は即座に無効）。kind に合わない field は 400。有効化・式 / zone の変更で `next_fire_at` は **現在時刻の次** から始まる（無効だった期間を catch-up しない）。

`DELETE` は `status = deleted` にして webhook の secret を消す。削除済みの trigger は GET / list / PATCH / DELETE で 404。

`GET .../fires?limit=50` → `TriggerFireResponse`: `fire_key`（`cron:<scheduled_at>` / `event:<event_id>`）、`scheduled_at` または `event_id`、`outcome`（`accepted` = invocation を受け付けた / `refused` = 恒久的に拒否、`reason` 付き）、`invocation_id`、`created_at`。

他 tenant の function・trigger はどの操作でも 404。作成・更新・削除は `deploy`、読み取りは `deploy` / `invoke` / `operator`。

#### 5.11.2 webhook の配信

```http
POST /v1/hooks/trg_01j7z3a4b5c6d7e8f9g0h1j2k3
content-type: application/json
x-tachyon-webhook-timestamp: 1789630500
x-tachyon-webhook-signature: v1=3b0d...(64 hex)
x-tachyon-webhook-id: evt_123

{"order":42}
```

- 署名: `v1=` + hex(`HMAC-SHA256(key = secret 文字列の UTF-8, message = "{timestamp}.{raw body}")`)。`,` 区切りで複数の `v1=` を送れる（どれか 1 つが一致すればよい）。`printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET"`、または `tsls triggers webhook-sign`。
- `timestamp` は 10 進の Unix 秒。gateway の時計との差が `tolerance_seconds` を超えれば（過去・未来とも）401。

```json
{ "trigger_id": "trg_01j7z3a4b5c6d7e8f9g0h1j2k3", "event_id": "evt_123", "invocation_id": "inv_01j7z4...", "status": "accepted", "replayed": false }
```

| 状況 | 応答 | 記録 |
|---|---|---|
| 未知・削除済み・cron の trigger id、形式不正の id | 404 `not_found`（同じ本文） | なし |
| body が `max_body_bytes` 超過（`Content-Length` なら読まずに、無ければ読みながら打ち切る） | 413 `payload_too_large` | なし |
| timestamp が無い・不正・tolerance 外 | 401 `unauthorized` | なし |
| 署名が無い・不正・不一致（別 secret、body や timestamp の改変） | 401 `unauthorized` | なし |
| 署名は正しいが trigger が無効 | 410 `gone` | なし |
| 署名は正しいが event id が無い・不正（1..=128 の可視 ASCII） | 400 `invalid_request` | なし |
| 受付 | 202、`replayed = false`、`x-tachyon-invocation-id` | invocation・入力・outbox event・fire 行 |
| 同じ event id の再送（署名し直し・body が違っても） | 202、**同じ `invocation_id`**、`replayed = true` | 何も増えない |
| 同じ署名済み request を別の event id で再送 | 202、同じ `invocation_id`、`replayed = true`、元の `event_id` | 何も増えない |
| 受付の拒否（function 削除、backlog、queue、store） | §5.6.1 と同じ | なし |

関数に渡る JSON: `{"source":"tachyon.webhook","trigger_id","trigger_name","event_id","content_type","body":<JSON>}`（JSON でなければ `body_text`、UTF-8 でなければ `body_base64`）。cron は `{"source":"tachyon.cron","trigger_id","trigger_name","scheduled_at","timezone","payload"}`。どちらも invocation の Idempotency-Key は `cron:{trigger_id}:{scheduled_at}` / `webhook:{trigger_id}:{event_id}`。

event id の dedup は `[triggers] webhook_dedup_retention_seconds`（既定 7 日）保持する。

#### 5.11.3 cron の発火と missed run

scheduler は gateway の中で `[triggers] scheduler_interval_ms`（既定 1000）ごとに回り、同じ `state.db` の gateway のうち scheduler lease を持つ 1 つだけが発火する（owner は dispatcher id）。予定時刻から `grace_seconds`（既定 30）以内は on time、それより遅いものは late:

| policy | late の時刻 | on time の時刻 |
|---|---|---|
| `skip` | 実行しない | 実行 |
| `run_once` | 最新の 1 つを実行 | 実行 |
| `run_all {max_runs}` | 新しい方から `max_runs` 個を古い順に実行 | 実行 |

`max_catchup_seconds`（既定 24 時間）より古い時刻は実行しない。受付が一時的に拒否された時刻（`backlog`、queue、設定 cache、store）は次の pass で再試行し、恒久的に拒否された時刻（function 削除、alias / revision 無し、revision 未 ready、policy）は `refused` として記録して進む。function が削除されていれば trigger を無効化する（`status_reason = function_deleted`）。

同じ予定時刻の fire 行は 1 つだけ（再起動・2 つの scheduler・fire と cursor 更新の間の crash のどれでも）。

### 5.12 snapshot（実験 X1、PLT-4653）

`SnapshotResponse`:

```json
{
  "id": "snap_01j...", "function_id": "fn_01j...", "revision_id": "rev_01j...",
  "state": "active", "manifest_digest": "sha256:...", "manifest_version": 1,
  "provider": "firecracker", "memory_mib": 256, "vcpus": 1,
  "source_environment_id": "env_01j...", "restores": 2,
  "created_at": "2026-09-17T12:00:00Z", "expires_at": "2026-09-17T13:00:00Z",
  "timings": {"pause_ms": 3, "create_ms": 250, "copy_ms": 40, "seal_ms": 900}
}
```

- `state_reason`: `revoked` / `quarantined` の理由（例 `artifact memory digest mismatch`、`stale: key_generation_changed`）。
- restore の拒否理由の code（`Host.RestoreRequiredUnavailable` の message と `restore_fallback`）: `not_configured`、`capability_unverified` / `capability_unsupported`、`revision_not_eligible`、`secret_bindings`、`no_snapshot`、`tenant_mismatch`、`revision_mismatch`、`spec_digest_mismatch`、`runtime_mismatch`、`host_cpu_mismatch`、`device_model_mismatch`、`memory_mismatch`、`vcpu_mismatch`、`storage_mismatch`、`network_mismatch`、`egress_unsupported`、`key_generation_changed`、`secret_generation_changed`、`lifecycle_version_mismatch`、`expired`、`revoked`、`quarantined`、`manifest_invalid`、`artifact_corrupted`、`clone_failed`、`cold_boot_detected`、`not_the_source`、`reconnect_timeout`、`reconnect_failed`、`init_failed`。
- snapshot の中身（memory・vmstate・scratch・function drive）は API から取得できない。

### 5.7 HTTP アダプタ

`ANY /v1/functions/{function_id}/http/{*path}`。gateway はリクエストを `tachyon.http.v1` event（`HttpRequestEvent`: method / path / query / headers / body_base64）に変換して関数に渡し、`HttpResponsePayload`（status / headers / body）をそのまま HTTP 応答にする。関数が 404 を返せば 404 が返る。gateway 側のエラー（関数が無い、init 失敗など）だけが §4 の JSON 本文になる。

event の `path` は `/http` より後ろの request-target path を **受け取ったまま**（percent-encoded のまま、decode せず、query を除く）渡す。例: `/http/a%2Fb?x=1` → `path = "/a%2Fb"`、`query = "x=1"`。decode は関数側の router が 1 回だけ行う。

### 5.8 Invocation

`GET /v1/invocations/{invocation_id}` → `InvocationResponse`:

```json
{
  "id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2",
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "revision_id": "rev_01j7z0m1n2p3q4r5s6t7v8w9x0",
  "alias": "prod",
  "alias_generation": 3,
  "mode": "sync",
  "status": "succeeded",
  "output": { "message": "hello, demo", "greeting": "v1", "secret_present": true },
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "input_digest": "sha256:5f3d2e0f6a1b4c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d",
  "input_size_bytes": 15,
  "accepted_at": "2026-09-15T01:10:00.000Z",
  "started_at": "2026-09-15T01:10:00.020Z",
  "finished_at": "2026-09-15T01:10:00.350Z",
  "deadlines": {
    "queue_deadline": "2026-09-15T01:10:10.000Z",
    "init_deadline": "2026-09-15T01:10:30.020Z",
    "execution_deadline": "2026-09-15T01:10:30.310Z",
    "client_deadline": "2026-09-15T01:11:10.000Z"
  },
  "attempts": [
    {
      "id": "att_01j7z2k3m4n5p6q7r8s9t0v1w3",
      "number": 1,
      "environment_id": "env_01j7z2k3m4n5p6q7r8s9t0v1w4",
      "epoch": 1,
      "status": "succeeded",
      "start_kind": "cold",
      "timings": {
        "queue_wait_ms": 1,
        "environment_boot_ms": 180,
        "runtime_init_ms": 95,
        "handler_ms": 12,
        "response_ms": 1,
        "total_ms": 330
      },
      "boot_evidence": {
        "guest_boot_id": "6b1e7a2c-2c2b-4a4e-9f3f-0d3f3c1a2b3c",
        "host_pid": 48213,
        "details": { "vmm": "firecracker v1.10.1", "kernel_digest": "sha256:..." }
      },
      "dispatched_at": "2026-09-15T01:10:00.295Z",
      "finished_at": "2026-09-15T01:10:00.310Z"
    }
  ]
}
```

- `status`: `accepted` | `queued` | `running` | `succeeded` | `failed` | `cancelled` | `outcome_unknown`。失敗時は `"error": {"class": "user_error", "error_type": "Handler.Error", "message": "..."}` が付き、`output` は無い。
- `attempts[].boot_evidence`: 実際に環境が起動した証跡。`host_pid` は VMM / 子プロセスの pid、`guest_boot_id` は guest の `/proc/sys/kernel/random/boot_id`（process provider では `null`）。
- `timings` はすべて host 側計測（guest の自己申告は根拠にしない）。

一覧 `GET /v1/functions/{function_id}/invocations?limit=20` は `{"items": [InvocationResponse...], "next_cursor": ...}`。

### 5.9 Logs

`GET /v1/invocations/{invocation_id}/logs` → `LogsResponse`:

```json
{
  "items": [
    { "timestamp": "2026-09-15T01:10:00.100Z", "stream": "platform", "phase": "boot", "environment_id": "env_01j7z2k3m4n5p6q7r8s9t0v1w4", "line": "bridge connected", "truncated": false },
    { "timestamp": "2026-09-15T01:10:00.200Z", "stream": "stderr", "phase": "init", "environment_id": "env_01j7z2k3m4n5p6q7r8s9t0v1w4", "line": "hello: starting", "truncated": false },
    { "timestamp": "2026-09-15T01:10:00.300Z", "stream": "stdout", "phase": "handler", "environment_id": "env_01j7z2k3m4n5p6q7r8s9t0v1w4", "invocation_id": "inv_01j7z2k3m4n5p6q7r8s9t0v1w2", "attempt_id": "att_01j7z2k3m4n5p6q7r8s9t0v1w3", "line": "handler: name=demo", "truncated": false }
  ],
  "dropped": false
}
```

`stream`: `stdout` | `stderr` | `platform`。`phase`: `boot` | `init` | `handler` | `shutdown`。1 行の上限を超えた行は `truncated: true`、invocation 単位の行数 / bytes 上限で捨てられた行があれば `dropped: true`。

### 5.10 Usage

`GET /v1/functions/{function_id}/usage` → `UsageSummaryResponse`:

```json
{
  "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
  "invocations": 12,
  "succeeded": 9,
  "failed": 3,
  "handler_ms_total": 1830,
  "environment_ms_total": 6120,
  "bytes_in_total": 240,
  "bytes_out_total": 1024,
  "not_billable": true
}
```

`not_billable` は常に true（数値は使用量の事実であって請求ではない）。

### 5.10.1 `GET /v1/usage` → `UsageReportResponse`（PLT-4642）

token の tenant の、host が測った利用量を version 付き価格表で集計した**仮**の報告（`docs/adr/0012-usage-ledger-and-rating.md`）。**請求書ではない**: `provisional` と `not_an_invoice` は常に true、`billing_enabled` は常に false。collector が ledger に運んだ event だけが入る（`collected_through`）。

```json
{
  "provisional": true,
  "not_an_invoice": true,
  "billing_enabled": false,
  "notice": "provisional usage estimate: not an invoice, nothing is charged, billing is disabled in this prototype",
  "tenant_id": "tn_01hzzzzzzzzzzzzzzzzzzzzzza",
  "from": "2026-09-16T00:00:00Z",
  "to": "2026-09-18T00:00:00Z",
  "group_by": ["function"],
  "price_table": {
    "version": "provisional-dev-2026-09-v1",
    "effective_from": "2026-09-01T00:00:00Z",
    "currency": "JPY",
    "billable_segments": ["user_init_ms", "handler_ms"],
    "unit_prices_micros": {"vcpu_second": 2500, "gib_second": 400, "invocation": 30, "gb_transferred": 15000000},
    "rounding": ["each segment: whole milliseconds rounded up from the host monotonic clock", "..."]
  },
  "lines": [
    {
      "function_id": "fn_01j7z0a1b2c3d4e5f6g7h8j9k0",
      "usage": {
        "invocations": 5, "attempts": 5, "retries": 0,
        "outcomes": {"succeeded": 4, "failed": 0, "timeout": 1, "cancelled": 0, "outcome_unknown": 0},
        "segments_ms": {"queue_wait_ms": 2, "vm_base_boot_ms": 610, "user_init_ms": 1260, "handler_ms": 5215, "teardown_ms": 95, "idle_pooled_ms": 0},
        "billable_ms": 6475, "vcpu_milli_ms": 3237500, "mib_ms": 1657600,
        "request_bytes": 71, "response_bytes": 312
      },
      "unmetered": {"attempts": 0, "segments": {"queue_wait_ms": 0, "vm_base_boot_ms": 0, "user_init_ms": 0, "handler_ms": 0, "teardown_ms": 0, "idle_pooled_ms": 0}, "bytes": 0},
      "cost": {"environments_stopped": 5, "environment_lifetime_ms": 7210, "idle_pooled_ms": 0, "teardown_ms": 95, "boot_without_attempt_ms": 0, "cgroup_cpu_usec": 0, "cgroup_cpu_unknown": 5, "cgroup_memory_peak_bytes_max": 0},
      "provisional_charges_micros": {"vcpu": 8094, "memory": 647, "invocations": 150, "transfer": 6, "total": 8897},
      "guest_reported": {"guest_handler_ms": 5190, "guest_init_ms": 5}
    }
  ],
  "totals": {"usage": {"...": "sum of lines"}, "provisional_charges_micros": {"total": 8897}},
  "unjournaled_events": 0,
  "collected_through": "2026-09-17T07:20:31.512Z"
}
```

| 欄 | 意味 |
|---|---|
| `usage` | **利用量**。課金対象の event（`AttemptSettled`）の、`host_measured` / `provider_reported` の量だけ。`invocations` は初回 attempt の数、`retries` は同じ invocation の 2 回目以降。`billable_ms` は価格表の `billable_segments` の和、`vcpu_milli_ms` / `mib_ms` はそれに要求 resource を掛けた値 |
| `provisional_charges_micros` | **仮料金**（通貨の 10⁻⁶ 単位の整数）。行・成分ごとに `round_half_up(量 × 単価 / 単位)`、`total` は成分の和。`totals` は行の和で再丸めしない |
| `cost` | **原価**の事実（環境寿命、pool の idle、teardown、attempt を持たなかった boot / init、cgroup CPU / peak memory）。価格を掛けない |
| `unmetered` / `unjournaled_events` | 測れなかった区間・bytes を持つ attempt の数と、journal に入らなかった event の数。0 として扱い、推測しない |
| `guest_reported` | guest の自己申告（参考値）。価格を掛けない |

- 他 tenant の function を `function_id` に指定しても空の報告（存在を明かさない）。operator role は 403。
- `from` の既定は `to` の 31 日前（価格表の `effective_from` より前には伸ばさない）。範囲は最大 92 日、`from` が `effective_from` より前なら 400。日付は event の host wall clock（UTC）。
- `/readyz` の `usage`: `accepting` / `metered` / `policy` / `billing_enabled` / `price_table_version`、`journal`（`healthy`、`pending_events` / `pending_bytes`、`cursor_seq`、`limits`、`admitting`、`unjournaled_events`、`last_error`）、`collector`（`runs`、`last_success_at`、`last_error`、`delivered` / `inserted` / `duplicates`）、`ledger`（`events`、`duplicates_ignored`）。tenant の情報は含まない。

### 5.10.2 `GET /v1/budget` → `BudgetReportResponse`（PLT-4643）

token の tenant の、1 期間（UTC の暦月）の予算（`docs/adr/0016-budget-reservation-and-admission.md`）。金額は PLT-4642 の価格表の通貨の 10⁻⁶ 単位で、**仮料金**。`provisional` は常に true、`billing_enabled` は常に false。予算の設定は control plane から設定 cache で配信され（§7、`ConfigKey::Budget`、auth lease で失効）、予約・精算は `<data_dir>/usage/budget.db`。

```json
{
  "enabled": true,
  "provisional": true,
  "billing_enabled": false,
  "notice": "provisional budget: amounts are the PLT-4642 provisional rating, not an invoice; nothing is charged, billing is disabled in this prototype",
  "tenant_id": "tn_01hzzzzzzzzzzzzzzzzzzzzzza",
  "period": "2026-09",
  "period_kind": "calendar_month_utc",
  "period_start": "2026-09-01T00:00:00Z",
  "period_end": "2026-10-01T00:00:00Z",
  "currency": "JPY",
  "price_table_version": "provisional-dev-2026-09-v1",
  "config_state": "valid",
  "config_generation": 12,
  "admitting": true,
  "tenant": {
    "soft_limit_micros": 15717,
    "alert_thresholds_percent": [50, 100],
    "hard_limit_micros": 526857,
    "reserved_micros": 146040,
    "settled_micros": 19911,
    "unmetered_hold_micros": 0,
    "committed_micros": 165951,
    "remaining_micros": 360906,
    "overrun_micros": 0,
    "active_reservations": 1,
    "reservations": 5, "settlements": 4, "releases": 0, "expiries": 0, "refusals": 5,
    "alerts_fired": [{"threshold_percent": 50, "soft_limit_micros": 15717, "consumed_micros": 19911, "fired_at": "2026-09-17T09:28:41Z"}]
  },
  "functions": [],
  "guarantee": ["covers only the provisional rating of PLT-4642: ...", "..."]
}
```

| field | 意味 |
|---|---|
| `enabled` | この gateway が予算を強制するか（`[budget] enabled`） |
| `config_state` / `config_generation` | 配信された予算の状態（`valid` / `expired`（auth lease 切れ）/ `not_delivered`（未配信・削除））と generation |
| `admitting` / `refusal` | 予算の面で新規 invoke を受け付けるか、受け付けないならその `error_type`（function ごとの上限は含まない） |
| `soft_limit_micros` / `alert_thresholds_percent` | **alert** の設定。消費（settled + unmetered hold）が閾値を初めて越えたとき 1 回ずつ `alerts_fired` に載る。何も止めない |
| `hard_limit_micros` | **停止**の設定。`committed` + 次の run の最大料金がこれを超えるなら 429 `budget_exhausted`。無ければ止めない |
| `reserved_micros` | 未精算の run が予約している**最大料金**の合計（timeout・初期化 timeout・要求 vCPU / memory・課金区間・最大 response size から計算） |
| `settled_micros` | 精算済み run の仮料金（ledger の `AttemptSettled` を rating した値。run ごとに丸めるので `GET /v1/usage` とは run 1 件あたり数 micro-unit 以内でずれうる） |
| `unmetered_hold_micros` | 計測しきれなかった run の予約の残り（終わりを報告しなかった run は最大額、journal に入らなかった・unknown の区間があった run は差額）。上限には数えるが**課金額ではない** |
| `committed_micros` / `remaining_micros` | `reserved + settled + unmetered_hold`、`hard_limit - committed`（0 で止まる。hard limit が無ければ省略） |
| `overrun_micros` | 実測が予約を超えた額（全額精算）。確約が上限を超えるのはこの分だけ |
| `functions[]` | 予算のある function と、期間内に予約のあった function。同じ欄 |
| `guarantee` | 金額が保証する範囲と保証しない範囲（ADR-0016 §7） |

- operator role は 403。他 tenant の予算・function は含まれない（`period` 以外の指定は無い）。
- 予約は run（同期 invoke は invocation）ごとに 1 つ。queue で待った invocation は grant の時点で予算を再確認する。
- `/readyz` の `budget`: `enabled`、`accepting`、`refusal`、`period`、`price_table_version`、`store`（`healthy`、`durable`、`path`、`stats`（`active_reservations`、`finished_unsettled`、`oldest_finished_unsettled_at`）、`last_error`）、`collector_stalled`、`max_unsettled_age_seconds`、`oldest_unsettled_age_seconds`、`expiry_grace_seconds`、このプロセスの `counters`、`publication`（control plane: file、再読込回数、最後のエラー）。tenant の情報は含まない。

## 6. invoke のステータス早見表

| 状況 | HTTP | `error.code` |
|---|---|---|
| 成功 | 200 | — |
| alias / revision が ready でない | 409 | `revision_not_ready` |
| Function 削除済み（新規、`Idempotency-Key` の再送、削除時に待機中だったもの） | 409 | `function_deleted`（`Host.FunctionDeleted`。待機中だったものは invocation に `Failed` として残り attempt なし） |
| alias 切替・削除の drain 中に `drain_timeout_seconds` を超えて実行中 | 504 | `timeout`（`Host.DrainTimeout`） |
| payload 上限超過 | 413 | `payload_too_large` |
| invokeAsync: 受付を確定できない（outbox 上限、queue 停止で outbox 満杯、object store、未設定） | 429 / 503 / 413 | `capacity_exceeded` / `async_unavailable` / `payload_too_large`（`reason`、§5.6.1） |
| queue 溢れ（件数 / bytes / tenant の持ち分）、node に収まらない環境 | 429 | `capacity_exceeded`（`reason` = `queue_full` / `quota` / `capacity`） |
| queue deadline 超過 | 504 | `queue_timeout`（`reason` = `capacity` / `quota` / `queue_deadline`） |
| 配置制約（jp-only など）を満たさない node | 503 | `provider_unavailable`（`reason = placement`、invocation を作らない） |
| revision の起動失敗が続き breaker が open | 503 | `provider_unavailable`（`reason = circuit_open`） |
| init 失敗 / init timeout | 502 | `init_error` |
| secret binding を解決できない（他 tenant の binding と存在しない binding は同じ応答。環境は作らない） | 502 | `init_error`（`Host.SecretBindingUnavailable`） |
| handler が Err | 502 | `user_error` |
| panic / crash | 502 | `crash` |
| Invoke を届ける前に user process が終了 / bridge が切断（handler は未開始） | 502 | `crash`（`Runtime.Exited` / `Host.BridgeDisconnectedBeforeInvoke`）。再利用環境（warm）で起きた場合は同じ分類の attempt を 1 行残し、cold で 1 回だけやり直すので invocation 自体はやり直しの結果になる（`docs/architecture.md` §4） |
| 実行 deadline 超過 | 504 | `timeout` |
| client deadline 到達（handler 起動前なら起動しない、実行中なら Cancel） | 504 | `timeout`（`Host.ClientDeadline`） |
| cancel API | 499 | `cancelled` |
| 結果不明（Invoke 送信後の接続断） | 502 | `outcome_unknown` |
| provider / 内部 | 500 / 503 | `platform_error` / `provider_unavailable` |
| 設定が未配信 / TTL 切れ / 認可 lease 切れ（受付前、台帳に残らない） | 503 | `config_unavailable`（`Host.ConfigNotDelivered` / `Host.ConfigExpired` / `Host.AuthLeaseExpired`） |
| grant の tenant が未知 / egress が policy 外 | 403 | `forbidden`（`Host.UnknownTenant` / `Host.PolicyDenied`） |
| control plane 到達不能かつ `allow_cold_start = false` で cold start が要る | 503 | `config_unavailable`（`Host.ColdStartRestricted`。再利用 off なら受付前、on なら warm を試した後に `Failed` として記録） |
| provider の制御 API が停止（preflight 失敗）で cold start が要る | 503 | `provider_unavailable`（`Host.ProviderControlUnavailable`） |
| data plane の gateway に管理 API を送った | 503 | `control_plane_unavailable`（`Host.ControlPlaneUnavailable`） |

## 7. 設定配信・認可 lease・control plane 停止（PLT-4636）

決定は `docs/adr/0007-config-distribution-and-auth-leases.md`、構成は `docs/architecture.md` §4「設定配信と認可 lease」。

### 7.1 役割

| `[control_plane] role` | 提供するもの |
|---|---|
| `combined`（既定） | 管理 API、invoke、`internal_token` を設定したときだけ `GET /v1/internal/config` |
| `data_plane` | invoke、HTTP アダプタ、`/v1/invocations/*`、`/v1/provider`、`/readyz`。管理 API はすべて 503 `control_plane_unavailable`。`/v1/internal/config` は 404 |

data plane は `[[identity.tokens]]` を持てない（token は control plane から grant として届く）。secret binding の値は data plane 自身の `[[secrets.bindings]]` から解決する（配信されるのは binding の参照だけ）。

### 7.2 `GET /v1/internal/config?since=<generation>`

`Authorization: Bearer <internal_token>`（tenant の token は 401）。`since` より大きい generation の entry をすべて返し、「ここに無い entry は `generation` の時点で変わっていない」ことを主張する。

```json
{
  "source": "management",
  "generation": 15,
  "since": 12,
  "config_ttl_seconds": 60,
  "auth_lease_seconds": 60,
  "entries": [
    {"key": {"kind": "route", "function_id": "fn_...", "alias": "prod"}, "generation": 13,
     "value": {"kind": "route", "value": {"function_id": "fn_...", "name": "prod", "revision_id": "rev_...", "generation": 2, "...": "..."}}},
    {"key": {"kind": "grant", "token_digest": "5bdc...43"}, "generation": 15, "value": null}
  ]
}
```

- key の `kind`: `function` / `route` / `revision` / `grant`（`token_digest` = HMAC-SHA256(internal_token, bearer token) の hex）/ `tenant` / `policy` / `budget`（`tenant_id`、tenant の予算。auth lease で失効、PLT-4643 §5.10.2）。
- `value: null` は tombstone（削除・revoke）。
- generation は control plane の `state.db` に刻まれ、再起動をまたいで単調に増える。

### 7.3 data plane の判断

| 状況 | invoke の応答 | `/readyz` の `control_plane` |
|---|---|---|
| 一度も配信を受けていない | 503 `Host.ConfigNotDelivered` | `new_invocations: refused`、`refusal: Host.ConfigNotDelivered` |
| control plane に届く | 通常どおり（deploy / rollback の反映は最大 `refresh_interval_ms`） | `control_plane_reachable: true` |
| 届かない、TTL 内 | 通常どおり（cache から解決） | `control_plane_reachable: false`、`new_invocations: accepted` |
| 届かない、`config_ttl_seconds` 超過 | 503 `Host.ConfigExpired`（受付前） | 503、`refusal: Host.ConfigExpired`、`existing_executions: continue` |
| 届かない、`auth_lease_seconds` 超過 | 503 `Host.AuthLeaseExpired`（認証時） | 503、`refusal: Host.AuthLeaseExpired` |
| 届かない、`[control_plane_outage] allow_cold_start = false` | cold start が要るものだけ 503 `Host.ColdStartRestricted`。warm 環境で受けられるものは継続 | `new_cold_starts: refused` |
| provider の preflight が失敗 | cold start が要るものだけ 503 `Host.ProviderControlUnavailable` | `new_cold_starts: refused`（`ready: false`） |
| 回復 | 最初の成功した refresh で最新 generation に収束。dispatcher lease を即座に再確認 | `cache.reconnects` が増える |

いずれの場合も **実行中の invocation は止めない**（`existing_executions: "continue"`）。`control_plane.cache` には `generation`、`last_success_at`、`consecutive_failures`、`last_error`、`config_valid_until`、`auth_valid_until`、`ignored_older_entries`、`ignored_regressed_deliveries` が入る。

revoke の遅延上限: control plane に届く data plane では 1 refresh、届かない data plane では最大 `auth_lease_seconds`。

## 8. スケール to zero・min_ready・drain（PLT-4635）

決定と理由は `docs/adr/0009-scale-to-zero-and-drain.md`。

- **zero**: `min_ready = 0`（既定）の revision は、pool の idle 環境が `idle_ttl` と cooldown を過ぎ、待機中の invocation も約束も無ければ scale reconciler（既定 1 s ごと）が終わらせ、環境数 0 になる。次の invoke は cold start（`attempts[].start_kind = "cold"`）。**環境数 0 は host 費用 0 ではない**（gateway・store・node は動き続ける。`GET /v1/capacity` の `scaling.at_zero`）。pool が無い gateway（`scaling.warm_pool = false`）では環境は invocation と一緒に終わる。
- **0 からの burst**: 起動は `desired`・`max_concurrency`・quota・node 容量・start rate の範囲でだけ行い、残りは待つ（§4 の `reason`）。最初の応答は cold start ぶん遅い。
- **route**: alias は受付時に 1 回だけ解決し、`InvocationResponse.alias_generation` に記録する。受付後の alias 切替で実行中・待機中の invocation の revision は変わらない。
- **drain**: alias が別 revision に移ると、旧 revision の idle 環境は次の reconcile で終わり、実行中の環境は完了後に pool へ戻らない（`route_state = superseded`）。secret の値が変わった revision では、古い世代の idle 環境が次の reconcile で終わる。削除は `deleting` → 待機中を 409 → 実行中の完了 → `deleted`。drain の開始から `drain_timeout_seconds` を過ぎて実行中の invocation は 504 `Host.DrainTimeout`。既定の drain timeout はどの revision の timeout より長いので、既定の設定では drain が自分の timeout の内側にいる invocation を止めることはない（alias 切替・secret 世代変更・削除のどれでも同じ）。
- **振動しない**: cooldown、待機者・約束の優先、先行起動の backoff。control plane に届かず設定が期限切れの間は route の観測を保持し、drain も先行起動の取り消しもしない。

gateway 設定 `[scaling]`: `reconcile_interval_ms`（既定 1000）、`scale_down_cooldown_seconds`（30）、`drain_timeout_seconds`（省略時は `limits.max_execution_timeout_seconds` + cancel grace + 60 s。既定の limits では 961。自分の timeout の内側にいる invocation を drain が止めないための値で、これ以下を設定するには `allow_short_drain = true` が要る）、`prestart_backoff_seconds`（5）。`GET /v1/capacity` の `scaling.drain_timeout_seconds` は実際に使う値。
