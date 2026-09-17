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
| `idempotency-key` | req (invoke) | 1..=256 文字。scope は `(tenant, function, key)`。同キー・同 input digest なら既存 Invocation を返す（容量が満杯でも、実行中でも、完了後でも。§5.6「Idempotency-Key」）。同キー・異なる digest → 409 `conflict`（本文は §4）。受付前に拒否された request（400 / 413 / 429）は key を消費しない。Invocation が terminal になってから `[store] idempotency_retention_seconds`（既定 24 時間）で失効する |
| `x-tachyon-client-timeout-ms` | req (invoke) | クライアント側の全体 deadline（相対 ms）。revision の timeout + init + queue で上限が掛かる。queue / init / execution の各 deadline はこれを超えず、handler の起動前に過ぎれば handler を起動しない（504 `timeout`、`Host.ClientDeadline`） |
| `x-request-id` | req/res | リクエスト id（省略時は gateway が採番） |
| `x-tachyon-invocation-id` | res (invoke/http) | 受け付けた Invocation の id |
| `x-tachyon-trace-id` | req/res (invoke/http) | trace id（request では任意、256 bytes 以下。超過は 400） |

## 3. エンドポイント一覧

| Method | Path | 役割 | 200 系 | 主なエラー |
|---|---|---|---|---|
| GET | `/healthz` | liveness | 200 | — |
| GET | `/readyz` | readiness（provider preflight OK、dispatcher lease が有効、かつ新しい invocation を受け付ける。本文に `dispatcher: {id, instance, fenced}` と `control_plane`（§7）） | 200 | 503 |
| GET | `/v1/provider` | provider 種別 / isolation / capability 表 / preflight | 200 `ProviderInfo` | — |
| POST | `/v1/artifacts` | 実行ファイルの生バイト (`application/octet-stream`) を upload → digest | 200 `ArtifactUploadResponse` | 401, 413 `payload_too_large` |
| POST | `/v1/functions` | Function 作成 | 201 `FunctionResponse` | 400 `invalid_request`, 409 `conflict`（同名） |
| GET | `/v1/functions` | Function 一覧（テナント内） | 200 `ListResponse<FunctionResponse>` | — |
| GET | `/v1/functions/{function_id}` | 取得 | 200 `FunctionResponse` | 404 |
| DELETE | `/v1/functions/{function_id}` | 削除（新規 invoke を止める） | 200/204 | 404 |
| POST | `/v1/functions/{function_id}/revisions` | Revision 作成（非同期 validation） | 202 `RevisionResponse` (`pending`) | 400, 404 |
| GET | `/v1/functions/{function_id}/revisions` | 一覧 | 200 `ListResponse<RevisionResponse>` | 404 |
| GET | `/v1/functions/{function_id}/revisions/{revision_id}` | 取得（`ready` / `failed` を poll する） | 200 `RevisionResponse` | 404 |
| PUT | `/v1/functions/{function_id}/aliases/{alias}` | alias を CAS 更新 | 200 `AliasResponse` | 404, 409 `conflict`（generation 不一致）, 409 `revision_not_ready` |
| GET | `/v1/functions/{function_id}/aliases/{alias}` | 取得 | 200 `AliasResponse` | 404 |
| GET | `/v1/functions/{function_id}/aliases` | 一覧 | 200 `ListResponse<AliasResponse>` | 404 |
| POST | `/v1/functions/{function_id}:invoke`<br>`/v1/functions/{function_id}/invoke` | 同期 JSON invoke（query: `alias`, `revision_id`） | 200 handler の出力 JSON | 下記 §6 |
| ANY | `/v1/functions/{function_id}/http/{*path}` | HTTP アダプタ。リクエストを `tachyon.http.v1` event に包んで実行し、関数の status / headers / body をそのまま返す | 関数が返した status | 下記 §6（本文が `ApiErrorBody` の時だけ platform エラー） |
| GET | `/v1/functions/{function_id}/invocations` | 履歴（query: `limit`, `cursor`） | 200 `ListResponse<InvocationResponse>` | 404 |
| GET | `/v1/invocations/{invocation_id}` | Invocation 詳細（attempts, timings, boot_evidence 含む） | 200 `InvocationResponse` | 404 |
| POST | `/v1/invocations/{invocation_id}:cancel`<br>`/v1/invocations/{invocation_id}/cancel` | 実行中の Invocation を cancel | 200 `InvocationResponse` (`cancelled`) | 404, 409（既に terminal） |
| GET | `/v1/invocations/{invocation_id}/logs` | ログ（invocation 単位、行数 / bytes 上限あり） | 200 `LogsResponse` | 404 |
| GET | `/v1/functions/{function_id}/usage` | 使用量集計（課金ではない） | 200 `UsageSummaryResponse` | 404 |
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

`invocation_id` と `error_type` は invoke 系の失敗でのみ入る（履歴・ログを引くために使う）。例外は `Idempotency-Key` の衝突で、409 `conflict` に key が結び付いている Invocation の id と `error_type = "Host.IdempotencyKeyReused"` が入る:

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
| `capacity_exceeded` | 429 | queue も満杯 | 2 |
| `revision_not_ready` | 409 | alias / revision が `ready` でない | 2 |
| `function_deleted` | 409 | 削除済み Function への invoke | 2 |
| `user_error` | 502 | handler が `Err` を返した（`Handler.Error`） | 3 |
| `crash` | 502 | panic / プロセス異常終了（`Runtime.Panic`, `Runtime.Crash`） | 3 |
| `init_error` | 502 | Ready 前に失敗 / init timeout | 3 |
| `timeout` | 504 | 実行 deadline 超過（host が環境を終了） | 4 |
| `queue_timeout` | 504 | queue deadline まで空きが出なかった | 4 |
| `cancelled` | 499 | cancel API による中断 | 3 |
| `outcome_unknown` | 502 | 結果を確認できない（自動再実行しない） | 5 |
| `platform_error` | 500 | provider / bridge / 内部エラー | 6 |
| `provider_unavailable` | 503 | provider が使えない（例: `/dev/kvm` 無し）、shutdown 中、dispatcher lease を失った gateway（別の gateway に送り直す）、または provider の制御 API が preflight に失敗していて新しい環境を起動できない（`Host.ProviderControlUnavailable`。実行中と warm 環境は継続） | 6 |
| `config_unavailable` | 503 | 設定 cache が新しい仕事を保証できない（PLT-4636、§7）。`error_type`: `Host.ConfigNotDelivered`（未配信）、`Host.ConfigExpired`（TTL 切れ）、`Host.AuthLeaseExpired`（認可 lease 切れ）、`Host.ColdStartRestricted`（control plane 到達不能で cold start を制限中） | 6 |
| `control_plane_unavailable` | 503 | 管理 API を提供できない。`Host.ControlPlaneUnavailable`（data plane の gateway）、`Host.StoreUnavailable`（台帳 store が応答しない） | 6 |

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

削除後は `deleted_at` が付く。一覧は `{"items": [...], "next_cursor": "..."}`（`next_cursor` は続きがある時だけ）。

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
- `egress`: `none`（既定。NIC なし）/ `restricted` / `public-web`。`egress_allow` は `restricted` のときだけ必須（1..=16 件）で、各要素は `{"cidr": "1.1.1.1/32", "protocol": "tcp", "ports": [443]}`（`protocol` は `tcp` 既定 / `udp`、`ports` は 1..=16 件）。IPv4 CIDR のみで、0/8・10/8・100.64/10・127/8・169.254/16・172.16/12・192.168/16 などの special-purpose 範囲と重なるものは 400。どの profile でも管理網・node・metadata・private 範囲・IPv6 には届かない（`docs/adr/0005-egress-profiles.md`）。spec の `egress_allow` は空なら省略される。
- `publish_to_prod`（既定 true）: `ready` になった時点で alias `prod` を向ける。

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

## 6. invoke のステータス早見表

| 状況 | HTTP | `error.code` |
|---|---|---|
| 成功 | 200 | — |
| alias / revision が ready でない | 409 | `revision_not_ready` |
| Function 削除済み | 409 | `function_deleted` |
| payload 上限超過 | 413 | `payload_too_large` |
| queue 溢れ | 429 | `capacity_exceeded` |
| queue deadline 超過 | 504 | `queue_timeout` |
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

- key の `kind`: `function` / `route` / `revision` / `grant`（`token_digest` = HMAC-SHA256(internal_token, bearer token) の hex）/ `tenant` / `policy`。
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
