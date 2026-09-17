# ADR-0007: 設定は generation 付きで配信し、data plane は期限付き cache と認可 lease だけで invoke を受け付ける

## ステータス

Accepted（2026-09-17、PLT-4636）。実装: `crates/application/src/control/`（`wire.rs` 配信の形、`source.rs` `ConfigSource` と ledger からの publication、`cache.rs` `ConfigCache`、`gate.rs` `InvokeGate`）、`crates/application/src/repository/config.rs` と migration `004_config_publication.sql`（generation の刻印）、`apps/gateway/src/config_client.rs`（HTTP の `ConfigSource`）、`apps/gateway/src/handlers.rs::internal_config`。検証: `crates/application/tests/config_cache.rs`、`apps/gateway/tests/gateway_integration.rs::a_data_plane_gateway_serves_invokes_from_delivered_configuration`、`scripts/control-plane/outage-e2e.sh`（`docs/evidence/20260917T045347Z-split-process/`）。予算 token との接続は P3（PLT-4643）で扱い、本 ADR の範囲外。

## コンテキスト

- PLT-4636 は「管理 API への毎回依存を避けつつ、失効した権限で無期限に実行しない」ことを求める。受入条件は (1) 有効期限内の配信済み設定だけで既存環境への Invoke を続けられる、(2) 認可 lease 期限切れ・不明な tenant・未配信版は新規受付を拒否する、(3) 管理 API / Kubernetes API の停止時に既存実行と新規起動の制限を区別して返す、(4) 古い generation が新設定を巻き戻さず、接続回復後に収束する。
- それまでの gateway は 1 プロセスが管理 API と invoke data plane を兼ね、invoke のたびに `state.db` の function / alias / revision を直接読み、bearer token を `[[identity.tokens]]` から直接引いていた。管理側が止まれば invoke も止まり、逆に「設定がいつまで有効か」という概念が無かった。
- 本プロジェクトに Kubernetes は無い。実行環境を作る host 側の制御 API は `ExecutionProvider`（Firecracker の API socket、process の spawn）であり、その可否は provider の preflight で観測している。
- `state.db` は同じ host の複数 gateway で共有できる（ADR-0003、PLT-4631 の dispatcher lease）。台帳の行保護（`repository/guard.rs`）は invocation の insert 時に親の function が同じ file にあることを確認する。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | control plane が generation 付きの publication を作り、data plane が pull して期限付き cache に保持する（本 ADR） | 採用 |
| B | data plane が管理 API を毎回呼ぶ（read-through、cache なし） | 不採用。要件の「毎回依存を避ける」に反し、管理側の停止がそのまま invoke の停止になる |
| C | 期限なしの cache（最後に見た設定で動き続ける） | 不採用。revoke した token・削除した function が control plane に届かない data plane で無期限に効く |
| D | control plane から data plane へ push（stream / webhook） | 保留。切断中の取りこぼしと再同期を結局 pull（`since`）で埋める必要があり、プロトタイプでは pull だけで足りる。push を足すときも本 ADR の generation 規則をそのまま使う |
| E | 設定の変更ごとに ledger の各 mutation で generation を採番する | 不採用。function / revision / alias のすべての書き込み経路（非同期の revision 検証を含む）に採番を足す必要があり、別プロセスの書き込みとの順序付けも要る。publication 側で「内容が変わったら採番」する方が書き込み経路を触らない |

## 決定

1. **配信の単位と形（`control/wire.rs`）。** 配信は key ごとの entry の列。key は `function` / `route`（function + alias）/ `revision` / `grant`（bearer token の鍵付き digest）/ `tenant` / `policy`。値はそれぞれ `Function`、`FunctionAlias`、`FunctionRevision`（artifact の digest、limits、egress、secret binding の**参照**。secret の値は含まない）、`AuthGrant{subject, tenant_id, roles}`、`TenantGrant`、`ConfigPolicy{allowed_egress}`。値の無い entry は tombstone（削除・revoke）。`ConfigDelivery{source, generation, since, config_ttl_seconds, auth_lease_seconds, entries}` は「`since` より大きい generation の entry すべて」と「ここに無い entry は `generation` の時点で変わっていない」を主張する。
2. **generation の刻印（`repository/config.rs`、migration 004）。** control plane（`combined` の gateway）は publish のたびに、**1 つのトランザクションの中で** ledger の function / alias / revision を読み、`[[identity.tokens]]` と policy と合わせて観測し、`config_publication` 表の前回の digest と比べ、内容が変わった key にだけ counter（`store_meta.config_generation`）を 1 進めた generation を付け、観測されなくなった key を tombstone にする。counter は `state.db` にあるので control plane の再起動をまたいで単調に増える。値そのものが持つ版（alias の generation、revision の状態の段階、function の削除）が保存済みより**小さい**観測は無視する。読みと刻印が同じトランザクションなので、2 つの publication が「古い読み」と「新しい刻印」を交差させることはない。tombstone は消さずに残す（どの `since` から聞かれても削除を伝えるため）。
3. **`ConfigSource` port（`control/source.rs`）。** 実装は 2 つ。`LedgerConfigSource` は上の publication を同じプロセス内で返す（`combined` の gateway が自分の cache に使い、`GET /v1/internal/config` でも返す）。`HttpConfigSource`（`apps/gateway`）は `GET <url>/v1/internal/config?since=<generation>` を `Authorization: Bearer <internal_token>` で呼ぶ（application crate は HTTP stack に依存しない）。
4. **data plane の cache（`control/cache.rs`）。**
   - entry ごとに `generation` と `valid_until`。`valid_until` = その entry を最後に確認した refresh の**開始時刻** + TTL。TTL は function / route / revision / policy が `config_ttl_seconds`、grant / tenant が `auth_lease_seconds`（認可 lease）で、それぞれ data plane の設定と control plane が配信で示す値の小さい方。成功した refresh は（差分が空でも）すべての entry を確認し直す。`now < valid_until` の間だけ有効で、`valid_until` ちょうどで失効する。
   - **巻き戻さない。** entry は保持している generation より**大きい** generation でしか置き換えない。配信全体の generation が cache の generation より小さい、または cache が居ない `since` に答えた配信は丸ごと無視し、何も確認し直さない（restore した backup や再送された古い応答が、revoke 済みの grant を延命しないため）。
   - 状態: `unknown`（未配信・tombstone）/ `fresh`（`stale_after` 以内に確認）/ `stale_but_valid` / `expired`。
   - refresh は同時に 1 つ。成功中は `refresh_interval_ms` ごと、失敗したら `backoff_initial_ms` から倍々で `backoff_max_ms` まで。**data plane は request の経路で control plane を待たない。** 例外は in-process の source（`combined`）で、自プロセスの function / revision / alias の書き込み（`SignalingConfigRepos` が signal を上げる）か、同じ `state.db` への別プロセスの commit（SQLite `PRAGMA data_version`）があったとき、または cache が期限に近いときだけ、答える前に refresh する。これで `combined` の gateway は従来どおり自分の deploy / rollback を即座に invoke に反映する。
5. **invoke は cache だけを読む（`control/gate.rs`、`services/invoke.rs`）。**
   - 認証: invoke・invocation の読み取り・cancel・`/v1/provider` は delivered grant で認証する（`authenticate_invoke`）。未配信（一度も同期していない）→ 503 `config_unavailable` / `Host.ConfigNotDelivered`、未知の token → 401、grant の lease 切れ → 503 / `Host.AuthLeaseExpired`、grant はあるが tenant が未知 → 403 `forbidden` / `Host.UnknownTenant`。管理 API は control plane 自身の `[[identity.tokens]]` で認証する。
   - 解決: function（未知 → 404。data plane は「存在しない」と「まだ届いていない」を区別できないので、tenant 間で存在を漏らさない 404 に揃える）、route、revision（route の先や pin した revision が未配信 → 503 / `Host.ConfigNotDelivered`。in-process source では 404）、policy（egress が許可外 → 403 / `Host.PolicyDenied`）。どれかが期限切れ → 503 / `Host.ConfigExpired`。いずれも受付前に返し、台帳に何も書かない。
6. **既存実行と新規起動を分ける。**
   - **実行中の invocation は止めない。** 期限切れも control plane の停止も、dispatch 済みの invocation と環境には何もしない。
   - **新しい環境の起動（cold start）** は、起動する時点で revision と tenant の認可がまだ有効であること（受付後に queue で待った場合を含む）、provider の制御 API が preflight を通っていること（直近の preflight が失敗としてキャッシュされていれば 503 `provider_unavailable` / `Host.ProviderControlUnavailable`）、control plane が到達不能な間は `[control_plane_outage] allow_cold_start = true`（既定）であることを要する。`false` なら到達不能な間は 503 / `Host.ColdStartRestricted`。環境再利用が off の gateway では invoke はすべて cold start なので受付前に拒否し、on なら受け付けて warm を試し、cold が要る時だけ invocation を `Failed{platform_error, Host.ColdStartRestricted}`（HTTP 503）にする。
   - `/readyz` の `control_plane` は `existing_executions: "continue"`、`new_invocations: accepted|refused`、`new_cold_starts: allowed|refused`、`refusal`（`error_type`）、`reason`、`control_plane_reachable`、cache の状態（generation、最終成功、連続失敗、`config_valid_until`、`auth_valid_until`、再接続回数、無視した古い entry / 配信の数）を返す。HTTP 200 は「新しい invocation を受け付ける」。
   - **「Kubernetes API 停止」の読み替え。** 本プロジェクトの host 制御 API は provider（Firecracker API / process spawn）で、その停止は preflight の失敗として観測する。上のとおり新規起動だけを止め、実行中と warm 環境の利用は続ける。
   - **管理 API** は data plane では提供しない（503 `control_plane_unavailable` / `Host.ControlPlaneUnavailable`）。`combined` の gateway でも store が応答しない（`RepoError::Store` / `Io`）ときは 503 `control_plane_unavailable` / `Host.StoreUnavailable`（以前は 500 `platform_error`）。
7. **dispatcher owner の再接続。** refresh が失敗の後に成功した（再接続）とき、`Application::refresh_config` は heartbeat を待たずに dispatcher lease を即座に更新し直す。停止の間に lease が失効して別の gateway に reclaim されていれば heartbeat は `Fenced` を返し、その dispatcher は従来どおり新しい仕事を拒否し続ける（re-register はしない。新しいプロセスとして起動し直す）。cache は再接続の最初の refresh で最新の generation に収束する。PLT-4631 の lease（台帳の所有）と本 ADR の lease（設定・認可の有効期限）は独立で、どちらかが切れれば新規受付は止まる。
8. **cell の形。** プロトタイプの data plane は control plane と**同じ `data_dir`（同じ cell）**を使う。台帳（invocation / environment / slot / dispatcher）と content-addressed の artifact store は cell で共有し、判断に使う設定（function / route / revision / grant / policy）だけを配信で受け取る。invocation の insert 時の行保護は同じ file の function を親として確認するが、これは台帳の整合性検査であって認可・解決の根拠ではない。data plane が独自の store と artifact の配布を持つ構成は後続。
9. **内部 credential。** `[control_plane] internal_token`（16 bytes 以上）は `GET /v1/internal/config` の bearer と、bearer token を配信する際の鍵（HMAC-SHA256）を兼ねる。tenant の token では 401、未設定の gateway と data plane では 404。data plane は `[[identity.tokens]]` を持てない（設定検査で拒否）。

## 結果（consequences）

- revoke（control plane の `[[identity.tokens]]` から token を消して再起動）は、data plane が control plane に届く限り次の refresh（≤ `refresh_interval_ms` + fetch 時間）で効き、届かない data plane では最大 `auth_lease_seconds`（最後に確認した refresh の開始から）まで効き続ける。これが revoke の遅延の上限である（`docs/threat-model.md` §14-9）。設定検査は `refresh_interval_ms < min(config_ttl_seconds, auth_lease_seconds) × 1000` を要求する。
- deploy / rollback が data plane に届くまで最大 `refresh_interval_ms`（+ fetch）かかり、その間は旧 revision を解決する。新しい function は届くまで 404。
- `combined` の gateway の invoke は、自プロセスと同じ `state.db` への他プロセスの commit のたびに publication の刻印（読み + 比較、変化があれば書き込み）を 1 回行う。function 数に比例するので、大量の function を持つ control plane では差分の刻印を mutation 側に寄せる必要がある（案 E の再検討）。
- 失敗した refresh は warn を 1 行ずつ出す（backoff の上限間隔）。
- `config_publication` の tombstone と `dispatchers` 表は retention が無く増え続ける。
- 予算 token・tenant quota は policy に含まない（P3、PLT-4643）。policy は egress profile の許可だけ。

## 検証

| 検証 | テスト / 記録 |
|---|---|
| 接続断の間も TTL 内は invoke 継続、配信に token・secret が無い | `tests/config_cache.rs::invokes_continue_from_the_cache_while_the_control_plane_is_down` |
| 期限境界（`valid_until - 1 ms` は有効、`valid_until` で失効、config TTL と auth lease の別々の理由） | `tests/config_cache.rs::new_work_is_refused_exactly_at_valid_until_with_the_matching_reason` |
| 設定順序の逆転（配信全体・entry 単位）で巻き戻らない、後退した source は何も延命しない | `tests/config_cache.rs::older_generations_never_roll_back_a_newer_configuration`、`repository/config.rs::tests::stamps_changes_ignores_stale_values_and_tombstones_removals` |
| 回復後の収束と再接続の記録 | `tests/config_cache.rs::a_reconnect_converges_to_the_latest_generation` |
| 停止中に fence された dispatcher は再接続後も fenced | `tests/config_cache.rs::a_dispatcher_fenced_during_the_outage_stays_fenced_after_the_reconnect` |
| 未知の tenant・未配信の revision | `tests/config_cache.rs::unknown_tenants_and_undelivered_revisions_are_refused` |
| revoke の遅延上限（1 refresh / 1 auth lease）と再起動をまたぐ generation | `tests/config_cache.rs::{a_revoked_token_stops_working_within_one_refresh_or_one_auth_lease, generations_are_monotonic_across_control_plane_restarts}` |
| outage policy（warm は継続、cold は 503） | `tests/config_cache.rs::the_outage_policy_restricts_cold_starts_but_not_running_environments` |
| provider 制御 API の停止 | `tests/config_cache.rs::a_failing_provider_control_api_refuses_cold_starts_only` |
| HTTP 面（内部 endpoint の認証、data plane の管理 API 503、`/readyz`） | `apps/gateway/tests/gateway_integration.rs::{a_data_plane_gateway_serves_invokes_from_delivered_configuration, the_internal_config_endpoint_is_absent_without_an_internal_credential}` |
| 2 プロセスの E2E（process provider） | `scripts/control-plane/outage-e2e.sh`、`docs/evidence/20260917T045347Z-split-process/`（19/19 PASS。管理停止から 7.9 s で `Host.ConfigExpired`（config TTL 8 s）、実行中の invocation は停止をまたいで 200、auth lease 後に `Host.AuthLeaseExpired`、再起動で再接続・収束・tenant B の revoke） |
