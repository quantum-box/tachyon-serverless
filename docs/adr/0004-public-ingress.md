# ADR-0004: 安定した public ingress は組織所有ドメイン上の named tunnel を第一とし、connector を KVM host に置く

## ステータス

Proposed（2026-09-16）。Linear issue は未採番。本 ADR は文書だけで、Rust は 1 行も変えていない。

**検証の範囲**: 実際に外部へ公開したことがあるのは、ephemeral な quick tunnel を使った **1 回だけ**である（`docs/evidence/public-20260916T120349Z/`、laptop 上の Lima VM から、実行後に閉鎖済み）。本 ADR で比較する他の 4 案は、このリポジトリで一度も動かしていない。時間の数値はすべて nested virtualization 上の参考値で、SLA でも性能の約束でもない。

## コンテキスト

### 1. いま公開されているもの（記録された事実）

`docs/evidence/public-20260916T120349Z/summary.txt` に記録された経路は次のとおり。

```
macOS host ─▶ インターネット ─▶ Cloudflare quick tunnel ─▶ Lima VM の gateway ─▶ Firecracker microVM
```

確認できたこと: `GET /http/` が 200、`POST /http/echo` が本文をそのまま返し、関数が返した 404 が素通しされ、`Authorization` header の無い request が 401（`unauth.txt`）になった。最後の invocation（`invocation.json`）には `guest_boot_id` と `firecracker_version: v1.17.0` があり、request が本当に microVM に届いたことが示されている（`environment_boot_ms` 9873、`total_ms` 10944）。

確認できていないこと: URL は quick tunnel が採番した一時的なもので、記録すらしていない（実行ごとに変わり、既に閉じている）。証明書は Cloudflare のもので、我々が管理する DNS record は無い。認証は設定ファイルに書いた静的 token 1 段だけである。つまり **「もう一度同じ URL を叩く」ことができない**。繰り返し呼べる API を提供するには、ingress が安定していて、かつ運用できる必要がある。

### 2. すでにリポジトリ側にある制約

| # | 制約 | 正本 |
|---|---|---|
| C1 | gateway は loopback で listen する。既定は `listen = "127.0.0.1:8080"` で、同梱の 2 つの設定ファイルも同じ値 | `crates/application/src/config.rs`（`default_listen`）、`config/gateway.dev.toml`、`config/gateway.firecracker.toml` |
| C2 | gateway に TLS 終端は無い。`axum::serve` が平文 TCP の listener を bind するだけで、証明書を読む経路がコードに無い | `apps/gateway/src/lib.rs`（`TcpListener::bind` → `axum::serve`） |
| C3 | provider は KVM を要求する。preflight が `/dev/kvm` の rw を検査し、Firecracker は host と同じアーキテクチャの guest しか動かさない。したがって **origin は `/dev/kvm` のある Linux host に固定される**（macOS では Lima VM の中） | `crates/providers/firecracker/src/preflight.rs`、`docs/kvm.md` §2・§6 |
| C4 | user code は egress none。NIC を付けず tap も作らないので、guest から外へ出る経路が無い。**inbound の唯一の経路は gateway である**（関数は callback も webhook 送信もできない） | `docs/protocol.md` §C、`docs/adr/0001-execution-provider-firecracker-first.md` §「決定」、実測 `docs/evidence/isolation-20260916T020934Z/` |
| C5 | network 境界は脅威モデルの範囲外に置かれている。「gateway の前段（TLS 終端、WAF、rate limit）」は deployment の責務、`listen = "127.0.0.1:8080"` は「ループバック外に露出しない前提」、残存リスク §14-3 は「平文 HTTP と静的 token」 | `docs/threat-model.md` §1（範囲外）・§5-2・§5-3・§14-3・§15 |
| C6 | 認証は静的 bearer token 1 段。token → `Principal{tenant, roles}` に解決するだけで、有効期限・失効・rotation・scope の仕組みが無い。token の追加は設定ファイルの編集と gateway の再起動を要する | `apps/gateway/src/middleware.rs`（`authenticate`）、`config/gateway.firecracker.toml`（`[[identity.tokens]]`）、`docs/threat-model.md` §5-4 |
| C7 | 容量制御は「同時実行数」であって rate limit ではない。gateway 全体の semaphore（既定 `max_concurrency = 8`）と bounded queue（`max_queue = 32`）で 429 / 504 を返すだけ | `docs/architecture.md` §3-5・§4、`docs/threat-model.md` §11 |
| C8 | 応答時間は cold start が支配する。33 attempt の中央値は `environment_boot_ms` 3503 / `runtime_init_ms` 358 / `handler_ms` 77、`total_ms` は 4258（最小 3243・最大 9413） | `docs/kvm.md` §5.5、`docs/evidence/20260915T171631Z-firecracker/invocations.json` |
| C9 | warm 再利用は 2 重 gate の内側にあり、同梱 provider では働かない。firecracker / process はどちらも `idle_quiesce` / `idle_resume` を `Unsupported` と報告し、`[pool] enabled` の既定は false。したがって公開しても毎回 microVM が起動する | `crates/application/src/services/pool.rs`（`PoolPolicy::decide`）、`crates/providers/firecracker/src/provider.rs`、`docs/architecture.md` §4 |

C8 と C9 を合わせると、**公開したときに外から見える API は「毎回 4 秒前後かかる API」である**。これは ingress の選択では変わらない（前段を速くしても microVM の boot は消えない）。

### 3. 安定した ingress に要るもの

1. 変わらない名前（我々が制御する DNS record）。
2. 更新できる証明書（誰が保持し、誰が更新するか）。
3. origin の再起動・IP 変更で URL が変わらないこと。
4. 認証（現状は C6 の 1 段だけ）。
5. 止め方（URL を落とす、token を失効させる）。
6. 継続運用の主体（どの host が常時稼働し、誰が見るか）。

## 選択肢

1. **組織が所有するドメイン上の Cloudflare named tunnel。** connector（`cloudflared`）を KVM host 上で常駐させ、zone の DNS record を tunnel に向ける。
2. **public address を持つ cloud VM / 専用サーバ + reverse proxy。** 前段（Caddy / nginx など）が ACME で自動取得した証明書で TLS を終端し、背後の gateway に proxy する。
3. **Tachyon Cloud App を front door にし、KVM host へ proxy する。**
4. **Tailscale 等の overlay + funnel 形式の public entry。**
5. **ephemeral tunnel のまま、public access を demo 限定として扱う（現状維持）。**

## 比較

いずれの行も **本リポジトリでの測定ではない**。5 だけが 1 回実行した記録を持ち、1〜4 は未検証である。

| 観点 | (1) named tunnel | (2) VM + reverse proxy | (3) Cloud App front door | (4) overlay + funnel | (5) ephemeral tunnel |
|---|---|---|---|---|---|
| 初期コスト | 小。ドメインの zone を事業者に預け、tunnel を 1 本作り、KVM host に connector を常駐させる | 中。server の調達、OS、firewall、proxy 設定、ACME、gateway への経路（同一 host か private network） | 大。別 product（`tachyon-apps`）側に route を作り、そこから KVM host まで到達経路を引く。本リポジトリの前提（cluster 非依存）の外側の作業 | 小〜中。両端に daemon を入れ、funnel を有効化する | 最小。コマンド 1 つ（実績あり） |
| 運用コスト | connector の常駐監視、token / credential の保管、zone の管理 | server の存在自体（OS 更新、証明書更新の監視、firewall、侵入対策）。TLS 終端が自分の責任になる | 上の (1) か (2) に加えて、front door 側の deploy と整合を取り続けるコスト。故障点が 2 つになる | daemon の常駐、account と device の管理、funnel の制約への追従 | 無い（上げっぱなしにできないので運用が成立しない） |
| 証明書の保持者 | 事業者（edge で終端）。我々は持たない | **我々**（ACME の鍵と証明書は自分の host 上） | front door の事業者 / 基盤 | overlay 事業者 | 事業者（一時的） |
| DNS の保持者 | **我々**（組織所有の zone に record を書く） | **我々**（A / AAAA を自分の address に向ける） | front door 側の zone。本リポジトリからは制御できない | 事業者が払い出す名前（自ドメインではない） | 誰でもない（毎回変わる） |
| origin 再起動時 | URL は変わらない。connector が再接続するまで 502 系になる | URL は変わらない。address 固定なら DNS も不変 | URL は変わらない（front door は生きたまま、背後が落ちる） | URL は変わらない | **変わる**（毎回新しい hostname） |
| 認証 | gateway の bearer token（C6）。edge 側で追加の認証（service token / access policy）を重ねられる | gateway の bearer token。前段で basic 認証や mTLS を足せる | front door の認証と gateway の token の 2 段。整合の定義が要る | overlay の identity（client 側も参加する場合）+ gateway の token。funnel 経由の public request には overlay identity が無い | gateway の bearer token だけ |
| residency（§「residency についての正確な言い方」） | 実行と state は KVM host のまま。**平文の request / response は事業者の edge を通る** | 実行・state・TLS 終端のすべてが我々の host。第三者の network を通るのは経路上の transit だけ | 実行と state は KVM host。平文は front door の基盤を通る | 実行と state は KVM host。平文は overlay 事業者の relay / funnel を通る | (1) と同じ。加えて経路の記録が残らない |
| 継続運用に要るもの | ドメイン、事業者 account、tunnel credential、常時稼働の KVM host、運用者 | 上に加えて server 1 台と、その host の管理主体 | 上に加えて Cloud App 側の担当と deploy 権限 | account、device の登録、常時稼働の KVM host、運用者 | 何も無い（demo のたびに人が手で上げる） |
| 本リポジトリでの検証 | 未検証 | 未検証 | 未検証 | 未検証 | **1 回だけ実測**（`docs/evidence/public-20260916T120349Z/`） |

補足（各案の、比較表に収まらない点）:

- **(1)** origin に inbound port を開けない環境でも成立する。現状の KVM host は laptop 上の Lima VM（`docs/kvm.md` §5）で、port forward も固定 address も持たないので、この性質は実際に効く。撤退は DNS record と tunnel を消すだけで済む。
- **(2)** 「第三者の edge を通さない」という要件が出たときに唯一成立する案。ただし C3 により、その server は **`/dev/kvm` を持つ Linux でなければ origin を同居させられない**。同居しない場合は server → KVM host の private な経路（VPN / 専用線 / SSH tunnel）が別途要り、故障点と設定量が増える。
- **(3)** `tachyon-apps` の Cloud Apps は「長期稼働サービス」を Cloud Run / Pages / Lambda / Workers に載せる product であり、本リポジトリの `Function`（関数 + 不変 Revision + alias）とは粒度が違う（`docs/inventory-tachyon-apps.md` §3.1・§3.11）。本リポジトリは tachyon-apps に compile-time 依存を持たないことを受入条件にしている（`docs/acceptance.md` PLT-4613 #3）。front door として使うだけなら compile-time 依存は増えないが、**公開経路の可用性が別 product の deploy に従属する**。
- **(4)** client 側も overlay に参加する「社内限定アクセス」には最短で、その用途なら有力である。しかし public な API の front door としては、client に daemon の導入を求めるか、funnel 形式の public entry（事業者が払い出す名前・事業者の証明書・事業者の制約）を受け入れるかのどちらかになり、要件 1・2 を満たさない。
- **(5)** 「人が見ている間だけ上がっている」ことが唯一の安全弁として働いているので、demo としては筋が通っている。繰り返し呼ばれる API には使えない。

### residency についての正確な言い方

「residency の表明がどう変わるか」を書くにあたり、**引用できる表明が何であるかを先に確定する**。

- `docs/adr/0001-execution-provider-firecracker-first.md` に data residency の条項は無い。そこにあるのは「既存 cluster に依存せず、**KVM のある Linux host 1 台で再現できる**」という位置づけ（§「比較」の「既存 Tachyon cluster への依存」行、§「Firecracker を第一にする理由」）である。
- RFC（quantum-box/knowledge#284）の本文は **このリポジトリから参照できない**。節番号まで引用しているのは `ReuseKey`（RFC §5.3）だけで、その事実は `docs/adr/0003-execution-state-persistence.md` §「参照」に書かれている。したがって本 ADR は「RFC の residency 条項がこう書いてある」とは言えない。
- `docs/threat-model.md` §15 は「コンプライアンス要件（監査ログの保全、**データ所在**）」を P1 の非目標として明示している。

この 3 つから言えることだけを書く。

1. **どの案でも変わらないこと**: 関数の実行（microVM）、artifact、`state.json`（invocation の inline output と input digest を含む）、secret の解決は KVM host 上に留まる（`docs/threat-model.md` §4・§14-4）。ingress を足しても計算とデータの所在は動かない。
2. **案によって変わること**: TLS をどこで終端するか。(1)(3)(4) では第三者の edge / relay で終端されるので、**request と response の平文がその事業者のプロセスを通る**。invoke の payload と handler の出力がそこに含まれる。(2) だけが終端を自分の host に保つ。
3. したがって、(1) を採る場合は「ADR-0001 の『KVM host 1 台で完結する』は実行系についてのみ成立し、**transport は第三者を経由する**」と明記する必要がある。これは §15 の非目標を目標に格上げするものではなく、非目標のままであることを公開時に読める形にする、という意味である。RFC 側に residency 条項がある場合は RFC を正とし、本 ADR を改版する（`docs/adr/0003-execution-state-persistence.md` §「参照」と同じ規則）。

## 決定

1. **推奨は (1): 組織が所有するドメイン上の named tunnel。connector は KVM host 上に置く。** 理由は次の 4 点。
   - **C3 と現状の host に合う**: origin に inbound port を開けない（laptop 上の Lima VM でも、後で bare metal に移しても同じ手順で動く）。(2) は固定 address を持つ host を先に用意しないと始まらない。
   - **要件 1・3 を満たす最小の構成**: 名前と DNS record は我々の zone に残り、origin が再起動しても URL が変わらない。(4) は名前が事業者払い出しになり、(5) は毎回変わる。
   - **撤退が 1 手**: DNS record と tunnel を消せば公開が止まる。公開を継続する判断がまだ無い段階では、止めやすさが要件である。
   - **故障点が増えない**: (3) は別 product の可用性に従属する。
2. **(2) は代替として残す。** 「平文を第三者の edge に通せない」という要件が出たら (2) に切り替える。gateway 側の設定（C1 の loopback listen）は **どちらでも同じ**なので、切り替えは前段の入れ替えだけで済む。この性質を壊す変更（gateway 自身に TLS を持たせる等）はしない。
3. **(3) は要件が固まるまで採らない。** 本リポジトリが独立して再現できるという前提（`docs/adr/0001-execution-provider-firecracker-first.md`、`docs/inventory-tachyon-apps.md` 冒頭）を、可用性の面で崩すため。
4. **(4) は「社内・関係者限定のアクセス」用途に限って可とする。** public API の front door には採らない。
5. **(5) は demo 限定として残す。** 人が見ている間だけ上げ、終わったら閉じ、URL を記録しない（`docs/evidence/public-20260916T120349Z/` の扱いを踏襲する）。demo 以外で ephemeral tunnel を上げっぱなしにしない。
6. **どの案でも、管理 API を公開面に出さない。** 分離は §「公開してよい面と、してはいけない面」のとおり **hostname で行う**。
7. **本 ADR は Proposed のままであり、これだけでは public endpoint を上げる根拠にならない。** §「推奨が owner に要求するもの」が揃い、§「demo より長く上げる前に必要なもの」の項目に issue が付いて初めて Accepted にする。

## 推奨が owner に要求するもの

(1) を実行するには、リポジトリの外から次が供給される必要がある。**どれも本リポジトリには無い。**

| # | 要るもの | 具体 | 無い場合どうなるか |
|---|---|---|---|
| R1 | ドメイン名 | 組織が所有し、DNS を管理できる zone。invoke 用に 1 つの hostname（例: `fn.<domain>`）。管理 API 用の hostname は **公開しない**（作るとしても公開 DNS に出さない） | 名前が事業者払い出しになり、要件 1 を満たさない |
| R2 | 事業者 account | zone を置く account と、tunnel を作る権限。account の所有者が誰か（個人ではなく組織）を決めておく | 個人 account に紐づくと、その人が離れた時点で公開が止まる |
| R3 | 資格情報 | tunnel の credential を KVM host に置く。保管の扱いは `config/gateway.{dev,firecracker}.toml` と同じ（operator だけが読める。`docs/threat-model.md` §5-3） | 漏れれば第三者が同じ名前で origin を差し替えられる |
| R4 | 常時稼働の host | `/dev/kvm` のある Linux host。**現状は laptop 上の Lima VM で、laptop を閉じれば落ちる**（`docs/kvm.md` §5.3 には「VM を起動するたびに `sudo chmod 666 /dev/kvm`」とある＝毎回人手が要る） | URL は生きているのに 502 を返し続ける。公開 API としては最悪の形 |
| R5 | 運用者 | 1 名以上。URL を止められ、token を失効でき、ログを見る人。連絡先（`SECURITY.md` の報告経路と対応づける） | 濫用に気付く人も止める人もいなくなる |
| R6 | token の発行・失効手順 | 公開用 token は `config/gateway.{dev,firecracker}.toml` に書いて gateway を **再起動**することで有効になる（C6）。失効も同じ手順。demo 用にコミットされている token（`dev-token-tenant-a` ほか）は公開面で使わない | 失効に再起動が要ることを知らずに公開すると、事故のときに「止める」手段が tunnel を落とすことだけになる |

R4 は最も軽く見られやすく、最も重い。**「laptop の中の VM」は公開 API の origin ではない。**

## 公開してよい面と、してはいけない面

router は 1 本で、認証 layer は `/v1` 全体に一律に掛かる（`apps/gateway/src/lib.rs`）。path 単位の公開制御は gateway に無いので、**分離は前段でしか行えない**。

| 面 | path | 公開 | 理由 |
|---|---|---|---|
| invoke surface | `POST /v1/functions/{id}:invoke`、`POST /v1/functions/{id}/invoke`、`ANY /v1/functions/{id}/http/{*path}` | **公開してよい** | これが「人が呼ぶ API」そのもの |
| 管理 API | `/v1/artifacts`、`/v1/functions`（作成 / 一覧 / 削除）、`/v1/functions/{id}/revisions*`、`/v1/functions/{id}/aliases*` | **公開しない** | 公開面で必要ない。artifact upload は 256 MiB まで受ける（`crates/domain/src/limits.rs`）ので、token が漏れた場合の被害が大きい |
| tenant データの読み取り | `/v1/functions/{id}/invocations`、`/v1/invocations/{id}`、`/v1/invocations/{id}/logs`、`/v1/functions/{id}/usage`、`/v1/invocations/{id}:cancel` | **公開しない** | invocation の inline output とログ本文（他人の payload を含みうる）が出る |
| provider 情報 | `/v1/provider` | **公開しない** | preflight の `detail` に host の絶対パス（firecracker バイナリ、`vmlinux`、`rootfs.ext4`、workdir）と kernel / rootfs の sha256 が入る。実例: `docs/evidence/20260915T171631Z-firecracker/provider.json` |
| 無認証の面 | `GET /healthz`、`GET /readyz`、`GET /openapi.json` | **公開しない** | `/readyz` は `PreflightReport` をそのまま返すため、**token 無しで上記の host パスと digest が読める**（`apps/gateway/src/handlers.rs` の `readyz`）。`/openapi.json` は管理 API の全体像を配る |

**hostname で分ける。path では分けない。** 根拠:

1. gateway 側に「この listener は invoke だけ」という設定が無い以上、分離は前段の route 定義に依存する。hostname で分ければ route を 2 本書くだけで、**公開側の route に管理 path が一切登場しない**。
2. path allowlist は間違えやすい。`/v1/functions/{function_id}` という **同じ path に、invoke（colon route の `POST`）と管理（`GET` / `DELETE`）が同居している**（`apps/gateway/src/lib.rs`: `invoke_colon` と `get_function` / `delete_function`）。path prefix で切ると、この 1 本を method まで見て分ける regex を前段に書くことになり、書き間違いが即露出になる。
3. 事故時に「公開 hostname を落とす」だけで管理面に影響が出ない。逆も同じ。

将来 path で分けたくなった場合は、gateway 側に「公開 listener は invoke surface だけを mount する」設定（listener を 2 つ持つ、または router を分ける）を先に実装する。それまでは hostname 分離を前提にする。

## demo より長く上げる前に必要なもの

**先に正直に書く: 本リポジトリに「P3」という語も、P3 の issue 番号も存在しない。** 現れるのは P0〜P1（PLT-4613〜PLT-4630）と P2（PLT-4631 / PLT-4632 / PLT-4633）だけである（`docs/acceptance.md`、`docs/adr/0003-execution-state-persistence.md`）。したがって「これらは P3 の issue が cover している」とは書けない。下表の「追跡先」は **未採番**であり、公開を継続する判断をするなら Linear に issue を立て、採番された番号を本 ADR に追記する（そのときまで本 ADR は Proposed のまま）。

| 項目 | 今あるもの | 足りないもの | 追跡先 |
|---|---|---|---|
| rate limit | 同時実行の semaphore（既定 8）と bounded queue（32）による 429 / 504 だけ（`docs/architecture.md` §3-5）。これは「並列数」であって「単位時間あたりの回数」ではない | IP 単位 / token 単位の request rate 制限。前段（ingress）と gateway のどちらで持つかの決定を含む。`docs/threat-model.md` §15 は rate limit を非目標と明記しているので、**その行を書き換える issue**でもある | 未採番 |
| per tenant quota | revision の `max_concurrency`（1..=1000）と gateway 全体の上限（`docs/threat-model.md` §11） | tenant 単位の割り当て（同時実行、1 日あたりの invocation 数、payload 総量）。現在 tenant は本リポジトリが発行すらしていない（`docs/threat-model.md` §2） | 未採番 |
| budget cap | `GET /v1/functions/{id}/usage` の集計のみ。`not_billable: true` が常に付く（`docs/api.md` §5.10） | 上限に達したら止める仕組み。usage は `InMemoryUsageSink` のままで再起動で消える（`docs/adr/0003-execution-state-persistence.md` §「非対象」）ので、**先に usage の永続化が要る** | 未採番（永続化の方針は ADR-0003） |
| log retention | invocation 単位の上限（2000 行 / 1 MiB、16 KiB/行）と `dropped` フラグ。log は memory のみでプロセスと運命を共にする（`docs/threat-model.md` §11、`docs/adr/0003-execution-state-persistence.md` §「決定」5） | 保持期間の定義と削除。公開すると第三者の payload が `state.json` の inline output に溜まり、`state.json` に retention は無い（ADR-0003 §「非対象」）。ファイル権限の扱いは `docs/threat-model.md` §14-4 | 未採番 |
| abuse response | 止める手段は 2 つだけ: token を設定から消して gateway を再起動する（C6）、または tunnel を落とす。function 単位の停止は `DELETE /v1/functions/{id}`（以後 invoke は 409） | 「誰が・何を根拠に・どれくらいで止めるか」の手順、連絡先（`SECURITY.md`）、記録の残し方。tenant 単位の緊急停止 API は存在しない | 未採番 |

加えて、公開を続けるなら以下も決まっていなければならない（いずれも現状は未決）。

- **cold start を前提にした前段の timeout**: `total_ms` の中央値 4258・最大 9413（C8）に対し、前段の idle / read timeout がそれ以上であること。warm 再利用は C9 により働かない。
- **client IP の扱い**: gateway は接続元 IP を `HttpRequestEvent.source_ip` として関数に渡す（`apps/gateway/src/handlers.rs`）。tunnel や proxy を挟むと **これは常に前段の IP になる**。`X-Forwarded-For` は解釈しない。IP 単位の制限を掛けるなら前段で行うしかない。
- **前段が付ける header の扱い**: 関数に渡らないのは `authorization` と `x-tachyon-tenant-id` ほか hop-by-hop の 9 個だけで（`apps/gateway/src/handlers.rs` の `STRIPPED_REQUEST_HEADERS`）、**前段が付けた header はそのまま user code に届く**。前段固有の header を認証の根拠にしない（`docs/threat-model.md` §6 の原則と同じく、境界の外から来たものを信じない）。

## 結果（consequences）

- `listen` は `127.0.0.1` のままにする。gateway に TLS 終端や証明書の設定を足さない（C2 を意図的に維持する）。ingress を差し替えられる性質は、これを守ることで保たれる。
- `docs/threat-model.md` §5-2 の前提（「ループバック外に露出しない」）は、ingress を実際に導入する時点で「前段が TLS を終端し、gateway へ到達できるのは前段だけ」に置き換わる。**その書き換えは実装 issue が threat-model 側で行う。本 ADR は文書を触らない。**
- 公開面と管理面が別 hostname になるため、CLI（`tsls`）の `TSLS_API_URL` は管理面（loopback もしくは非公開 hostname）を指し続ける。公開 hostname に向けると deploy 系が 404 になる。
- 公開しても warm 再利用は働かない（C9）。「公開したから速くなる / 遅くなる」ことはない。
- 本 ADR は ingress の**選択**であり、手順書ではない。採択後に runbook（connector の常駐方法、証明書と credential の置き場、停止手順）を別途書く。

## 非対象

- WAF、DDoS 対策、bot 対策（`docs/threat-model.md` §15 の非目標をそのまま維持する）。
- mTLS、OAuth / OIDC、tenant の self-service な token 発行。
- 複数 origin、fail over、HA、地理分散。単一 KVM host のままである（C3）。
- 本 ADR の時点での実施。実施は R1〜R6 が揃ってからで、揃うまでは (5)（demo 限定）が現状維持の正である。

## 参照

- 文書: `docs/architecture.md` §3・§4、`docs/protocol.md` §C、`docs/threat-model.md` §1・§5・§7・§11・§14・§15、`docs/api.md` §1・§3、`docs/kvm.md` §2・§5・§6、`docs/acceptance.md`、`docs/inventory-tachyon-apps.md` §3.1・§3.11、`docs/adr/0001-execution-provider-firecracker-first.md`、`docs/adr/0002-process-provider-dev-only.md`、`docs/adr/0003-execution-state-persistence.md`。
- コード: `crates/application/src/config.rs`（`default_listen`）、`apps/gateway/src/lib.rs`（router と `serve`）、`apps/gateway/src/middleware.rs`（`authenticate`）、`apps/gateway/src/handlers.rs`（`readyz`、`STRIPPED_REQUEST_HEADERS`、`source_ip`）、`crates/providers/firecracker/src/preflight.rs`（`/dev/kvm` の検査）、`crates/application/src/services/pool.rs`（`PoolPolicy::decide`）、`crates/provider-port/src/execution.rs`（`Capabilities` / `Support`）。
- 実行記録: `docs/evidence/public-20260916T120349Z/`（唯一の public 露出、1 回、quick tunnel）、`docs/evidence/20260915T171631Z-firecracker/`（E2E 28/28、33 attempt、時間の中央値の出典）、`docs/evidence/isolation-20260916T020934Z/`（egress none の実測）。
