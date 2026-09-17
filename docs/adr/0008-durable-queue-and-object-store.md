# ADR-0008: 非同期イベントは NATS JetStream（単一 node の file store）で配送し、大きな入出力は tenant / region で区切った暗号化 object として保持する（PLT-4638）

## ステータス

Accepted（2026-09-17、PLT-4638）。検証環境の用意までが範囲で、invoke pipeline からはまだ使わない（`invokeAsync` と transactional outbox は PLT-4639、dispatcher / retry / DLQ は PLT-4640、cron / webhook は PLT-4641）。

実装:

- port: `crates/durable-port`（`EventQueue`、`ObjectStore`、feature `testkit` の queue 契約テスト）
- queue: `crates/adapters/queue-nats`（JetStream adapter、`tachyon-queue-probe`）、`crates/application/src/durable/sqlite_queue.rs`（開発・CI 用の埋め込み実装）
- object: `crates/application/src/durable/{fs_objects.rs,crypto.rs,gc.rs}`、台帳側 `crates/application/src/repository/objects.rs` と migration `005_object_refs.sql`
- 設定: `[queue]` / `[objects]`（`crates/application/src/durable/config.rs`）、gateway の接続 `apps/gateway/src/durable.rs`
- IaC: `deploy/nats/`（pin した nats-server の版と sha256、server 設定、account / user の雛形）、`scripts/queue/{up,down,verify}.sh`
- 証跡: `docs/evidence/queue-objects-20260917T052554Z/`

## コンテキスト

- P2 の非同期 invoke・retry・cron・webhook は「受け付けた event を、プロセスや queue の再起動をまたいで少なくとも 1 回届ける」仕組みを要する。台帳（`state.db`、ADR-0003）は invocation の状態と冪等性の正本だが、配送（待ち行列、可視性 timeout、再配送回数、backlog）は持たない。
- 同期 invoke の入出力は 1 MiB / 6 MiB 上限で、出力は `inline_output_max_bytes` 以下だけ台帳に本文を持つ（ADR-0003）。非同期 invoke では「受付時に入力を保存し、後で実行する」「大きな出力を後で取りに来る」が要るので、台帳の外に本文の置き場が要る。
- 本リポジトリの検証環境は M4 Mac（Lima）と hosted CI runner で、docker を前提にできない。
- 受入条件は、queue 再起動後の読み出し、tenant 越境・未認証・上限超過の拒否、ACK / 再送と DB ledger の責任分界と実際の保存先・複製条件の記録、retention / GC が未完了 Invocation の object を消さないこと、単一 node を HA と説明しないこと。

## 選択肢

### queue

| 案 | 内容 | 採否 |
|---|---|---|
| A | NATS JetStream（file store、work-queue stream、pull consumer、明示 ACK） | **採用**（第一候補として検証） |
| B | 台帳 `state.db` に queue 表を作り、dispatcher が poll する | 開発・CI 用の埋め込み実装としてだけ採用（`SqliteEventQueue`）。単一 file・単一 node で、本番の queue にはしない |
| C | Kafka / Redpanda | 不採用。partition と consumer group は work queue（1 message を 1 worker が ACK するまで占有）に対して過剰で、local process として pin して起動する負担が大きい |
| D | Redis Streams | 不採用。永続性が AOF の設定次第で、`XCLAIM` による可視性 timeout を自前で組む必要がある |

### object

| 案 | 内容 | 採否 |
|---|---|---|
| A | 各 node の disk に AES-256-GCM で暗号化した file（`FsObjectStore`） | **採用**（既定の実装） |
| B | S3 互換 storage（MinIO / R2） | 今回は実装しない。port は S3 互換 adapter をそのまま載せられる形にした（key = `<region>/<tenant>/<object id>`、metadata は object metadata に写像できる）。検証環境に常駐 service と credential を増やさないため |
| C | 台帳に BLOB で入れる | 不採用。`state.db` の WAL と backup が本文で膨らみ、ADR-0003 の「本文を持たない」方針に反する |

## 決定

### 1. 責任分界: queue は配送、台帳は決定

| 事柄 | 正本 | 理由 |
|---|---|---|
| invocation が受け付けられたか、終わったか、結果 | 台帳（`state.db`） | 1 行の CAS で決まる。queue の ACK は「worker が受け取った」ことしか言わない |
| 同じ要求の二重受付の防止（Idempotency-Key） | 台帳（`idempotency` 表の主キー） | queue の dedup window は時間と再起動で失われる（下の観測） |
| 次に誰がどの event を処理するか、再配送、backlog | queue | 可視性 timeout（`ack_wait`）と `max_deliver` を持つ |
| 配送した event を処理してよいか | 台帳 | worker は event を受け取ったら台帳の CAS（例: `Accepted` → `Queued`、dispatcher lease）で自分の番かを決め、負けたら ACK して捨てる |

- **配送は at-least-once。** ACK は台帳の commit の**後**に送る。commit と ACK の間で worker が落ちれば event は `ack_wait` 後に再配送され、台帳の CAS が二重実行を止める。ACK を先に送ると、commit 前に落ちた event が失われる。
- **dedup は publisher の再送よけに過ぎない。** `Nats-Msg-Id`（`MessageId`）と `duplicate_window` で同じ id の再 publish を 1 件にまとめるが、exactly-once の根拠にしない。実測（証跡 `results.txt` の `restart.dedup_after_restart`）: kill -9 の後、JetStream は stream に**残っている** message から dedup 表を作り直すので、kill の前に ACK（= 削除）された 10 件の id は再 publish が新規として受理された。stream に残っていた 90 件と再起動後に見た id は重複と判定された。
- **ACK の確認。** adapter は ACK / NAK / TERM を request として送り、server の応答を待つ（double ack）。応答が無ければ `Unavailable` を返す（その event は再配送されうる）。

### 2. queue の形（stream as code）

`NatsEventQueue::connect` が起動のたびに stream を作るか更新する。server 側で手作業の設定をしない。

| 設定 | 値 | 理由 |
|---|---|---|
| `storage` | `file` | server の再起動で消えない |
| `retention` | `workqueue` | ACK / TERM で削除。1 subject に consumer は 1 つ |
| `discard` | `new` | 満杯なら**新しい publish を拒否**する（`QueueError::QueueFull`、code `queue_full`）。古い message を黙って捨てない |
| `max_msgs` / `max_bytes` / `max_msg_size` / `max_age` | `[queue.limits]`（既定 100 000 件 / 256 MiB / 256 KiB / 7 日） | 容量と保持期間を必ず有限にする（`max_age = 0` は設定検証で拒否） |
| `duplicate_window` | `[queue.limits] duplicate_window_seconds`（既定 120 s、`max_age` 以下） | publisher 再送の吸収 |
| `num_replicas` | `1` | **単一 node、複製なし** |
| subject | `<prefix>.<tenant_id>.<topic>`（既定 prefix `tachyon.events`） | tenant と topic は検証済みの label で、`.` `*` `>` を含まない |

consumer は durable pull consumer で `ack_policy = explicit`、`deliver_policy = all`、`ack_wait`、`max_deliver`、filter `<prefix>.*.<topic>`。`max_deliver` に達した message は配送されなくなるが stream には `max_age` まで残る（DLQ への移動は PLT-4640）。

`SqliteEventQueue`（`<data_dir>/queue.db`、0600、WAL、`synchronous = FULL`）は同じ意味を 1 file で実装し、`crates/durable-port/src/testkit.rs` の契約テスト（durable publish、dedup、discard new、ACK / NAK / TERM、再配送、`max_deliver`、`max_age`、再 open 後の未 ACK message）を JetStream と同じく通す。`profile = "production"` では設定検証で拒否する。

### 3. 認証と server 設定（`deploy/nats/`）

- nats-server は `deploy/nats/versions.env` で版（v2.14.7）と 4 platform の sha256 を pin し、`scripts/queue/lib.sh` が download 後に照合して一致しなければ実行しない。docker は使わない。
- listen は `127.0.0.1` だけ。JetStream は `sync_interval: always`（publish の ACK 前に fsync）。
- account `TACHYON` に user `gateway` を 1 つ。password は `up.sh` が生成して state directory に 0600 で置く（`auth.conf`、`gateway.password`）。permission は event の subject、`$JS.API.>`、`$JS.ACK.>` への publish と `_INBOX.>` の subscribe だけ。**`no_auth_user` を定義しない**ので、credential の無い接続は server が `Authorization Violation` で拒否する。
- gateway 側も匿名接続の設定を持てない: `[queue.nats]` は `user` + `password_file` か `nkey_seed_file` のどちらか一方が必須（設定検証）、credential file は group / other に読める mode なら拒否する。
- TLS は無い（loopback 前提）。別 host に置くなら TLS と nkey / JWT が前提条件で、今回は検証していない。

### 4. object の形

- **参照は `(tenant, region, object id)`。** `ObjectId` は `obj_<ULID>` で推測可能な情報を持たない。`get` / `head` / `delete` は scope と一緒にしか呼べず、別 tenant・別 region の scope では `NotFound`（存在しない id と区別できない）。ファイルの配置も `<root>/<region>/<tenant>/<id>.{data,meta}` で、metadata に記録した scope と一致しなければ `NotFound`。
- **region。** store は設定した region（既定 `local`）だけを受け持ち、それ以外の region への put は `RegionNotServed` で拒否する（別の場所に黙って置かない）。
- **暗号化。** AES-256-GCM、object ごとに乱数 96 bit nonce。鍵は `[objects] key_file`（64 hex、0600 必須）か `key_env`。metadata には `encryption = {algorithm: "AES-256-GCM", key_id: "k1-<16 hex>"}`（鍵の fingerprint）を記録し、鍵そのものは書かない。AAD に object id・tenant・region・key id・平文 digest・size を入れるので、ciphertext を別 tenant の directory に移す、metadata の digest を書き換える、のどちらも復号失敗（`Integrity`）になる。
- **digest。** put 時に平文の SHA-256 を記録し、読むたびに復号後の平文と照合する。一致しなければ bytes を返さない。
- **上限。** object ごと `max_object_bytes`（既定 8 MiB）→ `TooLarge`、tenant ごと `tenant_quota_bytes`（既定 1 GiB、全 region 合計）→ `QuotaExceeded`。どちらも何も書かない。
- **書き込み順。** data を書いて fsync → rename、metadata を書いて fsync → rename（commit point）。metadata の無い data は存在しない object で、GC が grace 後に消す。

### 5. retention / GC

- object は `expires_at`（put 時の TTL、既定 7 日）を持つが、store は自分では消さない。
- 台帳に `object_refs(object_id, tenant, region, invocation_id)` と `object_tombstones` を置く（migration 005）。`attach_object` は invocation と object の tenant が違えば `Refused`。
- GC（`ObjectGc::run`、gateway が `[objects] gc_interval_seconds` ごと）は候補（期限切れ、または `orphan_grace_seconds` より古い object、grace より古い書きかけ）ごとに `claim_for_collection` を 1 トランザクションで呼ぶ:
  - **非 terminal の invocation が参照していれば、TTL に関係なく残す**（`InUse`）。参照行の invocation が台帳に無い場合も残す。
  - 期限前で、terminal の invocation が参照している object は orphan ではない（`Referenced`、TTL を待つ）。
  - それ以外は tombstone を書いてから file を消し、参照行を消す。tombstone は残す。
- **put と invocation insert の競合。** 「object を put → invocation を insert → attach」は 1 トランザクションではない。(a) grace 内なら GC は候補にしない、(b) grace を過ぎて GC が先に tombstone を commit すれば、後から来た attach は `Refused` になり（file が消えた後も tombstone で拒否）、invocation が消えた object を指すことはない、(c) attach が先に commit すれば GC は `InUse` を見る。tombstone は 30 日で消し、それより古い id（ULID の時刻）の attach は常に拒否するので、tombstone の削除で競合が再び開くことはない。PLT-4639 の outbox はこの attach を invocation insert と同じトランザクションに入れる。

### 6. 保存先と複製（HA ではない）

| 対象 | 実際の保存先 | 複製 | 失うもの |
|---|---|---|---|
| queue（JetStream） | nats-server を動かす 1 host の local disk（`<state>/jetstream`、既定 `target/queue/nats/jetstream`） | なし（`num_replicas = 1`、cluster なし） | process の crash / kill -9 では失わない（実測）。disk・host の喪失、fsync を無視する storage では失う。server が止まっている間は publish も配送もできない |
| queue（SQLite） | `<data_dir>/queue.db` | なし | 同上。1 file を複数 host で共有する構成は非対応 |
| object | gateway の host の `<data_dir>/objects` | なし（region 間・node 間のコピーなし） | disk・host の喪失で全 object。backup・PITR なし |
| 参照と tombstone | `state.db` | なし（ADR-0003） | 同上 |

**この構成は HA でも region 障害耐性でもない。** region label は「どこに置いたか・どこに置いてはいけないか」を区別するための境界で、複製先ではない。HA にするには JetStream cluster（3 node 以上、`num_replicas = 3`、RAFT quorum）と、複製と region 間の配置を持つ object storage（S3 互換）が要り、どちらも本 ADR の範囲外で未検証。

## 結果（consequences）

- 既定（`[queue] backend = "none"`、`[objects] backend = "none"`）では何も起動・接続せず、同期 invoke の gateway は従来と同じに動く（E2E で確認）。
- gateway を `[queue] backend = "nats"` で起動すると、JetStream に接続できない・認証に失敗する場合は起動しない（queue なしで動き続けない）。
- `sync_interval: always` は publish ごとに fsync するので throughput は下がる。計測はしていない。
- object store の quota 判定は 1 プロセス内の lock で直列化する。同じ `root` を複数 gateway で共有すると、それぞれが最大 1 object ぶん quota を超えうる（共有は非対応）。quota と GC の列挙は metadata を全走査する（object 数に比例）。
- 鍵の rotation は未実装（読めるのは現在の鍵で書いた object だけで、別の鍵の object は `Encryption` エラーで鍵 id を示す）。

## 検証

| 受入条件 | テスト / 記録 |
|---|---|
| queue 再起動後も永続化済み message を読める | `scripts/queue/verify.sh` §2（publish 100、10 ACK、10 in-flight、kill -9、再起動、未 ACK 90 件すべて配送・うち 10 件再配送）。`durable::sqlite_queue::tests::committed_messages_survive_a_reopen_of_the_file`、契約 `unacked_messages_survive_a_reopen`（JetStream / SQLite） |
| 未認証アクセスの拒否 | verify §1（匿名、誤 password）、`queue-nats` `tests::nats_refuses_unauthenticated_and_wrong_credentials`、`durable::tests::nats_config_refuses_anonymous_connections` |
| tenant 越境参照の拒否 | `durable::tests::objects_are_invisible_across_tenants`、`repository::object_contract_tests::{memory,sqlite}::an_object_reference_never_crosses_a_tenant` |
| 上限超過の拒否 | verify §3（max_msgs 5 に 8 件 → 5 件保存・3 件 `queue_full`、既存 5 件は無傷、`message_too_large`）、契約 `a_full_stream_refuses_the_publish_and_keeps_what_it_has`、`durable::tests::object_size_and_tenant_quota_are_enforced` |
| 保持期限 | verify §4（`max_age` 2 s で 3 件 → 0 件）、契約 `messages_older_than_max_age_are_removed`、`durable::tests::gc_collects_orphans_only_after_the_grace_period` |
| retention / GC が未完了 Invocation の object を消さない | `durable::tests::{gc_never_collects_objects_of_unfinished_invocations, gc_and_attach_race_never_leave_a_dangling_reference}`、`repository::object_contract_tests::*::{a_non_terminal_reference_protects_an_expired_object, an_attach_after_a_collection_claim_is_refused}`（スレッド競合を含む） |
| 改竄の検出 | `durable::tests::tampered_objects_fail_verification`（ciphertext の 1 bit 反転、metadata の digest 書き換え、data 欠落） |

## 非対象

- `invokeAsync`、outbox、dispatcher、retry、DLQ、cron、webhook（PLT-4639〜4641）。
- 本番 queue の置き換え、JetStream cluster、TLS、account JWT / operator mode。
- S3 互換 object adapter、鍵 rotation、KMS、backup。
- queue 側の tenant ごとの quota（stream 全体の上限だけ。tenant 間の公平性は PLT-4640 以降）。

## 参照

- ADR-0003（台帳）、ADR-0007（設定配信）
- `docs/architecture.md` §4「durable queue と object store（PLT-4638）」
- `docs/threat-model.md` B6、T27〜T30、§14-11
