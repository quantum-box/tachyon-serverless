# Runtime protocol（host ↔ guest bridge ↔ user process）

正本は `crates/protocol`。ここでは順序と責任を固定する。

## A. host ↔ bridge（`wire.rs`）

- transport: Firecracker は vsock（guest → host CID 2, port 5000。host は `<vsock_uds>_5000` で listen）。process provider は unix socket（bridge が `--transport unix --path` で接続）。
- frame: `u32 BE length` + JSON。最大 8 MiB（`MAX_FRAME_BYTES`）。`type` tag で判別。未知 type はエラー、未知 field は無視。
- version: v1 → **v2**（PLT-4633）で `Ping` / `Pong` と `Invoke.remaining_ms` が入った。未知 type がエラーである以上 v1 の bridge に `Ping` を送ってはならず、v1 の bridge は絶対 deadline から guest 側 deadline を計算してしまうため、この追加は version を上げて隔離した。v2 → **v3**（PLT-4653、X1 実験）は restore frame だけを足した（§A-X1）。
- **version の交渉**（v3 から）: host は `Hello.protocol_version` が `MIN_PROTOCOL_VERSION`（2）〜`PROTOCOL_VERSION`（3）なら受け入れ、それ以外は `HelloReject`。session は **guest の version で話す**: v3 で足した frame（`CheckpointWaiting` / `Reconnect` / `Restore`）と `HelloAck.snapshot_hold` は v3 の guest にしか送らない / 期待しない（v2 の guest に hold を求める handshake は host 側で拒否）。bridge は feature `experimental-restore` 付きで build したときだけ 3 を名乗り、既定の build は 2 のまま（frame も fixture も v2 と同一）。つまり v2 の bridge と新しい host、v3 の bridge と新しい host はどちらも動く。v2 だけを知る古い host に v3 の bridge を当てると完全一致の検査で拒否されるので、host を先に上げる。golden fixture は v2 の `guest_hello.json` / `host_hello_ack.json` を変えずに残し、v3 分を別 file で持つ（`crates/protocol/tests/golden/wire/*v3*`、`*snapshot_hold*`、`guest_checkpoint_waiting.json`、`guest_reconnect.json`、`host_restore.json`）。
- response payload の上限は canonical JSON（frame に載る再 serialize 後のバイト数）で `min(HelloAck.max_response_bytes, MAX_RESPONSE_PAYLOAD_BYTES)`。`MAX_RESPONSE_PAYLOAD_BYTES` = `MAX_FRAME_BYTES − FRAME_ENVELOPE_HEADROOM`（64 KiB）で、bridge は HelloAck の値をこれに clamp する。host は設定検証でこれを超える `max_response_bytes` を拒否すべき。
- bridge は encode できない frame で session を止めない。過大な `Response` は同じ `(attempt_id, epoch)` の `Error{response_too_large}` に、過大な `Error` は message を落とした `Error` に置き換えて送り、それ以外は破棄して続行する。frame の書き込みが I/O で失敗したら user process を SIGKILL して exit 4。
- `HostMessage` の `Debug` は `HelloAck.env` を `<N vars, redacted>` と表示する。それでも frame そのものはログに出さない。

```
bridge ──Hello{protocol_version, bridge_version, environment_id, guest_boot_id, architecture}──▶ host
bridge ◀─HelloAck{environment_id, epoch, entrypoint, args, env, working_dir, init_timeout_ms,
                  max_response_bytes, max_log_line_bytes}── host       (不一致なら HelloReject → bridge exit 2)
bridge: Runtime API HTTP server 起動 → user process 起動 (env += TACHYON_RUNTIME_API, TACHYON_ENVIRONMENT_ID)
bridge ──Log{phase: init, ...}*──▶ host
bridge ──Ready{init_ms}──▶ host          (POST /runtime/v1/ready または最初の GET /next で 1 回だけ)
   | user process が Ready 前に終了 → InitError{exit_code} を送って接続を閉じ exit 3
   | POST /runtime/v1/init/error     → InitError{error_type, message} を送って接続を閉じ exit 3
host ──Invoke{invocation_id, attempt_id, epoch, event_type, deadline_ms, remaining_ms, trace_id, payload}──▶ bridge
   | in-flight が既にある → Error{kind: protocol} を新 attempt に返す（1 環境 1 実行）
bridge: 次の GET /next に配る。Log{phase: handler, attempt_id}* を送る
bridge ──Response{attempt_id, epoch, payload, handler_ms}──▶ host
   | または Error{attempt_id, epoch, error: handler|panic|crash|protocol|response_too_large, ...}
   | user process が in-flight 中に終了 → Error{crash{exit_code, signal}} → Exited → 接続を閉じ exit 0
host ──Cancel{attempt_id, grace_ms}──▶ bridge   (SIGTERM → grace → SIGKILL。結果は host が Timeout/Cancelled と判定済み)
host ──Shutdown{reason}──▶ bridge               (SIGTERM → 2s → SIGKILL → 接続を閉じ exit 0。待機中に bridge 自身が SIGTERM を受けたら即 SIGKILL)
bridge ──Heartbeat{ts_ms}──▶ host  (5 秒ごと。host は無視してよい)
host ──Ping{nonce}──▶ bridge                    (liveness 検査。user process は関与しない)
bridge ──Pong{nonce}──▶ host                    (frame loop が即答する。休止中の guest は答えられない)
```

- `Ping` / `Pong`（v2）: host が環境 pool から環境を取り出して再開した直後の **readiness 検査**に使う（`docs/architecture.md` §4）。bridge は frame loop で `Pong{nonce}` を返すだけで user process には渡さない。したがって「guest が scheduling されていて bridge が読んでいる」ことの証拠になる。user process が死んでいる場合は `Exited` が queue に載っているので、host 側の drain がそれを見る。答えが来なければ host は環境を retire して cold start に落ちる（bound は 500 ms）。
- `Invoke.deadline_ms` は **host の時計**での絶対時刻で、enforcement も host 側が行う。`remaining_ms` は frame を書いた時点の残り時間で、bridge が user process に見せる deadline は `guest の現在時刻 + remaining_ms` で計算する。休止していた guest の時計は止まっているため、絶対時刻をそのまま渡すと休止時間のぶんだけ余裕があるように見えてしまう（PLT-4633）。
- host は Lease の `(attempt_id, epoch)` に一致する Response/Error だけ受理する。
- host が deadline で先に判定した後に届く Response は無視される。
- bridge の exit code: 0 正常 / 2 handshake 拒否・protocol error / 3 init error / 4 transport 失敗。

### A-X1. 実験: restore frame（protocol v3、PLT-4653）

設計と判断は `docs/adr/0017-snapshot-manifest-and-clone.md`。gateway の `[snapshots] enabled`、revision の `restore.policy`、provider の snapshot capability がそろったときだけ使われる。

```
snapshot source（POST /v1/functions/{id}/snapshots）
bridge ──Hello{protocol_version: 3}──▶ host
bridge ◀─HelloAck{..., env: secret なし, snapshot_hold: true}── host
bridge: restore link が doorbell（guest vsock port 5001 の listen）を張る
user: bootstrap → checkpoint → GET continue（hold されるので答えない）
bridge ──CheckpointWaiting{lifecycle_phase: "checkpoint", lifecycle_version, after_restore_ran: false}──▶ host
host: PATCH /vm Paused → PUT /snapshot/create → scratch を paused のまま copy → source を terminate（resume しない）

clone（restore = prefer | require の invoke）
host: 検証済み snapshot を新しい jail / cgroup / scratch copy で PUT /snapshot/load → egress gate → resume → doorbell（CONNECT 5001）
bridge: 古い接続を捨てて再接続
bridge ──Reconnect{protocol_version: 3, environment_id: <source>, guest_boot_id: <source と同じ>, reconnects, lost}──▶ host
   | 最初の frame が Hello なら guest は cold boot した: host は HelloReject して restored に数えない
   | environment_id / boot id が snapshot の source と違えば拒否
bridge ◀─Restore{environment_id: <clone>, instance_id, generation, epoch, host_now_ms}── host
bridge: CLOCK_REALTIME = host_now_ms → continue に {"kind":"restored","instance_id","generation",...}
user: after_restore（identity・RNG・時計・接続）→ POST ready
bridge ──Ready──▶ host   （以降は §A と同じ。invoke は clone の epoch で来る）
```

- restore link（`crates/runtime-bridge/src/restore_link.rs`）は session と host 接続の間の frame pump。`snapshot_hold` の無い session では透過で、接続が切れれば従来どおり exit 4。hold 中だけ再接続（最大 120 s）し、`Restore` を受けたら透過に戻る。再接続していない接続に来た `Restore` は無視する（snapshot されていない guest に identity を渡さない）。
- restore の合図は doorbell（ADR-0015 の実測で load → 再接続 100 ms 台）と、doorbell が届かないときの次の write 失敗（heartbeat、最大 5 s）。
- guest の `TACHYON_ENVIRONMENT_ID` と kernel cmdline の env id は source のもの。host は clone を自分の環境 id で扱い、`Invoke` の epoch で結果を照合する。
- `Restore` は secret を運ばない。X1 は secret binding のある revision の snapshot を拒否する（restore 後に secret を渡す経路は未実装）。
- boot identity: clone の `guest_boot_id` は全 copy で同じなので、host は evidence の boot id を `<source boot id>/<instance id>` にする（ADR-0011 の `same_boot` を clone に適用しない）。

## B. bridge ↔ user process（Runtime API, `runtime_api.rs`）

`TACHYON_RUNTIME_API=http://127.0.0.1:<port>`（Firecracker guest は 9001、process provider は空きポート）。

| method/path | 意味 | 応答 |
|---|---|---|
| `GET /runtime/v1/next` | 次のイベントを long-poll。headers: `tachyon-invocation-id`, `tachyon-attempt-id`, `tachyon-epoch`, `tachyon-deadline-ms`（**guest の時計**での絶対時刻。bridge が `Invoke.remaining_ms` から計算する）, `tachyon-trace-id`, `tachyon-event-type`。body = payload JSON | 200 / Shutdown 時 410 |
| `POST /runtime/v1/invocations/{attempt_id}/response` | 結果 JSON | 202 / 404 未知 attempt / 409 完了済み / 413 過大（body または canonical JSON が上限超過。attempt は `Error{response_too_large}` `Runtime.ResponseTooLarge` で完了済み） |
| `POST /runtime/v1/invocations/{attempt_id}/error` | `RuntimeErrorReport{error_type, message, stack_trace}`（body は `MAX_ERROR_REPORT_BYTES` = 1 MiB まで） | 202 / 404 / 409 / 413 過大（attempt は `Error{handler}` `Runtime.ErrorReportTooLarge` で完了済み） |
| `POST /runtime/v1/init/error` | 初期化失敗（body は 1 MiB まで） | 202 / 413 過大 |
| `POST /runtime/v1/ready` | 準備完了（冪等） | 202 / 409（実験 lifecycle が開いていて continue 前。§B-X1） |

- 413 を返す前に bridge は宣言された body を 16 MiB まで（最大 5 秒）読み捨てる。body を書き切ってから応答を読む client も broken pipe ではなく 413 を受け取り、処理を続けられる。SDK は 413 を「bridge が attempt を完了済み」として扱う。
- SDK は error report を送る前に `error_type` 256 B、`message` 64 KiB、`stack_trace` 256 KiB に文字境界で切り詰め（`...[truncated N bytes]` を付ける）、escape 後も 1 MiB を超える場合は `stack_trace` を落として `message` を 16 KiB にする。

event_type:
- `tachyon.invoke.v1`: payload は任意 JSON。応答も任意 JSON。
- `tachyon.http.v1`: payload は `HttpRequestEvent`、応答は `HttpResponsePayload`。SDK の `serve_http(router)` は event を `http::Request` に変換し `tower::Service` として router を呼ぶ（ローカル TCP は使わない）。
  - `HttpRequestEvent.path` は受信した request-target の path そのもの: percent-encoded のまま（decode しない）、query を含まず、`/` で始まる。`query` も `?` を除いた raw 文字列。decode は user の router が 1 回だけ行う（`%2F` と `/` は区別される）。
  - SDK は `path` をそのまま router に渡す。防御として URI path に使えないバイト（RFC 3986 の pchar と `/` 以外。既存の `%XX` は保持）だけを percent-encode するので、規約外の path でも `http::Uri` の構築に失敗せず、`?` / `#` で query や fragment に化けない。

SDK のエラー型: handler の `Err` → `Handler.Error`、panic → `Runtime.Panic`（`catch_unwind`）、`serve_http` に非 http event → `Runtime.UnsupportedEvent`。

### B-X1. 実験: 初期化保存点と復元後 hook（PLT-4651）

**実験 API**。SDK は cargo feature `experimental-restore`（既定 off）の `tachyon_serverless_sdk::lifecycle` だけがこれを使う。feature を有効にしなくても bridge はこの path を提供するが、呼ばない process（P1 の `run` / `serve_http` を含む）には §B の表どおりの API しか見えない。snapshot の取得・復元は PLT-4653 で実験経路として実装した（§A-X1、ADR-0017）。

目的は、snapshot の全 copy で共有してよい**再利用可能な初期化状態**と、**instance ごとに作り直す状態**を分けること。

| method/path | 意味 | 応答 |
|---|---|---|
| `POST /runtime/v1/lifecycle/bootstrap` | lifecycle を開く。これから再利用可能な状態を作る | 202 / 409（ready 後、または 2 回目） |
| `POST /runtime/v1/lifecycle/checkpoint` | 再利用可能な状態ができた。ここから snapshot を取ってよい | 202 / 409（bootstrap 中でない） |
| `GET /runtime/v1/lifecycle/continue` | どう続けるかを long-poll。body は `{"kind":"cold"}` または `{"kind":"restored","instance_id","restored_at_ms","generation"}`、header `tachyon-lifecycle-version: 1` | 200 / 409（checkpoint 前、ready 後）。再試行には同じ答えを返す |
| `POST /runtime/v1/lifecycle/error` | hook の失敗。body は `RuntimeErrorReport`（1 MiB まで） | 202 / 409（lifecycle 外） / 413 |

```
user: POST bootstrap → bootstrap()（同期。async runtime・secret・socket・thread・一意な値を作らない）
user: POST checkpoint                         ← ここ以降なら snapshot を取ってよい
user: GET continue ─(RestoreSource が答えるまで block)→ cold | restored
user: 時計を読む → Tokio runtime を作る → after_restore(fixed, ctx)（identity・RNG・認証・接続・常駐 task）
user: POST /runtime/v1/ready                  → bridge ──Ready{init_ms}──▶ host
```

- **Ready の gate**: lifecycle が開いている間、bridge は `continue` に答えるまで `POST /ready` を 409 にし、`GET /next` は `ready` が明示的に来るまで 409 にする（lifecycle 内では `next` は ready を意味しない）。したがって bootstrap / checkpoint 待ち / after_restore の途中で `Ready` frame が host に届くことはない。
- **失敗の型**（`InitError.error_type`）。bridge が自分の観測した phase で決め、process の申告した型は `message` の先頭（`<型>: <message>`）に残す。
  - `Runtime.PreCheckpointFailed`: bootstrap 中（checkpoint 後で continue 前を含む）に `lifecycle/error`。
  - `Runtime.AfterRestoreFailed`: continue 後、ready 前に `lifecycle/error`。
  - `Runtime.PreCheckpointTimeout`: init deadline 到達時に bootstrap 中、または checkpoint 済みでまだ continue を要求していない。
  - `Runtime.CheckpointTimeout`: continue を要求済みで、bridge（provider 側の snapshot / restore）が答えていない。function の責任ではない。
  - `Runtime.AfterRestoreTimeout`: continue 後、ready 前に init deadline 到達。
  - lifecycle を開かなかった process の init timeout は従来どおり `Runtime.InitTimeout`。process の異常終了は従来どおり `Runtime.InitExit`。
- **init deadline**: cold は P1 と同じ 1 本（`HelloAck.init_timeout_ms`）で bootstrap から ready までを覆い、X1 で延びない。`restored` の答えを返したときだけ bridge は deadline を張り直す（bootstrap に使った時間は snapshot 元の process が使ったもの）。host 側の init deadline（`Host.InitTimeout`）は変えていないので、restore の budget を host がどう持つかは PLT-4653 で決める。
- **continue の答え**: bridge の `RestoreSource` が決める。既定の bridge は `NoSnapshot`（即座に `cold`）、feature `experimental-restore` の bridge は restore link の `LinkRestore`（`HelloAck.snapshot_hold` が無ければ即座に `cold`、あれば `CheckpointWaiting` を送って `Restore` まで保留）。**通常起動も同じ API 経路を通る**。
- **互換性の規則**:
  - lifecycle 自体は host↔bridge frame を変えていない（`InitError` の `error_type` の値を増やしただけ）。restore の frame は PLT-4653 で protocol v3 として足した（§A「version の交渉」、§A-X1）。
  - Runtime API への追加は additive。lifecycle を使わない process の挙動は変わらない。lifecycle を使う SDK が古い bridge に当たると `bootstrap` が 404 になり、SDK はその時点で（bootstrap hook を実行せずに）エラーで終了する。
  - PLT-4653 は新しい frame を足したので `PROTOCOL_VERSION` を 3 に上げ、host は 2 と 3 を受け入れて guest の version で話す（§A）。`HelloAck.snapshot_hold` は省略可能な field で、無い / false なら常に cold。
- **snapshot-safe を主張しない**: この API は function が「copy 間で共有してはいけない状態」を置く場所を用意するだけである。任意のライブラリ（乱数 seed・hostname・monotonic clock の基準・fd・thread pool・TLS session を初期化時に握るもの）や multithread runtime が透過的に snapshot-safe になるわけではない。SDK が保証するのは、SDK 自身が checkpoint 前に async runtime・thread・signal handler・永続接続を作らないこと（lifecycle の呼び出しは 1 リクエスト 1 接続の blocking HTTP）と、after_restore が成功するまで ready を送らないことだけである。hook の中身は function 作者の責任。
- **環境変数**: process の環境は process image の一部なので、restore された copy の `std::env` は snapshot 元の値である。restore 後に新しい secret を渡す経路は無いので、PLT-4653 は secret binding のある revision の snapshot と restore を拒否する（source の `HelloAck.env` にも secret は入らない）。

## C. Firecracker guest 規約（providers/firecracker と runtime-bridge の合意事項）

- rootfs（読み取り専用 base）: `/sbin/tachyon-init` = runtime-bridge の static musl バイナリ。空ディレクトリ `/proc /sys /dev /tmp /function`。`/etc/resolv.conf` は `/proc/net/pnp` への symlink（PLT-4622。NIC の無い guest では中身が無く、musl は従来どおり 127.0.0.1 を見る）。
- function drive（環境ごとに `mkfs.ext4 -d` で生成、read-only）: `/app` = artifact バイナリ (0755)。guest では `/function` に mount → entrypoint は `/function/app`。生成後に host の `stage/` は消す。
- scratch drive（PLT-4622。環境ごとに生成、read-write）: revision の `ephemeral_storage_mib` ちょうどの空の ext4（`mkfs.ext4 -q -F -b 4096 -m 0 -O ^has_journal -E nodiscard,lazy_itable_init=0 -L tachyon-scratch <image> <N>M`）。image は `fallocate` で確保する。guest では `/dev/vdc` を `/tmp` に `nosuid,nodev` で mount し、mode 1777。guest が書ける host ディスク上の領域はこれだけである。
- kernel cmdline: `console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init tachyon.env_id=<env_id> tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb tachyon.scratch_dev=/dev/vdc`
  （aarch64 は `keep_bootcon` を追加）。egress `restricted` / `public-web` の環境だけ、その後に `ip=<guest_ip>::<host_tap_ip>:255.255.255.252::eth0:off[:<resolver>] ipv6.disable=1` を追加する（resolver は `public-web` だけ。kernel が `/proc/net/pnp` に書く）。
- bridge の `--init` モード（PID 1）: `/proc` `/sys` `/dev`(devtmpfs) を mount → `/proc/cmdline` から `tachyon.*` を読む → `/dev/vdb` を `/function` に ro mount → `tachyon.scratch_dev` を `/tmp` に rw mount（無ければ `/tmp` は tmpfs。指定があって mount できなければ init error） → `/proc/sys/kernel/random/boot_id` を読む → vsock で host に接続 → 通常処理 → 終了時に `reboot(RB_POWER_OFF)`。
- Firecracker API 順序: `PUT /machine-config` → `PUT /boot-source` → `PUT /drives/rootfs` (ro, root) → `PUT /drives/function` (ro) → `PUT /drives/scratch` (rw) → `PUT /vsock {guest_cid: 3, uds_path}` → （egress `restricted` / `public-web` のときだけ）`PUT /network-interfaces/eth0 {iface_id: "eth0", host_dev_name: <tap>, guest_mac: "06:00:<guest ip の 4 byte>"}` → `GET /vm/config`（egress gate: `none` は `network-interfaces` が空配列、それ以外は検証済み policy の tap を指す `eth0` 1 本だけで、どちらも `mmds-config` が null でなければ起動しない） → （NIC があるとき）nftables の policy をもう一度読み戻して一致を確認 → `PUT /actions InstanceStart`。tap と policy は Firecracker の起動前（vsock listener の bind 前）に作って読み戻し済みで、`PUT /network-interfaces` はその後にしか送らない（`docs/adr/0005-egress-profiles.md`）。host は InstanceStart 前に `<uds_path>_5000` で listen しておく。
- host 側の上限（PLT-4622）: Firecracker の stdout/stderr（serial console）は pipe で受けて `console.log` に先頭 4 MiB だけ書き、残りは読み捨てる。`fc.log` は 1 秒ごとの watchdog が 4 MiB を超えたら truncate する。環境作成前に、staged artifact + drive 2 本 + ログ上限 + 512 MiB の空きが workdir に無ければ何も作らず `Unavailable`。
- idle 休止・再開（PLT-4633。環境 pool だけが呼ぶ）: 休止は `PATCH /vm {"state": "Paused"}`、再開は `PATCH /vm {"state": "Resumed"}`。vsock device と bridge の接続は休止をまたいで保たれるので、再開後に再 handshake はしない（pool 側の readiness 検査は再開の**後**に `Ping` / `Pong` で行う）。どちらも冪等で、「**要求した状態に**すでにある」という fault だけを成功として扱う（`Resumed` を要求して「paused」と言われたのは失敗である。解釈できない fault も失敗として扱う）。VMM プロセスが死んでいる / API socket が無い / 環境が存在しない場合はそれぞれ別の error を返し、pool は再利用をやめて cold start に落ちる。呼ばれるのは provider が `idle_quiesce` / `idle_resume` を申告し、`[pool]` の gate が開いたときだけである（`docs/architecture.md` §4）。
- terminate: Shutdown frame → 2 秒 → firecracker プロセスに SIGKILL → API socket / vsock uds / function drive / workdir を削除。ただし休止中の環境（pool が持っているもの）は `TerminateReason::Quiesced` で終わらせる: vCPU が止まっている guest は Shutdown frame を読めないので frame は送らず、猶予も待たずに SIGKILL する。
- network（PLT-4622、`docs/adr/0005-egress-profiles.md`）: egress `none` は NIC も tap も作らない。`restricted` / `public-web` は provider が tap `tsls<11 hex>` を作り、`guest_cidr` の /30 を割り当て（`<env_dir>/net.json`）、`table inet tachyon_egress` に環境 chain `g_<tap>` を入れて読み戻してから NIC を付ける。terminate は VMM 停止後に map 要素・chain・tap を消して読み戻しで確認し、lease が他に無ければ table も消す。起動時の reconcile（`list_environments`）は環境ディレクトリの無い tap / chain / map 要素を消す。
