# Runtime protocol（host ↔ guest bridge ↔ user process）

正本は `crates/protocol`。ここでは順序と責任を固定する。

## A. host ↔ bridge（`wire.rs`）

- transport: Firecracker は vsock（guest → host CID 2, port 5000。host は `<vsock_uds>_5000` で listen）。process provider は unix socket（bridge が `--transport unix --path` で接続）。
- frame: `u32 BE length` + JSON。最大 8 MiB（`MAX_FRAME_BYTES`）。`type` tag で判別。未知 type はエラー、未知 field は無視。
- version: `PROTOCOL_VERSION` は handshake で**完全一致**が要求される。v1 → **v2**（PLT-4633）で `Ping` / `Pong` と `Invoke.remaining_ms` が入った。未知 type がエラーである以上 v1 の bridge に `Ping` を送ってはならず、v1 の bridge は絶対 deadline から guest 側 deadline を計算してしまうため、この追加は version を上げて隔離する。
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

## B. bridge ↔ user process（Runtime API, `runtime_api.rs`）

`TACHYON_RUNTIME_API=http://127.0.0.1:<port>`（Firecracker guest は 9001、process provider は空きポート）。

| method/path | 意味 | 応答 |
|---|---|---|
| `GET /runtime/v1/next` | 次のイベントを long-poll。headers: `tachyon-invocation-id`, `tachyon-attempt-id`, `tachyon-epoch`, `tachyon-deadline-ms`（**guest の時計**での絶対時刻。bridge が `Invoke.remaining_ms` から計算する）, `tachyon-trace-id`, `tachyon-event-type`。body = payload JSON | 200 / Shutdown 時 410 |
| `POST /runtime/v1/invocations/{attempt_id}/response` | 結果 JSON | 202 / 404 未知 attempt / 409 完了済み / 413 過大（body または canonical JSON が上限超過。attempt は `Error{response_too_large}` `Runtime.ResponseTooLarge` で完了済み） |
| `POST /runtime/v1/invocations/{attempt_id}/error` | `RuntimeErrorReport{error_type, message, stack_trace}`（body は `MAX_ERROR_REPORT_BYTES` = 1 MiB まで） | 202 / 404 / 409 / 413 過大（attempt は `Error{handler}` `Runtime.ErrorReportTooLarge` で完了済み） |
| `POST /runtime/v1/init/error` | 初期化失敗（body は 1 MiB まで） | 202 / 413 過大 |
| `POST /runtime/v1/ready` | 準備完了（冪等） | 202 |

- 413 を返す前に bridge は宣言された body を 16 MiB まで（最大 5 秒）読み捨てる。body を書き切ってから応答を読む client も broken pipe ではなく 413 を受け取り、処理を続けられる。SDK は 413 を「bridge が attempt を完了済み」として扱う。
- SDK は error report を送る前に `error_type` 256 B、`message` 64 KiB、`stack_trace` 256 KiB に文字境界で切り詰め（`...[truncated N bytes]` を付ける）、escape 後も 1 MiB を超える場合は `stack_trace` を落として `message` を 16 KiB にする。

event_type:
- `tachyon.invoke.v1`: payload は任意 JSON。応答も任意 JSON。
- `tachyon.http.v1`: payload は `HttpRequestEvent`、応答は `HttpResponsePayload`。SDK の `serve_http(router)` は event を `http::Request` に変換し `tower::Service` として router を呼ぶ（ローカル TCP は使わない）。
  - `HttpRequestEvent.path` は受信した request-target の path そのもの: percent-encoded のまま（decode しない）、query を含まず、`/` で始まる。`query` も `?` を除いた raw 文字列。decode は user の router が 1 回だけ行う（`%2F` と `/` は区別される）。
  - SDK は `path` をそのまま router に渡す。防御として URI path に使えないバイト（RFC 3986 の pchar と `/` 以外。既存の `%XX` は保持）だけを percent-encode するので、規約外の path でも `http::Uri` の構築に失敗せず、`?` / `#` で query や fragment に化けない。

SDK のエラー型: handler の `Err` → `Handler.Error`、panic → `Runtime.Panic`（`catch_unwind`）、`serve_http` に非 http event → `Runtime.UnsupportedEvent`。

## C. Firecracker guest 規約（providers/firecracker と runtime-bridge の合意事項）

- rootfs（読み取り専用 base）: `/sbin/tachyon-init` = runtime-bridge の static musl バイナリ。空ディレクトリ `/proc /sys /dev /tmp /function`。
- function drive（環境ごとに `mkfs.ext4 -d` で生成、read-only）: `/app` = artifact バイナリ (0755)。guest では `/function` に mount → entrypoint は `/function/app`。
- kernel cmdline: `console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init tachyon.env_id=<env_id> tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb`
  （aarch64 は `keep_bootcon` を追加）。
- bridge の `--init` モード（PID 1）: `/proc` `/sys` `/dev`(devtmpfs) `/tmp`(tmpfs) を mount → `/dev/vdb` を `/function` に ro mount → `/proc/cmdline` から `tachyon.*` を読む → `/proc/sys/kernel/random/boot_id` を読む → vsock で host に接続 → 通常処理 → 終了時に `reboot(RB_POWER_OFF)`。
- Firecracker API 順序: `PUT /machine-config` → `PUT /boot-source` → `PUT /drives/rootfs` (ro, root) → `PUT /drives/function` (ro) → `PUT /vsock {guest_cid: 3, uds_path}` → `PUT /actions InstanceStart`。host は InstanceStart 前に `<uds_path>_5000` で listen しておく。
- idle 休止・再開（PLT-4633。環境 pool だけが呼ぶ）: 休止は `PATCH /vm {"state": "Paused"}`、再開は `PATCH /vm {"state": "Resumed"}`。vsock device と bridge の接続は休止をまたいで保たれるので、再開後に再 handshake はしない（pool 側の readiness 検査は再開の**後**に `Ping` / `Pong` で行う）。どちらも冪等で、「**要求した状態に**すでにある」という fault だけを成功として扱う（`Resumed` を要求して「paused」と言われたのは失敗である。解釈できない fault も失敗として扱う）。VMM プロセスが死んでいる / API socket が無い / 環境が存在しない場合はそれぞれ別の error を返し、pool は再利用をやめて cold start に落ちる。呼ばれるのは provider が `idle_quiesce` / `idle_resume` を申告し、`[pool]` の gate が開いたときだけである（`docs/architecture.md` §4）。
- terminate: Shutdown frame → 2 秒 → firecracker プロセスに SIGKILL → API socket / vsock uds / function drive / workdir を削除。ただし休止中の環境（pool が持っているもの）は `TerminateReason::Quiesced` で終わらせる: vCPU が止まっている guest は Shutdown frame を読めないので frame は送らず、猶予も待たずに SIGKILL する。
- network は P1 では設定しない（egress none）。tap を作らない。
