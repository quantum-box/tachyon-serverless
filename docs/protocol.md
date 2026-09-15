# Runtime protocol（host ↔ guest bridge ↔ user process）

正本は `crates/protocol`。ここでは順序と責任を固定する。

## A. host ↔ bridge（`wire.rs`）

- transport: Firecracker は vsock（guest → host CID 2, port 5000。host は `<vsock_uds>_5000` で listen）。process provider は unix socket（bridge が `--transport unix --path` で接続）。
- frame: `u32 BE length` + JSON。最大 8 MiB。`type` tag で判別。未知 type はエラー、未知 field は無視。

```
bridge ──Hello{protocol_version, bridge_version, environment_id, guest_boot_id, architecture}──▶ host
bridge ◀─HelloAck{environment_id, epoch, entrypoint, args, env, working_dir, init_timeout_ms,
                  max_response_bytes, max_log_line_bytes}── host       (不一致なら HelloReject → bridge exit 2)
bridge: Runtime API HTTP server 起動 → user process 起動 (env += TACHYON_RUNTIME_API, TACHYON_ENVIRONMENT_ID)
bridge ──Log{phase: init, ...}*──▶ host
bridge ──Ready{init_ms}──▶ host          (POST /runtime/v1/ready または最初の GET /next で 1 回だけ)
   | user process が Ready 前に終了 → InitError{exit_code} を送って接続を閉じ exit 3
   | POST /runtime/v1/init/error     → InitError{error_type, message} を送って接続を閉じ exit 3
host ──Invoke{invocation_id, attempt_id, epoch, event_type, deadline_ms, trace_id, payload}──▶ bridge
   | in-flight が既にある → Error{kind: protocol} を新 attempt に返す（1 環境 1 実行）
bridge: 次の GET /next に配る。Log{phase: handler, attempt_id}* を送る
bridge ──Response{attempt_id, epoch, payload, handler_ms}──▶ host
   | または Error{attempt_id, epoch, error: handler|panic|crash|protocol|response_too_large, ...}
   | user process が in-flight 中に終了 → Error{crash{exit_code, signal}} → Exited → 接続を閉じ exit 0
host ──Cancel{attempt_id, grace_ms}──▶ bridge   (SIGTERM → grace → SIGKILL。結果は host が Timeout/Cancelled と判定済み)
host ──Shutdown{reason}──▶ bridge               (SIGTERM → 2s → SIGKILL → 接続を閉じ exit 0)
bridge ──Heartbeat{ts_ms}──▶ host  (5 秒ごと。host は無視してよい)
```

- host は Lease の `(attempt_id, epoch)` に一致する Response/Error だけ受理する。
- host が deadline で先に判定した後に届く Response は無視される。
- bridge の exit code: 0 正常 / 2 handshake 拒否・protocol error / 3 init error / 4 transport 失敗。

## B. bridge ↔ user process（Runtime API, `runtime_api.rs`）

`TACHYON_RUNTIME_API=http://127.0.0.1:<port>`（Firecracker guest は 9001、process provider は空きポート）。

| method/path | 意味 | 応答 |
|---|---|---|
| `GET /runtime/v1/next` | 次のイベントを long-poll。headers: `tachyon-invocation-id`, `tachyon-attempt-id`, `tachyon-epoch`, `tachyon-deadline-ms`, `tachyon-trace-id`, `tachyon-event-type`。body = payload JSON | 200 / Shutdown 時 410 |
| `POST /runtime/v1/invocations/{attempt_id}/response` | 結果 JSON | 202 / 404 未知 attempt / 409 完了済み / 413 過大 |
| `POST /runtime/v1/invocations/{attempt_id}/error` | `RuntimeErrorReport{error_type, message, stack_trace}` | 202 / 404 / 409 |
| `POST /runtime/v1/init/error` | 初期化失敗 | 202 |
| `POST /runtime/v1/ready` | 準備完了（冪等） | 202 |

event_type:
- `tachyon.invoke.v1`: payload は任意 JSON。応答も任意 JSON。
- `tachyon.http.v1`: payload は `HttpRequestEvent`、応答は `HttpResponsePayload`。SDK の `serve_http(router)` は event を `http::Request` に変換し `tower::Service` として router を呼ぶ（ローカル TCP は使わない）。

SDK のエラー型: handler の `Err` → `Handler.Error`、panic → `Runtime.Panic`（`catch_unwind`）、`serve_http` に非 http event → `Runtime.UnsupportedEvent`。

## C. Firecracker guest 規約（providers/firecracker と runtime-bridge の合意事項）

- rootfs（読み取り専用 base）: `/sbin/tachyon-init` = runtime-bridge の static musl バイナリ。空ディレクトリ `/proc /sys /dev /tmp /function`。
- function drive（環境ごとに `mkfs.ext4 -d` で生成、read-only）: `/app` = artifact バイナリ (0755)。guest では `/function` に mount → entrypoint は `/function/app`。
- kernel cmdline: `console=ttyS0 reboot=k panic=1 pci=off init=/sbin/tachyon-init tachyon.env_id=<env_id> tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb`
  （aarch64 は `keep_bootcon` を追加）。
- bridge の `--init` モード（PID 1）: `/proc` `/sys` `/dev`(devtmpfs) `/tmp`(tmpfs) を mount → `/dev/vdb` を `/function` に ro mount → `/proc/cmdline` から `tachyon.*` を読む → `/proc/sys/kernel/random/boot_id` を読む → vsock で host に接続 → 通常処理 → 終了時に `reboot(RB_POWER_OFF)`。
- Firecracker API 順序: `PUT /machine-config` → `PUT /boot-source` → `PUT /drives/rootfs` (ro, root) → `PUT /drives/function` (ro) → `PUT /vsock {guest_cid: 3, uds_path}` → `PUT /actions InstanceStart`。host は InstanceStart 前に `<uds_path>_5000` で listen しておく。
- terminate: Shutdown frame → 2 秒 → firecracker プロセスに SIGKILL → API socket / vsock uds / function drive / workdir を削除。
- network は P1 では設定しない（egress none）。tap を作らない。
