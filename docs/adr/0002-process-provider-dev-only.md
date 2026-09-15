# ADR-0002: 隔離なしの process provider を dev 専用として持つ

## ステータス

Accepted（2026-09-15）

## コンテキスト

- Firecracker provider は Linux + KVM を要する。開発機（macOS）と KVM の無い CI では動かない。
- pipeline（受付 → 環境作成 → bridge session → Invoke → 結果 → terminate）、bridge、SDK、CLI は provider に依存しない部分が大半で、KVM なしで開発・テストできるべきである。
- fake provider（`crates/providers/fake`）は duplex stream 上のスクリプト guest で、bridge も user process も起動しない。SDK と bridge の実物を通す経路が別に要る。
- 一方で「process で動いた」を「microVM で動いた」と誤認する事故は、プロトタイプの信頼性を根本から損なう（`docs/architecture.md` §1）。

## 決定

1. `crates/providers/process` を **隔離なしの dev 専用 provider** として持つ。user process は host の子プロセスとして起動し、bridge は unix socket（`--transport unix --path`）で host に接続する（`docs/protocol.md` §A）。
2. provider は `ProviderKind::Process` を返し、`Capabilities` は次で固定する。
   - `isolation = IsolationLevel::Process`
   - `dev_only = true`
   - `egress_none` / `egress_restricted` / `egress_public_web` / `enforce_resource_limits` / `idle_*` / `snapshot_*` = `Unsupported{reason}`
   - `create_terminate` / `observe` / `enforce_deadline` / `host_metering` = `Supported`（プロセス粒度での意味。隔離を意味しない）
3. guest env に `TACHYON_UNISOLATED=1`（`crates/protocol::env::UNISOLATED`）を必ず載せる。SDK / examples はこれを見て「隔離なし」を表示できる。
4. **guard**: gateway は `profile = "production"` のとき `Capabilities.dev_only == true` の provider を起動時に拒否し、プロセスを終了する（設定エラー）。`profile = "dev"` でも `GET /v1/provider` は `dev_only: true`、`isolation: "process"` を返し、CLI（`tsls`）は provider が dev_only のとき invoke 結果の表示に警告を付ける。
5. `BootEvidence` には `host_pid` と `details.provider = "process"` を入れ、`guest_boot_id` は入れない（host の boot_id は microVM の起動を示さないため）。`AttemptResponse.boot_evidence` を見れば process provider の結果だと分かる。

## process provider が証明するもの

- protocol の往復（`Hello` / `HelloAck` / `Ready` / `Invoke` / `Response` / `Error` / `Cancel` / `Shutdown`）と frame codec。
- bridge の状態機械（1 in-flight、`Ready` gate、`InitError`、exit code）。
- SDK（`run(handler)`、`serve_http(router)`）と Runtime API。
- pipeline の分類（`UserError` / `Crash` / `InitError` / `Timeout` / `QueueTimeout` / `Cancelled` / `OutcomeUnknown`）と Lease fencing。
- deadline による kill の **手順**（`Cancel` → grace → SIGKILL）。
- log の phase / stream / 上限。
- CLI と gateway の縦断（deploy / invoke / logs / rollback / dev）。

## process provider が証明しないもの

- tenant 間の隔離（filesystem、network、memory、pids、kernel）。`docs/threat-model.md` §13。
- `egress_none`（host の network をそのまま使う）。
- resource 上限の実効性（cgroup を設定しない）。
- 起動時間・init 時間（microVM の測定にならない）。
- VMM 固有の cleanup（API socket、vsock uds、drive）。process provider の cleanup 対象は子プロセス、unix socket、workdir だけ。
- rootfs / kernel / function drive の規約（`docs/protocol.md` §C）。
- `BootEvidence.guest_boot_id`。

## 結果

- CI（KVM なし）は fake provider と process provider で pipeline を回し、Firecracker provider のテストは KVM のある job に分ける。
- デモの証跡（PLT-4630）は Firecracker provider で取る。process provider の出力は証跡に使わない。
- `Capabilities` と `GET /v1/provider` の出力を docs / CLI が表示するため、「どの provider で動いたか」が常に見える。
- process provider に隔離機能（cgroup、namespace、seccomp）を後付けしない。必要なら別の provider（例: container）として `ExecutionProvider` を実装し、`IsolationLevel::Container` を返す。
