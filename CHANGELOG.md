# Changelog

このプロジェクトの主な変更点を記録します。

フォーマットは [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) に基づき、
バージョニングは [Semantic Versioning](https://semver.org/lang/ja/) に従います。

## [Unreleased]

### Added

- プロトタイプ最終受入の実行と、lab demo の再起動 phase（Linear PLT-4649）
  - `scripts/lab/lab.sh demo restart`（`demo all` では P2 と P3 の間）: gateway を SIGTERM で止めて起動し直し、台帳・`prod` alias・invocation log・schema version・startup reconcile・受付済み async（全件 terminal、attempt 上限）・cron の継続・利用量（async は 1 回だけ計上、動いていない関数は不変）を検査する。これでデモシナリオの「無負荷で 0 →再起動→ async / cron」が lab.sh だけで再現できる
  - Lima VM（aarch64 nested、Firecracker + jailer + cgroup required）で clean clone から一気通貫を実行: lab `demo all` 64/64、E2E 29/29、隔離 21/21、zero-scale 18/18、warm、usage 18 / budget 19、async dispatch 40 / triggers 39、故障マトリクス（process 全シナリオ・Firecracker 8 シナリオ）、benchmark 41/41（714 request）。証跡は `docs/evidence/final-acceptance-20260918T014751Z/`、判定は `docs/acceptance.md`「PLT-4649」、`docs/known-constraints-and-beta-gap.md` §9
  - 受入の実行中に見つけた計測・harness の不具合を修正: `zero-scale.sh` が `promised`（idle 環境への割当）を環境として二重に数えていた、`measure-isolation.sh` の NOISY 判定が環境の無い attempt で落ちていた、chaos の `nats_up` が nats-server の起動失敗を分かりにくい unbound variable として報告していた、Firecracker では成立しない `worker_user_process_kill_sync` が一覧に残っていた、process provider の故障マトリクスを root で流すと `chmod 000` の故障が入らなかった

- MIT License を追加
- README / CONTRIBUTING / CODE_OF_CONDUCT / SECURITY などのリポジトリ基本ドキュメントを追加
- GitHub の Issue / Pull Request テンプレートと Dependabot 設定を追加
- VMM の host 側 cgroup v2 上限・jailer・noisy neighbor の計測（Linear PLT-4622）
  - Firecracker provider が環境ごとに `<cgroup root>/tachyon/<env_id>` を作り、`cpu.max` を `cpu_millis` から（500 m → `50000 100000`、全 vCPU と VMM thread の合計）、`memory.max` を guest memory + `memory_overhead_mib`（既定 64）、`pids.max`（既定 64）で書いて読み戻す。VMM は fork と exec の間に自分でその cgroup に入り、`cgroup.procs` と `/proc/<pid>/cgroup` で所属を確認してから構成する。terminate は `cgroup.kill` → 統計をログ（`cgroup stats at teardown`）→ rmdir、起動時 reconcile は環境の無い cgroup を消す
  - `[provider.firecracker.cgroup] mode = required | best-effort | off`。`profile = "production"` の既定は `required` で、それ以外は設定エラー。委譲の無い host では preflight `host_cgroup` が失敗し環境作成は `Unavailable`。dev の既定 `best-effort` は使えなければ上限なしで起動し、`enforce_resource_limits` を `unverified` にする
  - `[provider.firecracker.jailer]`: VMM を Firecracker の jailer 経由で起動する（`<chroot_base>/<exec>/<instance id>/root` の chroot、uid / gid への降格、mount / PID namespace）。kernel・rootfs・function / scratch drive・`fc.log` を chroot に hard link し（scratch と log は jail の uid 所有 0600）、API socket と vsock UDS は chroot 内、egress の tap は jail の uid / gid を owner にして作る。terminate と起動時 reconcile が jail を消す。preflight `jailer`（root、同一 file system、binary 名）。`scripts/kvm/bootstrap.sh` が `.kvm/bin/jailer` を置き、`config/gateway.firecracker.toml` は jailer と `required` を有効にした（gateway は root で起動する）
  - `examples/isolation-probe` に `{"probe":"cpu"}` / `{"probe":"io"}` / `{"probe":"work"}`、`scripts/kvm/host-watch.sh`（VMM の cgroup・uid・capability・namespace・chroot・seccomp を host から記録）、`scripts/kvm/measure-isolation.sh` に HOST（exit 6）と NOISY（exit 5）を追加
- ephemeral storage の上限・host 側の上限・egress の起動ゲート（Linear PLT-4622）
  - Firecracker の guest の `/tmp` を、revision の `ephemeral_storage_mib` ちょうどの scratch drive（ext4、環境作成時に `fallocate` で確保）にした。rootfs と function drive は read-only のままなので、guest が書ける host ディスクはこの drive だけになる。`ephemeral_storage_mib` に下限 32 を追加（32..=2048）、CLI に `--ephemeral-storage-mib`
  - 環境ごとの host 側の成果物を上限付きにした: `console.log` は pipe 経由で 4 MiB まで、`fc.log` は watchdog で 4 MiB、`stage/` は function drive 作成後に削除。budget + 512 MiB の空きが無ければ何も書かずに `Unavailable`
  - egress gate: `InstanceStart` の前に API 計画と `GET /vm/config` を検査し、network interface や MMDS があれば起動しない
  - `examples/isolation-probe` に `{"probe":"disk"}`、`scripts/kvm/measure-isolation.sh` に DISK ステップ（exit 3）を追加。KVM 実測（`docs/evidence/isolation-20260917T011555Z/`）を経て `enforce_resource_limits` を `Supported` にした
- egress profile `restricted` / `public-web`（Linear PLT-4622、ADR-0005）
  - Firecracker provider が環境ごとに tap と provider 所有の `table inet tachyon_egress` の chain を作り、`nft -j list table` で読み戻してから NIC を付け、`InstanceStart` の直前にも再検証する。管理網・node・metadata・link-local・RFC1918 / CGNAT / loopback などの special-purpose 範囲と IPv6 はどの profile でも drop、tap 宛ての新規接続（他 tenant・inbound）も drop。public-web の DNS は設定した resolver だけ
  - revision に `egress_allow`（IPv4 CIDR × tcp/udp × port）を追加。CLI に `--egress` / `--egress-allow`。special-purpose 範囲や IPv6 の許可は deploy 時に 400
  - terminate と起動時 reconcile が tap / chain / map 要素 / table を消して確認する。`CAP_NET_ADMIN`・nftables・`ip_forward` の無い host では capability が理由付きの `unsupported` になり、環境作成は `Unavailable`（preflight の optional check `egress_network`）
  - `[provider.firecracker.network]`（`guest_cidr` / `dns_resolver` / `nft_binary` / `ip_binary`）、rootfs の `/etc/resolv.conf` → `/proc/net/pnp`
  - `examples/isolation-probe` に `{"probe":"net"}` / `{"probe":"listen"}`、`scripts/kvm/measure-isolation.sh` に NET（2 tenant 同時、DNS / 直接 IP / redirect、初期 race、後始末。exit 4）。KVM 実測（`docs/evidence/isolation-20260917T031126Z/`）を経て `egress_restricted` / `egress_public_web` を `Supported` にした
  - Firecracker の API socket をファイルの出現ではなく接続の成功まで待つ（2 環境同時起動で `Connection refused` になった）
- idle 休止・再開と warm 機能ゲート（Linear PLT-4633）
  - `ExecutionProvider` に `idle_quiesce` / `idle_resume` を追加（既定実装は `Unavailable` を返すので、実装しない provider は今までどおり）。Firecracker provider は `PATCH /vm {state: Paused/Resumed}` で実装し、「すでにその状態」は成功、VMM プロセス死亡 / API socket 消失 / 環境不在はそれぞれ別の error にする
  - 環境 pool が pool 入りで休止し、claim で再開してから readiness を確認する。休止に失敗した環境は pool に入れず terminate、再開を確認できない環境は retire して cold start に落ちる（dispatch しない）
  - warm attempt が実際の費用を報告する: `AttemptTimings` に `resume_ms` / `readiness_ms` を追加し、API（`attempts[].timings`）と CLI に出す
  - Firecracker の `idle_quiesce` / `idle_resume` は実機未計測のため `Unverified`。計測用の `[pool] allow_unverified_idle`（既定 `false`）を立てたときだけ再利用が動き、その構成は `GET /v1/provider` の `reuse.verified = false` と起動時の warn で一貫して「未検証」と示される
  - `scripts/kvm/measure-warm.sh`: cold / warm の比較と休止中 VMM の host 資源を測り、`docs/evidence/warm-<UTC>/` に記録する。warm が 1 度も起きなければ失敗する
  - レビュー指摘の修正:
    - Firecracker の `PATCH /vm` の「すでにその状態」判定を**要求した状態と向き**で行う。`Resumed` を要求して「paused」と返された場合や解釈できない fault は失敗（= cold start）にする。以前は非 2xx の本文に "already" と状態語があれば両方向で成功扱いだった
    - readiness 検査が guest に**実際に問い合わせる**。protocol を v2 に上げて `Ping` / `Pong` を追加し、再開後に bound 付き（500 ms）で往復する。答えない環境は retire して cold start に落ちる
    - pool が terminate する環境は休止済みなので、`Shutdown` frame を送らず `TerminateReason::Quiesced` で終わらせる（provider は猶予を待たない）。sweep と drain が毎回 grace 分止まらなくなる
    - 休止を client の応答経路から外した。`release` は環境を pool に引き渡して即座に戻り、行は休止が完了するまで `Busy`（= claim 不可）のまま。休止に失敗した環境は pool が terminate して 1 回だけ計測する。drain は引き渡し中の環境を待ってから sweep する
    - `Invoke` に `remaining_ms` を追加し、bridge は user process 向けの deadline を `guest の現在時刻 + remaining_ms` で計算する。休止で止まった時計に絶対時刻を渡さない（強制は従来どおり host 側）
    - `reuse.verified` を capability の事実（provider が両方 `supported` を申告しているか）として計算する。`[pool] enabled` とは独立
    - `[pool] allow_unverified_idle` を `profile = "production"` で拒否する（dev 専用 provider と同じ扱い）
- P2 の前提（Linear PLT-4627 / PLT-4618）
  - 再起動時の分類を dispatch 済みかどうかで分ける。`Running` だったものは `OutcomeUnknown{Host.Restarted}`、未 dispatch は `Failed{platform_error}`
  - 起動時に `ExecutionProvider::list_environments` で孤児環境を回収し、結果を構造化ログと `GET /readyz` の `reconcile` に出す。`[reconcile] on_startup` で無効化できる
  - 実行状態の永続化方針を `docs/adr/0003-execution-state-persistence.md` に決定（P2 が要求する複数プロセス間の原子性と移行手順）
- P1 動作プロトタイプ（Linear PLT-4613〜PLT-4630）。SLA なし、API と設定は予告なく変わる
  - Rust workspace（Rust 1.95.0 / edition 2024）と契約 crate: `crates/domain`（ID・entity・状態遷移・`ErrorClass`・`Limits`）、`crates/protocol`（host ↔ bridge frame、Runtime API）、`crates/provider-port`（`ExecutionProvider` ほかの port）、`crates/api-types`（DTO・`ErrorCode`）
  - `crates/application`: function / revision / alias / invoke / 履歴の usecase、in-memory repository と `data_dir/state.json` への永続化（壊れたファイルは退避方法を示して拒否）、静的 token と secret binding、容量と queue、queue / init / execution の deadline、cancel、再起動時の reconcile
  - `apps/gateway`（axum）: 管理 API、同期 invoke と HTTP アダプター、logs・履歴・usage、`/healthz`・`/readyz`・`/openapi.json`、`profile = "production"` での dev_only provider の拒否
  - `apps/cli`（`tsls`）: `functions`（create / deploy / invoke / http / logs / rollback / cancel ほか）、`provider`、`health`、使い捨ての gateway で往復する `dev`
  - 実行 provider: `crates/providers/firecracker`（Firecracker v1.17.0、vsock、環境ごとの read-only function drive、host 強制 timeout、冪等な terminate、`fc-smoke`）、`crates/providers/process`（隔離なし・開発専用）、`crates/providers/fake`（テスト用）
  - `crates/runtime-bridge`（guest の `/sbin/tachyon-init`。unix / vsock transport、PID 1 で init モードに自動で入り loopback を up にする）と `crates/sdk`（`run(handler)`、`serve_http(router)`）
  - サンプル `examples/hello`、`examples/http-axum`、`examples/cpu-burn`
  - スクリプト `scripts/kvm/`（preflight / bootstrap / build-rootfs / smoke / teardown）と `scripts/e2e/`（demo / orphan-check / selftest）
  - CI `.github/workflows/ci.yml`（fmt / clippy / test / build、musl の guest ビルド、shell scripts）と手動起動の `.github/workflows/kvm-integration.yml`
  - 文書 `docs/architecture.md`、`docs/protocol.md`、`docs/threat-model.md`、ADR 0001 / 0002、`docs/api.md`、`docs/cli.md`、`docs/kvm.md`、`docs/inventory-tachyon-apps.md`、`docs/acceptance.md`
  - 実行記録 `docs/evidence/20260915T073238Z-process/`（process provider、macOS、E2E 27/27）、`docs/evidence/kvm-20260915T080221Z/`（Firecracker microVM の smoke、aarch64 Linux/KVM、Lima の nested virtualization）、`docs/evidence/20260915T125610Z-firecracker/`（Firecracker provider の E2E 27/27）
  - 2026-09-16 のコードレビューで確認された指摘と、受入条件ごとの残りの未検証項目は `docs/acceptance.md` に記載
