# CI の gate 構成（PLT-4645）

- 対象: `.github/workflows/ci.yml`、`.github/workflows/kvm-integration.yml`、`scripts/ci/*`、`docs/openapi.json`、`crates/protocol/tests/golden/`
- 関連: [kvm.md](kvm.md)（KVM の手順）、[threat-model.md](threat-model.md)（守るもの）、[acceptance.md](acceptance.md) §PLT-4645
- 状態（2026-09-17）: hosted runner の gate は GitHub Actions 上で実行した（§8）。**self-hosted KVM runner は未登録**のため、`kvm` job は一度も実行されていない。branch protection の required check の設定も未実施（repository owner の作業、§4.4）。

## 1. 目的

開発速度を落としすぎずに、実機でしか分からない安全性の回帰を検出する。

- すべての PR（fork を含む）: hosted runner で fmt / clippy / test / musl build / shellcheck と、**名前の付いた契約 gate**（OpenAPI snapshot、wire golden、security regression group）を回す。docs だけの変更では重い job を省く。
- runtime / network / kernel / billing の変更: 上に加えて、self-hosted KVM runner での Firecracker 統合試験（smoke・隔離計測・warm 計測・E2E）を **merge の条件**にする。未実行を合格と表示しない。
- 未信頼の PR に KVM runner・常駐 runner の権限・credential を渡さない。

## 2. gate matrix

branch protection で required にするのは **`ci-gate` と `kvm-gate` の 2 つだけ**。どちらも配下の job を集約し、「実行されなかった」を黙って成功にしない。

| gate（job） | workflow | 実行条件 | required / optional | 検出するもの | skip 時の扱い |
|---|---|---|---|---|---|
| `classify changes`（`changes`） | ci.yml | 常に | `ci-gate` 経由で required | 変更 path の分類（§3） | skip しない |
| `fmt / clippy / test / build`（`rust`） | ci.yml | docs-only 以外 | `ci-gate` 経由で required | fmt、clippy `-D warnings`、`cargo test --workspace`（unit / property / fake provider / 偽 Firecracker / mock gateway の統合テスト）、build | docs-only のときだけ skip を成功扱い |
| `contracts`（`contracts`） | ci.yml | docs-only 以外 | `ci-gate` 経由で required | 下の 3 step | 同上 |
| └ OpenAPI snapshot | | | | `apps/gateway/tests/openapi_snapshot.rs`: utoipa から生成した文書が `docs/openapi.json` と違えば失敗（route・schema・status の変更をレビュー対象の diff にする）。`/v1/` の全 operation に bearer security があること | |
| └ protocol golden | | | | `crates/protocol/tests/golden.rs`: host↔bridge の全 frame 種別・全 `GuestErrorKind`・length prefix のバイト列、Runtime API の JSON body（error report / HTTP event / HTTP response / continuation）と定数（path・header・event type・上限・`PROTOCOL_VERSION`）を双方向（fixture → decode、sample → encode）で照合。fixture の過不足も検出 | |
| └ security regression group | | | | `scripts/ci/security-regression.sh`: `scripts/ci/security-regression.list` の 228 テスト（tenant 認可 53、reuse key 22、lease / epoch 42、deadline 15、egress（起動ゲートの順序・allowlist・nftables 規則）15、資源上限 81（うち PLT-4646 の SQLite store（`state.db`・usage journal / ledger・budget・queue）が lock されたときの待ちの上限 5、 PLT-4634 の admission・quota・queue 上限・breaker・placement 18、PLT-4638 の queue 満杯・object の size / quota 2、PLT-4635 の scale-down 判定・min_ready・非振動 7、PLT-4639 の outbox の上限と object の拒否 3、PLT-4637 の metrics の cardinality 上限・負荷ハーネスの送り先と上限・overshoot / starvation 検出 6、PLT-4642 の usage journal の満杯・停止による受付拒否・上限・headroom と請求の無効化 5、PLT-4641 の webhook body の上限 1、PLT-4640 の retry budget と試行回数の上限 2）。PLT-4640 は ほかに tenant 認可 4（dead letter と redrive の tenant 境界・redrive role、偽の envelope は poison）、lease / epoch 3（取り直された claim の settle 拒否、重複配送、ACK 喪失）、deadline 1（event の最大年齢）。PLT-4641 は ほかに tenant 認可 7（署名・timestamp の拒否が永続化の前、trigger CRUD の tenant 境界、secret の非開示、未知・削除済み trigger の存在を漏らさない）、lease / epoch 4（webhook の再送 dedup、再起動・2 scheduler で同じ予定時刻を 1 回、disable / delete と発火の競合）。PLT-4637 は ほかに tenant 認可 2（`/metrics` の operator credential と設定検証）、reuse key 1（boot identity）。tenant 認可のうち 5 は PLT-4638 の object の tenant 境界と queue の匿名接続拒否、2 は PLT-4635 の削除後の受付・待機・再送の拒否、2 は PLT-4639 の非同期 invocation と入力 object の tenant 境界、3 は PLT-4642 の usage 報告の tenant 境界。PLT-4635 は ほかに reuse key 2（alias 切替・secret rotate の drain）、lease / epoch 3（sweep と claim の race、先行起動の pool 入り）、deadline 3（drain timeout: 既定で長い handler を止めない・短い値の設定拒否・明示した短い値で止める）。PLT-4639 はほかに lease / epoch 3（Idempotency-Key の収束・mode をまたぐ 409・outbox の claim））を `--exact` で実行し、**list にあるテストが実行結果に現れなければ失敗**（rename・削除・`#[ignore]` で coverage が黙って落ちない） | |
| `guest musl build`（`guest-musl-build`） | ci.yml | docs-only 以外 | `ci-gate` 経由で required | guest 側（bridge・examples）の x86_64 musl static build | 同上 |
| `durable queue and objects`（`durable-queue`） | ci.yml | docs-only 以外 | `ci-gate` 経由で required | `scripts/queue/verify.sh`（PLT-4638）: `deploy/nats/versions.env` で pin した nats-server（linux-amd64）を download して sha256 を照合し、local process で起動（docker なし）。匿名・誤 password の拒否、kill -9 後の再配送、`discard: new` の容量境界、`max_age`、JetStream 契約テスト（`TACHYON_NATS_REQUIRED=1` なので server が無ければ skip せず失敗）、object store / GC テスト。続けて `scripts/queue/async-e2e.sh`（PLT-4639）: `failpoints` feature の gateway を nats-server と object store で起動し、4 つの failpoint での SIGKILL と再起動、nats-server の停止中の受付と 503、再起動後の収束、orphan object の GC、全受付の JetStream 上の message を照合。続けて `scripts/queue/triggers-e2e.sh`（PLT-4641、約 1 分、SQLite queue なので nats-server 不要）: 6 field の cron trigger の発火、SIGKILL / SIGTERM での再起動をまたいで同じ予定時刻が 2 回発火しないこと、disable 後に発火しないこと、openssl で署名した webhook の 202・event id と署名の dedup・401 / 413 / 400 が何も保存しないこと。さらに `scripts/queue/async-dispatch-e2e.sh`（PLT-4640）: 同じ構成で process provider と `examples/idempotent-async` を使い、retry・dead letter（attempts_exhausted / non_retryable / poison）・terminal commit 後 ACK 前と副作用後 commit 前の SIGKILL・redrive の認可と監査・台帳の収束を確認。証跡は artifact `queue-evidence` | docs-only のときだけ skip を成功扱い |
| `console (lint / typecheck / unit / API types / build)`（`console`） | ci.yml | docs-only 以外 | `ci-gate` 経由で required | PLT-4644。`apps/console` を pin した node 22.21.1 / pnpm 10.34.5 と lockfile（`--frozen-lockfile`）で install し、Biome lint・`tsc --noEmit`・vitest・`pnpm check:api`（`src/gen/openapi/serverless-api.ts` が `docs/openapi.json` から生成し直したものと一致）・`next build`（static export）。Playwright E2E は gateway の build・Chromium・実 invoke が要るので CI では走らせず、手元で `scripts/console/e2e.sh`（§7） | docs-only のときだけ skip を成功扱い |
| `shell scripts`（`scripts`） | ci.yml | 常に | `ci-gate` 経由で required | `bash -n`、shellcheck、`scripts/e2e/selftest.sh`、`scripts/ci/selftest.sh`（分類規則・kvm-gate 判定・security list の検証を固定） | skip しない |
| **`ci-gate`** | ci.yml | 常に（`if: always()`） | **required** | 上の job の集約。failure / cancelled は失敗。skip は docs-only のときだけ許す | — |
| `classify (is KVM required / allowed)` | kvm-integration.yml | 常に（hosted） | `kvm-gate` 経由 | KVM 必要か、この event が runner を使ってよいか | — |
| `KVM integration (self-hosted, firecracker)`（`kvm`） | kvm-integration.yml | KVM 必要 かつ trusted（§4.1） | `kvm-gate` 経由で、KVM 必要な変更に対して required | preflight → bootstrap → profile → `scripts/kvm/smoke.sh`（boot・handshake・host 強制 timeout・terminate）→ `scripts/kvm/measure-isolation.sh`（egress none、vCPU / memory、ephemeral storage 上限）→ `scripts/kvm/measure-warm.sh`（休止・再開、reuse）→ `scripts/e2e/demo.sh`（firecracker、cross-tenant 404、cancel、orphan）→ **常に** cleanup + orphan check → **常に** evidence artifact | 条件を満たさなければ skip。その skip は `kvm-gate` で失敗になる |
| **`kvm-gate`** | kvm-integration.yml | 常に（`if: always()`） | **required** | 下表 | — |

`kvm-gate` の判定（`scripts/ci/kvm-gate.sh`、`scripts/ci/selftest.sh` で固定）:

| KVM 必要 | `kvm` job | `kvm-gate` | 表示 |
|---|---|---|---|
| いいえ | skipped | success | `NOT REQUIRED - ... KVM integration did not run and is not claimed as passed` |
| はい | success | success | `PASSED` |
| はい | skipped（`kvm` label なし） | **failure** | `REQUIRED but was NOT RUN: a maintainer must review the diff and add the kvm label` |
| はい | skipped（fork / 未信頼） | **failure** | `REQUIRED but was NOT RUN: the change comes from a fork ...` |
| はい | failure / cancelled | **failure** | `finished with '<result>'` |
| はい | runner 待ちで queued | **pending**（`kvm-gate` は `needs: kvm` なので始まらない） | GitHub が 24 時間後に cancel → failure |

## 3. 変更範囲の分類（`scripts/ci/classify-changes.sh`）

PR は `base...merge commit`、main への push は `before...sha` の差分で判定する。範囲が分からないとき（branch の初回 push、`before` が取得できない force push、`workflow_dispatch`）は「すべて変更」とみなす（docs-only ではなく、KVM 必要）。

| 分類 | path | 効果 |
|---|---|---|
| docs-only | すべての path が `docs/**`（`docs/openapi.json` を除く）、`*.md`、`LICENSE*`、`.github/ISSUE_TEMPLATE/**` | `rust` / `contracts` / `guest-musl-build` / `durable-queue` / `console` を skip。`scripts` と `ci-gate` は走る |
| KVM 必要 | `crates/providers/firecracker/**`、`crates/runtime-bridge/**`、`crates/protocol/**`、`crates/provider-port/**`、`crates/application/src/bridge_session.rs`、`crates/application/src/services/pool.rs`、`crates/domain/src/egress.rs`（egress allowlist。host の nftables / tap 規則になる）、`crates/**` のうち path に `billing` / `usage` / `metering` を含むもの、`scripts/kvm/**`、`scripts/e2e/**`、`scripts/ci/kvm-*`、`config/gateway.firecracker.toml`、`examples/{hello,cpu-burn,isolation-probe}/**`、`.github/workflows/kvm-integration.yml`、`rust-toolchain.toml`（いずれも `*.md` を除く） | `kvm-gate` が KVM 実行を要求する |
| contract（参考） | `crates/protocol/**`、`crates/api-types/**`、`apps/gateway/**`、`docs/openapi.json`、`scripts/ci/security-regression.list` | 表示だけ（contract gate は docs-only 以外で常に走る） |

判断メモ:

- `Cargo.lock` は KVM 必要に含めていない。依存更新のたびに KVM を要求すると、runner が無い現状では merge が止まるため。`tokio-vsock` など guest に入る依存を上げる PR は、レビューで `kvm` label を付けるか dispatch する。
- PLT-4638 の `crates/durable-port/**`・`crates/adapters/queue-nats/**`・`crates/application/src/durable/**`・`deploy/nats/**`・`scripts/queue/**` は KVM 必要にしない。guest / provider に触れない control plane の保存で、hosted runner の `durable-queue` job が実際の nats-server に対して検査する。port を `crates/provider-port` ではなく別 crate にしたのはこのため（`provider-port` は KVM 必要）。
- PLT-4639 の `crates/application/src/services/invoke_async/**`・`crates/application/src/failpoints.rs`・`scripts/queue/async-e2e.sh` も KVM 必要にしない。provider に触れない受付と配送で、unit test（failpoint と再起動）と `durable-queue` job の E2E が検査する。E2E は provider を使う実行を行わないので `scripts/e2e/` ではなく `scripts/queue/` に置いた。
- PLT-4637 の `crates/application/src/metrics/**`・`apps/load/**`・`scripts/load/**`・`deploy/prometheus/**` は KVM 必要にしない（負荷シナリオは local の throwaway gateway に対する観測で、`scripts/e2e/` に置くと KVM gate を要求するので `scripts/load/` に置いた）。ただし同じ変更で `crates/provider-port/**`（`environment_stats`）と `crates/providers/firecracker/**`（cgroup の読み取り）に触れているので、その PR 全体は KVM 必要に分類される。`scripts/load/scenarios.sh` は CI では実行しない（shellcheck だけ）。手元の証跡は `docs/evidence/load-*`。
- PLT-4641 の `crates/application/src/services/triggers/**`・`apps/gateway/src/trigger_handlers.rs`・`scripts/queue/triggers-e2e.sh` も KVM 必要にしない。trigger は `invokeAsync` の受付を呼ぶだけで provider に触れず、unit test（fake clock）と `durable-queue` job の E2E が検査する。
- PLT-4640 の dispatcher（`services/invoke_async/{dispatch,retry,dead_letters}.rs`、`apps/gateway/src/dead_letters.rs`、`examples/idempotent-async/**`、`scripts/queue/async-dispatch-e2e.sh`）も KVM 必要にしない。実行は既存の driver（`services/invoke.rs`）を通り、guest・bridge・provider の境界は変えない。E2E は process provider で、`durable-queue` job が実際の nats-server に対して走らせる。
- PLT-4644 の `apps/console/**`・`scripts/console/**`・`apps/gateway/src/console.rs` は KVM 必要にしない。console は公開 management API の client で、gateway 側の変更は `[console]` の静的配信だけ（`apps/gateway/*` なので契約 path には入る）。
- PLT-4648 の `scripts/lab/**`・`deploy/lab/**` は KVM 必要にしない。lab は既存の gateway / provider / `scripts/queue` / `scripts/kvm/build-rootfs.sh` を呼ぶだけの入口で、KVM job（§2）は lab.sh を実行しないため、KVM gate を要求しても lab.sh の firecracker 経路を検証したことにはならない。その経路は KVM host での手動実行が要る（`docs/runbook.md` §10）。CI では `scripts` job の shellcheck だけがかかる。`scripts/ci/selftest.sh` で固定。
- PLT-4631 の `crates/application/src/services/dispatcher.rs`・`crates/application/src/repository/slot.rs`（dispatcher lease、slot の CAS、fencing）は KVM 必要にしない。provider に依存しない control plane の排他で、`crates/application/tests/leases.rs` と repository 契約テスト（別プロセスの競合を含む）が決定的に検査し、security regression group（`lease_epoch`）に入れている。
- PLT-4646 の `scripts/chaos/**` は KVM 必要にしない（process provider と local の nats-server だけを使う）。約 50 分かかり timing に依存するので CI では実行しない（shellcheck だけ）。同じ変更の `crates/application/src/failpoints.rs` と `repository/sqlite/mod.rs`（connection mutex の待ちの上限）、その follow-up の `crates/application/src/sqlite_wait.rs` と usage journal / ledger・budget store・SQLite queue の同じ上限は unit test（security regression group の resource_limits）が検査する。`examples/idempotent-async` の `sleep_ms` は guest probe ではない。
- `crates/application` の lease / fencing / deadline のロジックは fake provider で決定的に試験できるので、KVM ではなく security regression group で守る（`bridge_session.rs` と `services/pool.rs` だけは実 VM の frame・休止に依存するので KVM 必要）。
- 分類は path だけを見る保守的な規則で、変更の中身は見ない。docs 以外の path が 1 つでもあれば docs-only にはならない。

## 4. 信頼モデル

### 4.1 誰のコードがどこで動くか

| event | hosted runner（ci.yml、kvm-integration.yml の classify / kvm-gate） | self-hosted KVM runner（`kvm` job） |
|---|---|---|
| fork からの `pull_request` | 動く（read-only token、secret なし。GitHub の既定で fork PR の token は read-only、secret は渡らない） | **動かない**（classify が `trusted=false`、job の `if:` でも head repo を再確認） |
| 同じ repository の branch からの `pull_request` | 動く | `kvm` label があり、KVM 必要のときだけ |
| `push` to `main` | 動く | KVM 必要のとき |
| `workflow_dispatch`（write 権限が必要） | 動く | 常に（明示的な要求として扱う） |
| `pull_request_target` | **使わない**（fork のコードを base の権限で動かす事故を避ける） | 使わない |

- すべての job で `permissions: contents: read`、`actions/checkout` は `persist-credentials: false`。secret・production credential・cloud credential は一切参照しない（`kvm` job が触るのは runner 上で起動する gateway と生成した dev token だけ）。
- 3rd party action は commit SHA で pin（`dependabot.yml` の github-actions 更新で追従）。
- `kvm` job は `Swatinem/rust-cache` を使わない。PR 由来の build cache が main の run に混ざる（cache poisoning）経路を作らないため。
- evidence は `RUNNER_TEMP` に書き、repository の作業ツリーに残さない。cleanup で `scripts/kvm/teardown.sh --purge` を実行し `.kvm/`（PR のコードで作った rootfs を含む）と `data/` を消す。次の job が前の job の rootfs・kernel・VM を使うことはない。job 開始時に firecracker process が残っていれば開始を拒否する。

### 4.2 `kvm` label の意味

`kvm` label は「maintainer が diff を読み、KVM runner で実行してよいと判断した」ことを表す。同じ repository に push できる人（write 権限）は既に信頼されている前提で、label はその上での実行許可である。

- label を付けた後の追加 push（`synchronize`）も label が残っている限り KVM で走る。レビュー後に大きな変更が入った場合は label を外して付け直す運用にする。
- label の付与は write 権限以上に限られる（GitHub の仕様）。
- fork の PR は label を付けても `kvm` job は走らない。必要なら maintainer が内容を確認したうえで同じ repository の branch に取り込み、その PR に label を付ける。

### 4.3 workflow の `if:` は多層防御にすぎない

この repository は **public** である。PR は workflow ファイル自体を書き換えられる（`pull_request` の run は PR の merge commit の workflow を使う）ので、`if:` 条件だけでは fork PR が self-hosted runner に job を投げることを防げない。runner 側で次を必ず設定する（§5）。

1. runner を **この repository 専用の runner group** に置き、public repository での使用を明示的に許可したうえで、他 repository から使えないようにする。
2. Settings → Actions → General → 「Fork pull request workflows from outside collaborators」を **Require approval for all outside collaborators** にする（fork PR の workflow は maintainer が承認するまで一切走らない）。
3. runner を **ephemeral**（`--ephemeral`、1 job で登録解除）にし、VM / コンテナごと作り直す。常駐 runner にしない。
4. runner のユーザーは sudo なし、`kvm` group のみ。host には production credential・SSH key・cloud metadata へのアクセスを置かない。

### 4.4 owner が行う設定（未実施）

- branch protection（または ruleset）で `main` の required status checks に `ci-gate` と `kvm-gate` を追加する。個々の job（`rust` など）は required にしない（docs-only で skip されたときに「expected」で止まるため）。
- repository label `kvm` を作る。
- §4.3 の fork PR 承認設定。

## 5. KVM runner を安全に登録する手順（将来）

**この Issue では登録していない。** 登録は repository owner が行う。

1. host を用意する: Linux x86_64（`docs/inventory-tachyon-apps.md` §6 の第一 profile）、bare metal または nested virtualization を許す VM。production と同じネットワーク・アカウントに置かない。外向き通信は GitHub、`github.com/firecracker-microvm` の release、`s3.amazonaws.com/spec.ccfc.min`（guest kernel）、crates.io、static.rust-lang.org に絞る。
2. 1 job ごとに作り直す使い捨て VM（またはコンテナ + `/dev/kvm`）を image から起動する仕組みを用意する。image には `curl jq e2fsprogs gcc rustup pgrep` と、`kvm` group の非 root ユーザーを入れる。**`.kvm/` や `target/` を image に焼かない**（bootstrap が毎回取得・検証する）。
3. `scripts/kvm/preflight.sh` が READY になることを image で確認する（`/dev/kvm` rw、socket path 長 ≤ 107 バイト。runner の work directory は短いパスにする）。
4. GitHub の Settings → Actions → Runner groups で専用 group を作り、この repository だけを許可する（public repository の許可が必要）。
5. JIT / 登録 token で runner を `--ephemeral --labels self-hosted,linux,kvm` として登録する。token は登録直後に失効する短命のものを使い、image や repository に残さない。
6. §4.3 の fork PR 承認設定と §4.4 の required check を有効にする。
7. 最初の実行は `workflow_dispatch`（`reason: runner bring-up`）で行い、artifact の `profile.json`・`cleanup.txt` と、job 後に host に firecracker process・`.kvm/`・tap・loop device が残っていないことを確認する。
8. `docs/acceptance.md` §PLT-4645 の「未検証」行を、run の URL と artifact 名で更新する。

## 6. runtime profile の変更: canary と rollback

runtime profile = Firecracker の版（`FIRECRACKER_VERSION`、`scripts/kvm/bootstrap.sh` の既定）、guest kernel（`CI_VERSION` / `GUEST_KERNEL_SERIES` → `.kvm/manifest.json` の `kernel_sha256`）、rootfs（bridge の commit から毎回 build、sha256）、`config/gateway.firecracker.toml`、runner host の kernel / CPU。`kvm` job はこれを `profile.json` に記録する（`scripts/ci/kvm-profile.sh`）。

### 6.1 canary（変更を入れる前）

1. **profile の変更だけ**を 1 つの PR にする（コード変更と混ぜない）。`scripts/kvm/**` か `config/gateway.firecracker.toml` を変えるので `kvm-gate` が KVM を要求する。
2. 取り込む前に、現行 main のコードのまま新しい profile を試す: `workflow_dispatch` で `ref=main`、`firecracker_version` / `ci_version` / `guest_kernel_series` を指定して 3 回 dispatch する（override は dispatch のときだけ有効。evidence の `profile-overrides.txt` に残る）。
3. 合格条件（3 回とも）: smoke の 2 シナリオ PASS、measure-isolation が exit 0（M8 egress none PASS、DISK PASS）、measure-warm が exit 0（warm が 1 回以上）、E2E が全 step PASS、`cleanup.txt` が `cleanup rc=0`。`profile.json` の `firecracker.version` / `guest_kernel.sha256` / `rootfs.sha256` が意図した値であること。
4. 時間の値（boot / init / handler / resume）は前回の main の evidence と並べて PR に貼る。参考値であり合否には使わない（nested virtualization と bare metal は比較しない）。
5. PR に `kvm` label を付け、PR の run でも同じ合格条件を満たしてから merge する。merge 後の main の push run が 2 回目の確認になる。

### 6.2 rollback

1. main の run で `kvm-gate` が失敗し、原因が profile 変更なら、**profile の PR を `git revert` した PR** を作る（KVM 必要に分類される）。
2. revert の PR に `kvm` label を付け、`profile.json` が変更前の値（直前に合格した main の artifact の `firecracker.binary_sha256` / `guest_kernel.sha256`）に戻ったこと、§6.1 の合格条件を満たすことを確認して merge する。
3. 急ぐ場合は merge 前に `workflow_dispatch`（`ref=main`、旧版の `firecracker_version` 等を指定）で旧 profile が現行 main で動くことを先に確認する。
4. `kvm` job は毎回 `.kvm/` を purge して取り直すので、runner 上に新 profile の生成物が残って rollback を妨げることはない。runner host の kernel を変えた場合の rollback は runner image の切り戻しで行い、`profile.json` の `host.kernel_release` で確認する。
5. `docs/acceptance.md` と `docs/kvm.md` §5 の「確認済みの環境」を、合格した artifact に合わせて更新する。

## 7. ローカルでの再現

| 目的 | コマンド |
|---|---|
| hosted gate 一式 | `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` |
| OpenAPI snapshot | `cargo test -p tachyon-serverless-gateway --test openapi_snapshot`（意図した変更は `TSLS_UPDATE_SNAPSHOTS=1` を付けて再生成し、`docs/openapi.json` を commit） |
| wire golden | `cargo test -p tachyon-serverless-protocol --test golden`（意図した変更は `TSLS_UPDATE_SNAPSHOTS=1`。frame の形を変えるなら先に `PROTOCOL_VERSION` を判断する。`docs/protocol.md` §A） |
| security regression group | `scripts/ci/security-regression.sh`（1 分類だけ: `--category deadline`）。テストを rename / 置換したら同じ変更で `scripts/ci/security-regression.list` を直す |
| invokeAsync / outbox | `scripts/queue/async-e2e.sh`（証跡は既定で `target/queue/async-e2e-<UTC>/`、`--evidence DIR` で変更） |
| cron / webhook trigger | `scripts/queue/triggers-e2e.sh`（証跡は既定で `target/queue/triggers-e2e-<UTC>/`、`--evidence DIR` で変更。`curl` `jq` `openssl` `python3` が要る。§5 は fire が PLT-4640 の dispatcher で terminal になること、失敗する fire の retry と dead letter、`AttemptSettled` の計測を確かめる） |
| 非同期 dispatcher・retry・DLQ・redrive | `scripts/queue/async-dispatch-e2e.sh`（証跡は既定で `target/queue/async-dispatch-e2e-<UTC>/`） |
| durable queue / object store | `scripts/queue/verify.sh`（証跡は既定で `target/queue/verify-<UTC>/`、`--evidence DIR` で変更）。手で試すなら `eval "$(scripts/queue/up.sh)"` → `TACHYON_NATS_REQUIRED=1 cargo test -p tachyon-serverless-queue-nats` → `scripts/queue/down.sh --purge` |
| Functions console | `cd apps/console && pnpm install --frozen-lockfile && pnpm lint && pnpm ts && pnpm test && pnpm check:api && pnpm build`（CI の `console` job と同じ）。E2E は `scripts/console/e2e.sh [--evidence DIR]`（gateway・example の build、console の build、Chromium headless で 15 シナリオ、約 1〜2 分。`docs/console.md`） |
| 故障マトリクス（PLT-4646） | `scripts/chaos/matrix.sh`（約 50 分、単一 host・process provider。証跡は既定で `docs/evidence/chaos-<UTC>/`、`--only` / `--retries` / `--evidence`。手順と判定は `docs/failure-matrix.md`） |
| 分類・gate 判定の自己テスト | `scripts/ci/selftest.sh` |
| 変更の分類 | `scripts/ci/classify-changes.sh --base origin/main --head HEAD` |
| gate が壊れた変更を検出することの確認 | `scripts/ci/prove-gates.sh`（作業ツリーの一時コピーに既知の悪い変更を 1 つずつ入れ、対応する gate が失敗することを確認。証跡は `docs/evidence/ci-gates-<UTC>/`） |
| workflow の lint | `actionlint -config-file .github/actionlint.yaml .github/workflows/*.yml`（CI には組み込んでいない） |
| KVM job と同じ流れ | Linux/KVM で `docs/kvm.md` §3 の順に実行 |

## 8. 検証の記録

- 意図的な破損: `docs/evidence/ci-gates-*/summary.txt`（baseline 3 gate が PASS し、16 件の既知の悪い変更がすべて対応する gate で失敗）。
- GitHub Actions 上の実行: `docs/acceptance.md` §PLT-4645 に run の URL と結果を記録する。
- KVM job: **未検証**（runner 未登録）。

## 9. 既知の制約

- `kvm` job の手順（特に `measure-warm.sh` と `demo.sh` を同じ job で続けて実行したときの port 8080・`data/` の再利用、x86_64 host での bootstrap）は KVM runner 上で一度も通していない。
- KVM 必要な変更は、runner が登録されるまで `kvm-gate` が失敗または pending のままになる。これは意図した挙動で、merge するには owner が branch protection を一時的に外す判断を明示的にする必要がある（その場合は PR に「KVM 未検証」と書く）。
- `durable-queue` job は GitHub の release から nats-server を download する。GitHub に届かない、または release が消えた場合は失敗する（checksum が一致しない binary は実行しない）。
- security regression group は既存のテストを束ねたもので、新しい検査を増やしたわけではない。list に無い安全性テストは group の対象外（`cargo test --workspace` では走る）。
- OpenAPI snapshot は utoipa の注釈から生成した文書を比べる。注釈と実際の handler の挙動のずれは検出しない（`apps/gateway/tests/gateway_integration.rs` の範囲）。
