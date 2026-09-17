# Runbook: fresh 環境からの配備・デモ・rollback・削除（PLT-4648）

別の開発者が、実装者の手元の設定なしに同じプロトタイプを立ち上げ、P1 / P2 / P3 のデモを通し、壊れたら復旧し、最後に自分の作ったものだけを消せるようにするための手順書。入口は 1 つ: **`scripts/lab/lab.sh`**。外部から取ってくるものの版と sha256 はすべて **`deploy/lab/versions.lock`** にある。

> [!WARNING]
> これは local host だけで完結する使い捨ての lab。本番データ・顧客データ・本物の secret を入れない（§3）。cloud のリソースは作らない。

## 目次

1. 何を再現するか
2. 事前条件と必要権限
3. 安全上の注意（データ・region・秘密情報・費用）
4. 手順（quickstart、Linux/KVM、demo、日常操作）
5. component ごとの health 確認
6. 失敗時の復旧
7. teardown と孤児検査
8. lab directory と command log
9. 検証の記録
10. まだ再現できないこと

---

## 1. 何を再現するか

| phase | 内容 | lab.sh |
|---|---|---|
| 配備 | pin した nats-server（macOS / Linux）と Firecracker・jailer・guest kernel（Linux/KVM）を sha256 照合して取得、gateway / CLI / guest を build、使い捨ての設定と secret を生成、nats-server と gateway を起動、SQLite 台帳の migration、全 component の health 確認 | `preflight` → `bootstrap` → `up` |
| P1 | 関数登録 → revision publish → 同期 invoke（結果・env・secret binding・user error・host 強制 timeout・HTTP adapter）→ logs → boot evidence → v2 publish → rollback → 他 tenant 404 | `demo p1` |
| P2 | revision の `max_concurrency` を超える burst、scale to zero、cold 再アクセス、alias 切替と戻し、placement label（`jp` node で `jp` は受付・`us` は拒否）、`/metrics` snapshot（operator 専用） | `demo p2` |
| P3 | `invokeAsync`（inline と object store 経由の大きな入力）、retry → dead letter → redrive（role 検査）、cron trigger、署名付き webhook（正しい署名 202・偽署名 401）、usage 報告、budget による受付停止と解除、secret 値の非漏洩検査 | `demo p3` |
| 片付け | 自分の lab id が付いたものだけを止めて消し、孤児を検査 | `down` / `teardown` |

rollback は 2 種類ある。**関数の rollback**（alias を前の revision に戻す）は P1 / P2 の demo が行う。**lab 自体の作り直し**は `teardown` → `bootstrap` → `up`（§7）。

## 2. 事前条件と必要権限

### 2.1 host

| 用途 | OS / arch | provider | 状態 |
|---|---|---|---|
| 開発モード（既定） | macOS（Apple Silicon で確認）または Linux、aarch64 / x86_64 | `process` — **microVM ではない。隔離なし**。関数は gateway の子プロセスとして動く | macOS arm64 で clean clone から通した（§9） |
| 本来の形 | Linux + `/dev/kvm`、aarch64 / x86_64 | `firecracker` — Firecracker microVM、jailer、cgroup v2 | **lab.sh 経由は未検証**（§10）。同じ構成要素は `scripts/kvm/*` と `scripts/e2e/demo.sh` で aarch64 nested virt 上の記録がある（docs/kvm.md §5） |

macOS で microVM を動かすには Lima VM の中で Linux/KVM の手順を使う（docs/kvm.md §5）。

### 2.2 tool と版

`scripts/lab/lab.sh preflight` がすべて検査する（FAIL が 1 つでもあれば exit 1）。

| tool | 版 | 必須 | 備考 |
|---|---|---|---|
| bash | ≥ 3.2 | 必須 | macOS 標準の `/bin/bash` で動く |
| curl, tar, git, openssl | 任意の現行版 | 必須 | openssl は乱数ではなく webhook 署名の確認用。LibreSSL 3.3.6 で確認 |
| jq | ≥ 1.6 | 必須 | 1.7.1 で確認 |
| python3 | ≥ 3.8 | 必須 | 空き port の選択と SQLite の読み取り（`sqlite3` CLI は不要）。3.9.6 で確認 |
| sha256sum または shasum | — | 必須 | |
| Rust | **1.95.0**（`rust-toolchain.toml`・`mise.toml`・versions.lock で固定） | 必須 | rustup なら repository の `rust-toolchain.toml` を読んで自動で入る。mise なら `mise install` |
| perl | 任意 | 任意 | command log の時刻付け（無ければ shell で代替、遅いだけ） |
| rustup, cc, mkfs.ext4（e2fsprogs ≥ 1.43） | — | firecracker のみ必須 | musl guest の build と rootfs 作成 |
| ip, nft | — | firecracker のみ任意 | teardown の tap / nftables 残留検査 |
| node 22.21.1 / pnpm 10.34.5 | versions.lock | `--console` のときだけ | `cd apps/console && mise install` |

Docker は使わない。

### 2.3 必要な権限（明示リスト）

| 操作 | 必要な権限 | いつ |
|---|---|---|
| lab directory（既定 `<repo>/.lab`）の作成・書き込み | 実行ユーザー | 常に |
| `127.0.0.1` の空き port 3 つ（gateway、nats client、nats monitor）に listen | 実行ユーザー（1024 以上の port を自動選択） | `up` |
| cargo build（`target/`）、crates.io からの依存取得 | 実行ユーザー | `bootstrap` |
| `/dev/kvm` の読み書き | `kvm` group か `chmod 666 /dev/kvm`（Lima では起動のたび） | firecracker |
| jailer（chroot・uid 64000 への降格・PID/mount namespace） | **root**（passwordless `sudo -n`） | firecracker（`LAB_FC_PRIVILEGED=1`、既定） |
| cgroup v2 に `tsls-<lab id>/<env>` を作り VMM を入れる | **root** | 同上 |
| tap / nftables の残留削除（teardown） | **root** | firecracker で egress を使ったときだけ（lab の demo は egress none） |
| root が作ったファイル（jail、data）の削除 | **root** | firecracker の teardown |

root を使わない firecracker は `LAB_FC_PRIVILEGED=0`: profile dev、jailer なし、cgroup `best-effort`。`enforce_resource_limits` は unverified と表示され、隔離の主張はできない。

### 2.4 network egress

| 宛先 | 用途 | いつ |
|---|---|---|
| `github.com`（release asset のリダイレクト先を含む） | nats-server v2.14.7、Firecracker v1.17.0 の tarball | `bootstrap` |
| `s3.amazonaws.com/spec.ccfc.min` | guest kernel `vmlinux-6.1.155` | `bootstrap`（firecracker） |
| `crates.io` / `static.crates.io`、`github.com`（git 依存） | cargo の依存 | `bootstrap` |
| `registry.npmjs.org`、`github.com` | console の依存 | `bootstrap --console` |

lab の実行中（`up` 以降）は外部に出ない。gateway・nats・console はすべて `127.0.0.1` に bind する。

### 2.5 disk / memory

| 項目 | 目安 | preflight |
|---|---|---|
| `target/`（cargo build） | 約 8 GiB | WARN |
| lab directory | process 2 GiB / firecracker 6 GiB（versions.lock） | FAIL |
| memory | 4 GiB 以上 | WARN |
| firecracker の unix socket path | 長い方が 107 byte 以下: `<lab>/fc/run/env_<26>/v.sock_5000`、privileged（jailer）では jail の中の `<lab>/fc/jail/firecracker/env-<26>/root/v.sock_5000`（lab directory の path が 39 byte を超えると FAIL。例: `/home/<user>/<clone>/.lab` なら clone の path を短くするか `--lab-dir` を短くする） | FAIL（短い `--lab-dir` にする） |

## 3. 安全上の注意（データ・region・秘密情報・費用）

- **本番データ・顧客データを使わない。** demo の payload はすべて合成値（`{"name":"lab"}` など）。lab に自分のデータを入れた場合、その扱いは保証しない（process provider は隔離なし、object store の鍵は lab directory の中にある）。
- **region は label であって証明ではない。** 生成される設定の `[capacity.node] region = "jp"` は、`--region jp` の revision をこの node に載せ、`us` を拒否する **scheduling label** にすぎない。data がどの国のどの disk にあるかを示すものではない（lab は手元の host で動く）。P2 の demo はこの点を `NOTE` として出力する。
- **秘密情報は使い捨ての生成値だけ。** `up` が `<lab>/secrets/`（0700）に tenant token 3 つ・operator の metrics token・demo secret 値・object store 鍵（AES-256-GCM、64 hex）・trigger 封印鍵を `od /dev/urandom` から生成し 0600 で置く。nats の password は `<lab>/nats/gateway.password`（0600）。生成設定 `<lab>/config/gateway.toml` も token を含むので 0600。値は command log に出さない（CLI には環境変数で渡し、curl の表示は header を省く）。webhook の secret は作成時に 1 回だけ返り、demo は memory に持つだけで書かない。`demo` の最後に token と secret 値を `logs/`・`demo/`・`state.db`（WAL を含む）から grep し、見つかれば FAIL にする。**本物の token や secret をこれらのファイルに入れない。**
- **費用: local だけ、cloud の支出なし。** lab は cloud のリソースを作らない。usage と budget は dev の仮価格表（`provisional-dev-2026-09-v1`、JPY の micro 単位）による **仮の数字** で、`billing_enabled=false`、請求は発生しない。budget の hard limit は受付停止の仕組みを見せるためのもので、費用上限の保証ではない。host の電気代・CPU 時間は当然かかる（zero environments は zero host cost ではない。gateway・nats・台帳は動き続ける）。
- **process provider は隔離しない。** 信頼できない binary を deploy しない。

## 4. 手順

すべてリポジトリの root から実行する。以下のコマンドは §9 の clean clone 追試でそのまま使ったもの。

### 4.1 quickstart（macOS / Linux、process provider）

```sh
git clone https://github.com/quantum-box/tachyon-serverless.git
cd tachyon-serverless

scripts/lab/lab.sh preflight          # host の検査。FAIL が無いこと（macOS では os が WARN）
scripts/lab/lab.sh bootstrap          # nats-server を sha256 照合で取得、cargo build（初回は数分）
scripts/lab/lab.sh up                 # 設定・secret 生成、nats + gateway 起動、migration、health 表
scripts/lab/lab.sh demo all           # P1 → P2 → P3（約 1 分）。exit 0 = 全 check PASS
scripts/lab/lab.sh status             # いつでも health 表
scripts/lab/lab.sh teardown           # 停止・削除・孤児検査。最後に "orphan check: clean"
```

`preflight` の期待出力（抜粋）:

```text
WARN  os                         macOS: process provider only (dev mode, NOT a microVM, no isolation)
ok    pin:rust                   rust-toolchain.toml 1.95.0 = versions.lock
ok    pin:nats                   versions.lock = deploy/nats/versions.env (v2.14.7)
ok    network                    github.com releases reachable (nats-server)
READY with warnings (read the WARN rows)
```

`up` の期待出力（抜粋。全行 `ok` で、console は既定では `skip`）:

```text
[lab] step 3/5: gateway on 127.0.0.1:61119 (applies forward-only migrations to <lab>/data/state.db before listening)
ok    ledger                 state.db schema_version = migrations         8/8
ok    queue                  tsls_async_queue_condition{healthy}          1
skip  console                GET /console/                                not enabled (up --console)
[lab] step 5/5: ready
```

port は空きから自動で選ばれ、`<lab>/manifest.env` に記録されて次の `up` でも同じものを使う。自分で CLI を叩くとき:

```sh
set -a; . .lab/secrets/tokens.env; set +a
export TSLS_API_URL=http://127.0.0.1:$(sed -n 's/^GATEWAY_PORT=//p' .lab/manifest.env) TSLS_TOKEN=$TOKEN_A
target/debug/tsls functions list
```

console を見る場合は `bootstrap --console`（node / pnpm が要る）→ `up --console` → `http://127.0.0.1:<port>/console/` に `tokens.env` の token を貼る。

### 4.2 Linux/KVM（firecracker provider）

> [!IMPORTANT]
> lab.sh の firecracker 経路は **未検証**（§10）。下の手順は既存の `scripts/kvm/bootstrap.sh`・`config/gateway.firecracker.toml`・docs/kvm.md で確認済みの構成（Firecracker v1.17.0、guest kernel 6.1.155、jailer、cgroup required）をそのまま lab directory に閉じ込めたもので、実機での通し実行はまだ記録が無い。

```sh
scripts/lab/lab.sh --provider firecracker preflight   # /dev/kvm rw、cgroup v2、sudo -n、socket path 長
scripts/lab/lab.sh --provider firecracker bootstrap   # firecracker + jailer + vmlinux を pin の sha256 で照合、
                                                      # musl guest build、<lab>/cache/kvm/rootfs.ext4 を作成
scripts/lab/lab.sh up                                 # provider は manifest から。gateway は sudo -n で root 起動
scripts/lab/lab.sh demo all
scripts/lab/lab.sh teardown
```

- 生成される設定は `profile = "production"`、`[provider.firecracker.cgroup] mode = "required"`、`parent = "tsls-<lab id>"`、`[provider.firecracker.jailer] chroot_base = "<lab>/fc/jail"`、workdir `<lab>/fc/run`。cgroup だけは lab directory の外（`/sys/fs/cgroup/tsls-<lab id>`）に作られ、teardown が名前で消す。
- 非特権で試すなら `LAB_FC_PRIVILEGED=0 scripts/lab/lab.sh up`（profile dev、jailer なし、cgroup best-effort、制限は unverified）。
- provider は lab ごとに固定。process の lab を firecracker に変えるには別の `--lab-dir` を使う。
- P3 の非同期 demo は、firecracker では `examples/idempotent-async`（host の file system を使う）が動かないので `hello` を使い、redrive 後の実行は `failed` になる（これは期待どおり）。burst は 5 並列・上限 2 に下げる。
- 隔離・egress・資源上限の実測は lab の範囲外。docs/kvm.md §3.6 の `scripts/kvm/measure-isolation.sh` を使う。

### 4.3 demo の各 phase と期待出力

`demo <phase>` は稼働中の lab gateway に対して動き、自分で gateway を起動しない。何度でも実行できる（関数は get-or-create、revision と trigger は毎回追加）。結果は `<lab>/demo/<UTC>-<phase>/results.txt`、各 check が読んだ JSON も同じ場所に残る。すべての check が PASS のときだけ exit 0。

**P1**（`scripts/lab/lab.sh demo p1`）

| check | 何を確かめるか | 期待 |
|---|---|---|
| `p1.provider_kind` | `tsls provider --json` | `kind=process isolation=process dev_only=true`（firecracker では `micro_vm` / `false` も検査） |
| `p1.publish_v1` | `tsls functions deploy --function hello --env GREETING=v1 --secret DEMO_SECRET=demo-secret` | revision が ready、`prod` が v1 |
| `p1.sync_invoke_ok` / `p1.env_and_secret_binding` | `tsls functions invoke hello --payload '{"name":"lab"}'` | `exit=0 message=hello, lab`、`greeting=v1 secret_present=true`（値そのものは返らない） |
| `p1.logs` | `tsls functions logs --invocation <id>` | `hello: handling invocation=...` の行がある |
| `p1.boot_evidence` | `tsls functions invocation <id> --json` | `host_pid` と `handler_ms` がある（firecracker では `guest_boot_id` も） |
| `p1.user_error` | `{"fail":true}` | `exit=3 code=user_error` |
| `p1.http_adapter` | `tsls functions http http-axum --method GET --path /` | `status=200 body=ok` |
| `p1.timeout_enforced_by_host` | timeout 2 s の revision に SIGTERM を無視する 30 s の処理 | `exit=4 code=timeout` |
| `p1.publish_v2` → `p1.rollback_to_v1` | v2 を publish → `tsls functions rollback hello` | `greeting=v2` → `greeting=v1` |
| `p1.other_tenant_404` / `_invoke_404` | tenant B の token で A の関数 | `exit=2 code=not_found` |
| `p1.no_environment_left` | `tsls capacity --json` | 全状態の環境数 0 |

```text
PASS  p1.sync_invoke_ok                                          exit=0 message=hello, lab
PASS  p1.timeout_enforced_by_host                                exit=4 code=timeout (handler ignores SIGTERM; host kills at 2 s + grace)
PASS  p1.rollback_to_v1                                          prod -> previous revision, greeting=v1
```

**P2**（`demo p2`）

| check | 期待 |
|---|---|
| `p2.burst_all_served` / `p2.burst_bounded` | `max_concurrency 3` の revision に 7 並列で全部 exit 0、観測した環境数の最大が 1〜3（サンプルは `p2-capacity-samples.ndjson`） |
| `p2.scale_to_zero` | revision の環境数が 60 s 以内に 0。`zero environments is not zero host cost` の注記付き |
| `p2.cold_reaccess` | 0 の状態から invoke して exit 0 |
| `p2.alias_switch` / `p2.alias_rollback` | `--no-publish` の v3 に `tsls functions alias-set --expected-generation` で切替 → `greeting=v3`、rollback で元に戻る |
| `p2.placement_jp_label_admitted` / `p2.placement_us_refused` | `--region jp` は exit 0、`--region us` は `exit=6 reason=placement`（HTTP 503、緩めない） |
| `p2.metrics_snapshot` / `p2.metrics_operator_only` | operator token で 200 と `tsls_environment_starts_total` など（`p2-metrics.prom`）、tenant token で 401 |

```text
PASS  p2.burst_bounded                                           max environments of the revision observed=3 (limit 3; samples in p2-capacity-samples.ndjson)
PASS  p2.placement_us_refused                                    exit=6 reason=placement (never relaxed)
NOTE  region = "jp" is a scheduling LABEL written into this lab's config. It is not evidence of where data is stored or processed.
```

**P3**（`demo p3`）

| check | 期待 |
|---|---|
| `p3.async_accepted_202` / `p3.async_succeeded` | `POST /v1/functions/{id}:invokeAsync` が 202、NATS JetStream 経由で dispatcher が実行して `succeeded` |
| `p3.async_large_input_object_store` / `p3.object_store_ciphertext` | 100 KiB の入力（inline 上限 64 KiB 超）で `data/objects` にファイルが増え、平文の pad が読めない |
| `p3.retries_then_dead_letter` / `p3.dead_letter_listed` | `fail_first=3`・`max_attempts 3` で `status=failed attempts=3`、`reason=attempts_exhausted` |
| `p3.redrive_needs_role` / `p3.redrive_accepted` / `p3.redrive_succeeded` | deploy+invoke の token では `code=forbidden`、invoke+redrive の on-call token で受付、4 回目の実行で `succeeded`（副作用は 1 回） |
| `p3.cron_created` / `p3.cron_fired` / `p3.cron_fire_ran` | 6 field の `*/2 * * * * *` が約 7 s で 2 回以上 fire、fire の invocation が `succeeded`、disable → delete |
| `p3.webhook_*` | `tsls triggers webhook-sign --secret-env` で署名した POST `/v1/hooks/{id}` が 202 → `succeeded`、偽署名は 401 |
| `p3.usage_report` | `tsls usage --group-by function` に行があり、`provisional=true billing_enabled=false` |
| `p3.budget_hard_limit_stops` / `p3.budget_is_per_function` / `p3.budget_released` | budget file で cpu-burn の `hard_limit_micros = 1` にすると 45 s 以内に `code=budget_exhausted reason=budget`、hello は受付のまま、既定に戻すと再び exit 0 |
| `secrets.not_leaked` | token・secret 値が `logs/`・`demo/`・`state.db` に 0 件 |

```text
PASS  p3.redrive_needs_role                                      deploy+invoke token: exit=2 code=forbidden
PASS  p3.cron_fired                                              accepted fires in ~7 s: 4 (then disabled)
PASS  p3.budget_hard_limit_stops                                 exit=2 code=budget_exhausted reason=budget
PASS  secrets.not_leaked                                         20 locations x 5 values checked, 0 hits (config/ and secrets/ hold them by design)

47 checks passed, 0 failed
```

`tsls budget` と `tsls usage` の表は先頭に `PROVISIONAL - ... nothing is charged, billing is disabled in this prototype` を出す。

### 4.4 日常操作

| やりたいこと | コマンド |
|---|---|
| 状態と health | `scripts/lab/lab.sh status`（FAIL があれば exit 1） |
| 止める（data は残す） | `scripts/lab/lab.sh down` |
| 再開 | `scripts/lab/lab.sh up`（同じ port・token・data。設定は毎回生成し直す） |
| gateway / nats の log | `scripts/lab/lab.sh logs gateway`、`logs nats`（`LINES_N=500` で行数） |
| command log の一覧 / 最新 | `scripts/lab/lab.sh logs commands`、`logs last` |
| 別の lab を並べる | `--lab-dir /path/to/other-lab`（port・token・data・cgroup 名はすべて別） |

## 5. component ごとの health 確認

`up` の最後と `status` が同じ表を出す。`demo` は開始前にこの表が全部 ok でなければ止まる。

| component | check | 期待 | 異常時 |
|---|---|---|---|
| nats-server | pid file の process | 生きている | §6.6 |
| nats-server | `GET http://127.0.0.1:<monitor>/healthz?js-enabled-only=true` | 200 | §6.6 |
| gateway | pid file の process | 生きている | `logs gateway`、§6.5 / §6.7 |
| gateway | `GET /healthz` | 200 | 同上 |
| gateway | `GET /readyz` の `.ready` | `true` | 下の行のどれが原因かを見る |
| provider | `/readyz .preflight` | `<provider> ok=true`（失敗した check 名を列挙） | process: bridge binary（`bootstrap`）。firecracker: §6.2 / §6.3 |
| dispatcher | `/readyz .dispatcher.fenced` | `false` | lease を失った。`down` → `up` |
| config-cache | `/readyz .control_plane.new_invocations` | `accepted config=fresh` | 台帳が読めない・設定配信の停止。`logs gateway` |
| usage-journal | `/readyz .usage` | `accepting=true journal_healthy=true collector_error=null` | §6.8（disk） |
| budget | `/readyz .budget` | `accepting=true store_healthy=true stalled=false file_error=null` | §6.7（budget file） |
| ledger | `state.db` の `MAX(schema_version)` = `crates/application/src/repository/sqlite/migrations/*.sql` の数 | `8/8`（現時点） | §6.4 |
| queue | `/metrics` の `tsls_async_queue_condition{condition="healthy"}` | `1` | §6.6 |
| trigger-scheduler | `/metrics` の `tsls_trigger_scheduler_owner` | `1` | 別 gateway が同じ data_dir を使っていないか |
| async-dispatcher | `/metrics` の `tsls_async_dispatch_runs_in_flight` | 値がある | `[queue]` が接続できていない |
| metrics | operator token で 200、tenant token で 401 | `401` | 設定の生成を確認（`up` をやり直す） |
| object-store | `<lab>/data/objects` がある、鍵が 0600、gateway log に `"objects":"filesystem"` | ok | `up` をやり直す（鍵は再利用） |
| console | `GET /console/` | 200（`up --console` のときだけ。既定は skip） | `bootstrap --console` |

## 6. 失敗時の復旧

どの失敗でも最初に見るのは、そのコマンドの command log（`scripts/lab/lab.sh logs last`）と `logs gateway`。lab は使い捨てなので、原因が分からなければ **`teardown` → `bootstrap` → `up`** がいつでも使える最終手段（§7）。稼働中の controller・DB・queue・object store・usage journal・worker の障害で何が起き、どう収束するかの実測は docs/failure-matrix.md（PLT-4646、`scripts/chaos/matrix.sh`）にある。この節は lab の立ち上げと運用で詰まる場所だけを扱う。

### 6.1 bootstrap の取得失敗・checksum 不一致

- 症状: `[lab] ERROR: download failed: <url>`、または `sha256 mismatch for <url>: expected ..., got ... The file was deleted and is NOT used`。nats は `[queue] ERROR: sha256 mismatch ... (refusing to run it)`。
- 取得失敗: §2.4 の宛先に出られるか確認（proxy、社内 firewall）。`scripts/lab/lab.sh bootstrap` を再実行すれば、取得済みで sha256 が合うものは再取得しない。
- 不一致: **pin を書き換えて通さない。** 取得途中のファイルは消えている。再実行して再現するなら、配布元の差し替え・経路上の改ざん・pin の誤記のどれかなので、release page の checksum と `deploy/lab/versions.lock`（nats は `deploy/nats/versions.env` も）を突き合わせ、pin を変える場合はその理由を PR に残す（preflight が 2 つの nats pin の一致を検査する）。
- `up` が `guest kernel sha256 changed since bootstrap` で止まる: `<lab>/cache/kvm/vmlinux` が書き換わった。`teardown` してから `bootstrap` し直す。

### 6.2 KVM の権限

- 症状: preflight の `kvm` が FAIL（`/dev/kvm missing` / `not rw`）、`up` 後の `provider` 行が `firecracker ok=false`。
- `/dev/kvm` が無い: BIOS/UEFI の VT-x / AMD-V、cloud VM なら nested virtualization を有効にする。macOS は Lima（docs/kvm.md §5）。
- 権限が無い: `sudo usermod -aG kvm $USER` の後に再ログイン。Lima の shell では group が反映されないので、VM を起動するたびに `sudo chmod 666 /dev/kvm`。

### 6.3 jailer / cgroup が使えない

- 症状: `/readyz` が 503 のまま、`provider` 行に `host_cgroup` や `jailer` が失敗 check として出る。invoke は `Unavailable` で拒否される（fail closed）。
- `provider` 行に `socket_path_length` が出る（`config-cache` 行も `refused`）: jail の中の vsock socket の path が 107 byte を超えた。preflight の `socket_path` は jail の path も数える（2026-09-17 の KVM 検証までは run directory だけを数えていたので、preflight が ok でも `up` で止まった）。短い `--lab-dir`（または短い clone の path）で `teardown` → `bootstrap` → `up` し直す。
- `privileges` が FAIL: passwordless sudo を用意するか、root で実行する。
- cgroup v2 でない（`/sys/fs/cgroup/cgroup.controllers` が無い）: systemd の unified hierarchy で boot する。
- 一時的に隔離なしで試すだけなら `scripts/lab/lab.sh down` → `LAB_FC_PRIVILEGED=0 scripts/lab/lab.sh up`（profile dev、制限は unverified。本来の検証にはならない）。

### 6.4 migration の失敗・新しい schema の拒否

- 仕組み: gateway は起動時、listen の前に `state.db` へ前進のみの migration を 1 transaction で適用する。失敗すると schema は前の版のまま残り、gateway は起動しない。
- 症状（新しい schema）: `up` が `[lab] ERROR: state.db has a newer schema than this gateway` で止まり、直前に `Error: conflict: state.db is at schema version 99, newer than this binary supports (8). Migrations are forward-only: run a newer gateway, or restore the database from before the upgrade` が出る（§9 で実際に起こして確認）。
- 復旧: 新しい commit の gateway を build し直す（`git pull` → `bootstrap`）か、使い捨ての lab なので `teardown --keep-cache` → `up`（`--keep-cache` を付けないと取得物も消えるので `teardown` → `bootstrap` → `up`）。down migration は無い。台帳を残したい場合は、`down` してから `<lab>/data/state.db*` を lab の外に退避してから teardown する。
- 症状（migration 自体の失敗）: `migration 00N_<name> failed: ...`。disk 満杯（§6.8）か、壊れた `state.db`。disk を空けて `up`、直らなければ teardown。

### 6.5 port が使用中

- 症状: preflight の `port:GATEWAY_PORT` が `is used by another process`、`up` が `[lab] ERROR: port <n> is in use` で止まり gateway log に `Address already in use`（§9 で確認）。
- 他の lab か、前回の gateway の生き残り: `scripts/lab/lab.sh status` / `teardown` で止める。
- 無関係の process: それを止めないなら、`down` 状態で `<lab>/manifest.env` の `GATEWAY_PORT=`（または `NATS_PORT=` / `NATS_HTTP_PORT=`）を空にして `up` すれば、新しい空き port が選ばれる。

### 6.6 nats-server が起動しない

- 症状: `[lab] ERROR: nats-server did not start`。直前に nats の log が出る（port 使用中なら `[FTL] Error listening on port: 127.0.0.1:<n> ... address already in use`。§9 で確認）。
- port: §6.5 と同じ（`NATS_PORT` / `NATS_HTTP_PORT`）。
- 設定: `<lab>/nats/nats-server.conf` は `deploy/nats/nats-server.conf` から毎回描画される。`logs nats` を見る。
- store の破損: `down` → `<lab>/nats/jetstream` を lab の外へ退避 → `up`。stream にあった未配送の event は失われうる（lab ではこの復旧後の収束を確認していない。使い捨ての lab なら `teardown --keep-cache` → `up` の方が確実）。
- 稼働中に nats だけ落ちた: `status` の nats-server 行と queue 行が FAIL になる。停止中の `invokeAsync` の受付・拒否は ADR-0010「queue 停止時の方針」のとおり（`scripts/queue/async-e2e.sh` が検査）。`up` を再実行すると、生きている gateway はそのままで nats だけ起動し直す。

### 6.7 設定が不正

- gateway の設定は `up` が毎回生成するので、手で直さない（`deny_unknown_fields` で未知の key は起動時に拒否される）。起動しないときは `logs gateway` の最後の `Error:` 行を見て、lab.sh のバグなら issue にする。
- budget file（`<lab>/config/budgets.toml`）は稼働中に読み直される。壊すと `status` の budget 行が `file_error=budget file: TOML parse error at line ...` の FAIL になり、**直前の正しい設定が使われ続ける**（受付は止まらない。§9 で確認）。ファイルを直すと次の配信（数秒）で `file_error=null` に戻る。
- `Host.BudgetUnknown`（503）が出る: budget file に `[default_tenant]` が無く、その tenant の行も無い（fail closed）。`up` が作る既定の file に戻す。

### 6.8 disk 満杯

- 症状: `usage-journal` 行が `accepting=false`（journal に書けないと新しい invoke を 503 で拒否する）、`migration ... failed`、nats の `insufficient resources`、build の `No space left on device`。
- 復旧: 空きを作る（`target/` は `cargo clean` で数 GiB）→ `down` → `up`。lab の data 自体が大きいなら `teardown`。
- Lima VM の場合、host 側の disk が満杯になると VM の ext4 に I/O error が出て、長さ 0 の binary や 0 埋めのファイルが残る。host の空きを作って VM を再起動し、長さ 0 のファイルを消してから `bootstrap` し直す。

### 6.9 環境が残る・止まらない

- 症状: `status` は ok だが `tsls capacity --json` の `environments` が 0 に戻らない、`teardown` の孤児検査に bridge / firecracker process が出る。
- gateway を SIGKILL した後など: `up` をもう一度実行する。gateway は listen の前に起動時 reconcile（台帳の収束と孤児環境の回収）を行い、`/readyz .reconcile` に件数が出る。
- それでも残る: `teardown`。lab directory を command line に含む process（jailer / firecracker / 生成物から起動した user process）を SIGKILL し、`tsls-<lab id>` の cgroup、台帳に記録された環境の tap（`tsls` + sha256(env_id) の 11 hex）と nftables chain `g_<tap>` を消す。
- 実行中の invocation だけ止めたい: `tsls functions cancel <invocation id>`。

## 7. teardown と孤児検査

### 7.1 手順

```sh
scripts/lab/lab.sh teardown --dry-run     # 何を消すか・今動いているものを表示（何も変えない、exit 0）
scripts/lab/lab.sh teardown               # 実行。orphan check が clean なら exit 0
scripts/lab/lab.sh teardown --keep-cache  # pin 済みの download（<lab>/cache）は残す
scripts/lab/lab.sh teardown --purge       # clean のときだけ lab directory ごと消す（command log は $TMPDIR に残す）
```

teardown が対象にするのは **この lab のものだけ**:

1. process: `run/gateway.pid` の gateway を SIGTERM（実行中の invocation は cancel、環境は破棄）→ nats-server を SIGTERM → command line に lab directory の path を含む残りの process を SIGKILL
2. Linux の host resource（名前で特定）: cgroup `/sys/fs/cgroup/tsls-<lab id>`、台帳にある環境 id から計算した tap と nftables chain
3. file: `fc/jail`、`fc`、`nats`（JetStream の stream data）、`data/objects`（object root）、`data`（data_dir: state.db・artifacts・usage）、`demo`、`run`、`config`、`secrets`、`cache`（`--keep-cache` で残す）
4. 孤児検査（下）

安全策:

- lab directory には `.tsls-lab`（lab id）が必要。marker が無く中身がある directory は lab として扱わない。
- 消す path はすべて、symlink を解決した上で lab directory の **内側** にあることを確認してから消す。外側なら `refusing to remove ...` で止まる。
- `--lab-dir` に `/`、`$HOME`、リポジトリ root、`/tmp` などは指定できない。
- 他の lab・他の gateway・`scripts/e2e` / `scripts/queue` の scratch directory・他人の nats-server には触れない（process は lab directory の path で、cgroup / tap / chain は lab id と台帳で特定するため）。
- `manifest.env`（`STATE=torn_down`）と `logs/` は残る。同じ directory でもう一度使うときは、`--keep-cache` で消したなら `up` から、cache も消したなら `bootstrap` から（`up` は `lab not bootstrapped` で止まる）。lab id は変わらず、port・token・鍵・台帳は新しく作られる。

### 7.2 孤児検査の読み方

期待出力:

```text
4. orphan check
orphan check: clean (lab lab-d4c0557e)
```

残りがあると `LEFTOVER <種類>` を 1 行ずつ出し、`orphan check: leftovers found` で exit 1。

| LEFTOVER | 意味 | 対処 |
|---|---|---|
| `processes naming <lab>` | lab の path を含む process が SIGKILL 後も生きている（root 所有で sudo が無い、D state） | `ps -p <pid>`。root 所有なら sudo を用意して `teardown` を再実行 |
| `runtime bridge / user processes` | この checkout の build の bridge、または lab の artifact から起動した process（同じ checkout の別 lab が動いている場合もここに出る） | 他の lab が動いていないか確認。いなければ `kill` |
| `gateway pid file with a live process` | gateway が止まらなかった | `kill -KILL <pid>` → `teardown` |
| `cgroup /sys/fs/cgroup/tsls-<lab id>` | cgroup に process が残っている | `cat .../cgroup.procs` → 止める → `teardown` |
| `tap tsls...` / `nft chain g_tsls...` | egress 付きの環境の後始末が残った | sudo を用意して `teardown` を再実行 |
| `loop devices` | lab の image が loop device に attach されたまま（lab は attach しない） | `sudo losetup -d` |
| `path <lab>/...` | 消せなかった file（root 所有など） | sudo を用意して `teardown` を再実行 |

lab の外の古い残骸（`scripts/kvm/teardown.sh` が扱う `.kvm/run`、`scripts/e2e/orphan-check.sh` の全体検査）は lab の検査範囲外。

## 8. lab directory と command log

```text
<lab>/                       既定 <repo>/.lab（.gitignore 済み）
  .tsls-lab                  lab id（所有の印）
  manifest.env               lab id、provider、port、取得物の sha256、git commit、状態
  logs/commands/<UTC>-<command>-<pid>.log   全 subcommand の時刻付き transcript
  logs/gateway.log           gateway（JSON log、起動ごとに区切り行）
  secrets/ (0700)            tokens.env, object.key, triggers.key（0600）
  config/                    gateway.toml（0600）, budgets.toml
  data/                      data_dir: state.db, artifacts/, objects/, usage/, process/
  nats/                      nats-server.conf, auth.conf, gateway.password, jetstream/, nats-server.log
  cache/nats, cache/kvm      pin 済みの download、firecracker / jailer / vmlinux / rootfs.ext4
  fc/run, fc/jail            firecracker の workdir と jailer の chroot
  run/gateway.pid
  demo/<UTC>-<phase>/        results.txt と各 check の JSON、p2-metrics.prom など
```

command log の形式: 先頭に `# command:`、`# started:`、`# lab_dir:`、`# lab_id:`、`# host:`、`# git:`（未 commit の変更があれば `(dirty)`）、各行に UTC の時刻、最後に `# finished: ... exit=<n>`。実行したコマンドは `+ tsls ...` / `+ curl -X POST <url>` の形で出る（token と Authorization header は出さない）。追試の記録として、この directory をそのまま渡せばよい（`secrets/` と `config/` は除く）。

## 9. 検証の記録

| 日付 | 誰が | 環境 | 範囲 | 結果 |
|---|---|---|---|---|
| 2026-09-17 | **自動化された agent**（実装した agent 自身が、作業ツリーではなく GitHub から fresh clone した別 directory で、この文書の §4.1 のコマンドだけを順に実行。**別の人間による追試ではない**） | Darwin 25.6.0 arm64、process provider | preflight → bootstrap → up → demo all → status → CLI → teardown、§6.4 / §6.5 / §6.6 / §6.7 / §6.9 の失敗を起こして記載の手順で復旧 | すべて通過（demo 47/47）。1 回目の追試で見つけた不足 5 件を直してから最終 commit で再実行。記録 `docs/evidence/lab-20260917T1219Z-process-clean-clone/`、詳細と不足の一覧は docs/acceptance.md「PLT-4648」 |
| — | — | Linux/KVM、firecracker provider | §4.2 | **未検証**（§10） |

## 10. まだ再現できないこと

- **lab.sh の firecracker 経路**: 実機（KVM host）で通していない。構成要素（Firecracker v1.17.0、kernel 6.1.155、jailer、cgroup required、gateway 経由の E2E）は aarch64 の Lima nested virt で別の script から確認済みだが、lab.sh の生成設定・sudo 起動・teardown の cgroup / jail 削除は未確認。
- **x86_64**: bare metal でも VM でも、このプロトタイプを Firecracker で動かした記録が無い。versions.lock の x86_64 kernel / Firecracker の sha256 は配布物から計算した値で、boot は確認していない。
- **bare metal**: KVM の記録はすべて nested virtualization 上。時間の値は参考値。
- **KVM CI runner**: 未登録（docs/ci.md §4.4 / §5）。`scripts/lab/**` は CI では shellcheck だけで、KVM job は lab.sh を実行しない（そのため KVM 必要の分類にもしていない。docs/ci.md §3）。
- **既存 Tachyon Console への統合**: 未着手（docs/console-integration.md）。lab の `--console` は本リポジトリの最小 console。
- **別の人間による追試**: まだ無い（§9）。
- 複数 host、TiDB（lab は SQLite）、public ingress、本物の secret backend、課金は範囲外。
