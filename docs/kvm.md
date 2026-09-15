# Firecracker / KVM で動かす（Track C: providers/firecracker）

- 対象: `crates/providers/firecracker`, `scripts/kvm/*`, `.kvm/`（実行時生成物、gitignore 済み）
- 関連: [architecture.md](architecture.md) §3・§4、[protocol.md](protocol.md) §C
- 状態: プロトタイプ。aarch64 の Linux/KVM（Apple M4 上の Lima VM、nested virtualization）で smoke と E2E デモが通った記録がある（§5、`docs/evidence/kvm-20260915T080221Z/`、`docs/evidence/20260915T125610Z-firecracker/`）。**x86_64 host と bare metal では未確認。** コードは macOS 上でも偽 Firecracker を使った lifecycle テストまで通る。実機での結果は `docs/evidence/kvm-<UTC>/`（smoke）と `docs/evidence/<UTC>-firecracker/`（E2E）に残す。

## 1. 目的

process provider（隔離なし）では「動いた」ことにならない縦断（PLT-4615 / PLT-4621）を、
Firecracker microVM で実際に通し、証跡（guest の boot_id・host の pid・boot/init/handler の時間）を残す。

```
fc-smoke / gateway ──ExecutionProvider──▶ FirecrackerProvider
   1. <workdir>/<env_id>/stage/app に artifact を置き mkfs.ext4 -d で function.ext4 を作る
   2. <env_dir>/v.sock_5000 で listen（InstanceStart より前）
   3. firecracker --api-sock <env_dir>/fc.sock --id env-... を自分のプロセスグループで起動
   4. PUT /machine-config → /boot-source → /drives/rootfs → /drives/function → /vsock → /actions InstanceStart
   5. guest の /sbin/tachyon-init（runtime bridge）が vsock で接続 → Hello → HelloAck → Ready → Invoke → Response
   6. Shutdown → guest が poweroff → terminate（SIGKILL を保険で送り、socket / drive / env dir を削除）
```

## 2. 前提

| 項目 | 要件 |
|---|---|
| OS / CPU | Linux x86_64 または aarch64。Firecracker は **host と同じアーキテクチャの guest だけ** を実行する |
| KVM | `/dev/kvm` が存在し、実行ユーザーで rw であること（`sudo usermod -aG kvm $USER` して再ログイン、または `chmod 666 /dev/kvm`）。**root は不要**。Lima VM では VM を起動するたびに権限が戻るので `sudo chmod 666 /dev/kvm` をやり直す（§5） |
| ネットワーク | P1 では guest にネットワークデバイスを付けない（egress none）。**tap は作らない**ので `CAP_NET_ADMIN` も不要 |
| jailer | 使わない。Firecracker は実行ユーザーの権限のまま動く（本番の隔離設計とは別） |
| ツール | `curl` `sha256sum` `tar` `mkfs.ext4`（e2fsprogs ≥ 1.43、`-d` オプション） `cargo` `rustup` `gcc`（musl target のリンカドライバ） |
| Rust | 1.95.0（`rust-toolchain.toml`）。`<arch>-unknown-linux-musl` target は bootstrap が追加する |
| パス長 | Unix socket のパス（`<repo>/.kvm/run/env_<26 文字>/v.sock_5000`）が **107 バイト以下**。深いディレクトリに clone すると超える。`scripts/kvm/preflight.sh` が検査する |
| 外部取得物 | Firecracker `v1.17.0` のリリース tgz（sha256 検証）と、Firecracker CI が公開する guest kernel（S3 `spec.ccfc.min`） |

## 3. 手順

すべてリポジトリルートから実行する。生成物は `.kvm/` 以下。

### 3.1 preflight

```sh
scripts/kvm/preflight.sh
```

表形式で ok / FAIL を出し、1 つでも FAIL なら非 0 で終了する。変更は加えない。

### 3.2 bootstrap（取得・ビルド）

```sh
scripts/kvm/bootstrap.sh
```

1. `FIRECRACKER_VERSION=v1.17.0` のリリース tgz と `.sha256.txt` を GitHub Releases から取得し検証、`.kvm/bin/firecracker` に展開
2. guest kernel: Firecracker v1.17 の getting-started と同じ方法で、S3 の `firecracker-ci/` 配下で最も新しい日付付き prefix（`YYYYMMDD-<sha>-<n>/`）から `<arch>/vmlinux-X.Y.Z` の最新版を選び `.kvm/vmlinux` に保存。URL と sha256 を `.kvm/manifest.json` に記録する。
   - 2 回目以降は manifest の sha256 と一致すれば再取得しない
   - `CI_VERSION=v1.15` のように prefix を固定、`GUEST_KERNEL_SERIES=6.1` で系列を固定できる（`firecracker-ci/v1.17/` という prefix は S3 に存在しないため、既定は日付 prefix の自動解決）
   - 実機で確認した組み合わせは `CI_VERSION=v1.15 GUEST_KERNEL_SERIES=6.1`（guest kernel 6.1.155、§5）。既定の自動解決は実機で確認していない
3. `rustup target add <arch>-unknown-linux-musl` → `cargo build --release --target <arch>-unknown-linux-musl -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn`
   - musl target は既定で static-pie。`readelf` があれば `PT_INTERP` が無いことを確認する。動的リンクになった場合は `RUSTFLAGS="-C target-feature=+crt-static"` を付けて再実行
4. host 用の `fc-smoke` をビルド（`target/release/fc-smoke`）
5. `scripts/kvm/build-rootfs.sh` を呼び、最後に各成果物の sha256 を表示

### 3.3 build-rootfs（単体でも実行可）

```sh
scripts/kvm/build-rootfs.sh            # BRIDGE_BIN / ROOTFS / ROOTFS_SIZE で上書き可
```

staging に `sbin/tachyon-init`（bridge の static バイナリ、0755）と空ディレクトリ `proc sys dev tmp function` を作り、
`mkfs.ext4 -q -F -d staging .kvm/rootfs.ext4 64M` で read-only の base rootfs を作る。root 不要。

### 3.4 smoke（gateway なしで microVM を 2 回起動）

```sh
scripts/kvm/smoke.sh
```

| ケース | 内容 | 期待 |
|---|---|---|
| `hello` | `example-hello` に `{"name":"kvm"}` を Invoke | `outcome=response`、terminate 後にプロセス・socket・env dir が残らない |
| `timeout` | `example-cpu-burn` に短い deadline（既定 3 秒）で Invoke（`--timeout-demo`） | deadline で host が `Cancel(grace 1s)` → `terminate(Timeout)` → SIGKILL。`outcome=timeout`、`terminate.was_running=true`、`leftovers.process_alive=false` |

cpu-burn の payload は `CPU_BURN_PAYLOAD`（既定 `{"seconds":60}`）で変更できる。
結果は `docs/evidence/kvm-<UTC>/` に保存され、最後に PASS / FAIL を表示する。確認済みの記録は `docs/evidence/kvm-20260915T080221Z/`。

`fc-smoke` を直接使う場合:

```sh
target/release/fc-smoke \
  --firecracker .kvm/bin/firecracker --kernel .kvm/vmlinux --rootfs .kvm/rootfs.ext4 \
  --workdir .kvm/run --binary target/$(uname -m)-unknown-linux-musl/release/example-hello \
  --payload '{"name":"kvm"}' --timeout-seconds 30
# 終了コード: 0 成功 / 1 失敗 / 2 preflight 失敗。stdout に JSON サマリ、stderr に進捗
```

### 3.5 gateway で動かす

`config/gateway.firecracker.toml`（Track D）で provider を firecracker にする。値は `.kvm/` の生成物を指す。

```toml
profile = "production"          # dev_only provider を拒否する側で動かす

[provider]
kind = "firecracker"

[provider.firecracker]
firecracker_binary = ".kvm/bin/firecracker"
kernel = ".kvm/vmlinux"
rootfs = ".kvm/rootfs.ext4"
workdir = ".kvm/run"
vsock_port = 5000
```

provider は相対パスをプロセスの cwd 基準で絶対化するので、gateway はリポジトリルートで起動する。
`TSLS_PROVIDER=firecracker scripts/e2e/demo.sh`（Track D）はこの設定で gateway を自分で起動し（`127.0.0.1:8080`）、登録 → publish → invoke → logs → timeout → rollback → 他 tenant 404 → cancel → 環境破棄を通す。別の gateway を同じポートで起動したまま実行しない。確認済みの記録は `docs/evidence/20260915T125610Z-firecracker/`（27/27 PASS）。
`GET /v1/provider` の capabilities は次のとおり（`Unverified` は「コードはあるが実機で未計測」）。

| capability | 値 |
|---|---|
| isolation | micro_vm |
| create_terminate / observe / enforce_deadline / egress_none | supported |
| enforce_resource_limits | unverified（vcpu / mem は machine-config で指定、ephemeral storage は未制御） |
| host_metering | unverified |
| egress_restricted / egress_public_web | unsupported（ネットワークデバイス未設定） |
| idle_quiesce / idle_resume / snapshot_create / snapshot_clone | unsupported（P1 未実装） |
| dev_only | false |

### 3.6 teardown

```sh
scripts/kvm/teardown.sh           # .kvm/run を参照する firecracker を SIGKILL → .kvm/run 削除 → 孤児監査
scripts/kvm/teardown.sh --purge   # .kvm を丸ごと削除
```

孤児監査: `pgrep -f "firecracker.*--api-sock <repo>/.kvm/run/"`、`.kvm` 配下の `*.sock` / `v.sock_*`、
`.kvm` のイメージに attach された loop device（想定 0）、`tsls*` という tap（想定 0）。残っていれば非 0。

## 4. 証跡の読み方

`docs/evidence/kvm-<UTC>/`:

| ファイル | 内容 |
|---|---|
| `summary.txt` | host / firecracker / kernel・rootfs の sha256 と、各ケースの主要値 |
| `hello.json` / `timeout.json` | `fc-smoke` の JSON サマリ（下表） |
| `*.stderr.txt` | 進捗ログ（preflight 表、guest のログ行 `[guest stdout init] ...`） |
| `*-console.txt` | guest のシリアルコンソール（kernel ログ + bridge の stdout/stderr） |
| `*-fc.txt` | Firecracker 自身のログ（`--log-path`, level Warning） |

JSON サマリの主なフィールド:

| フィールド | 意味 | 由来 |
|---|---|---|
| `hello.guest_boot_id` / `evidence.guest_boot_id` | guest の `/proc/sys/kernel/random/boot_id`。VM を起動するたびに変わる | guest 申告（Hello frame） |
| `evidence.host_pid` | Firecracker プロセスの pid（プロセスグループ leader） | host |
| `evidence.details.firecracker_version` | `firecracker --version` の出力（`v1.17.0`） | host |
| `evidence.details.kernel_sha256` / `rootfs_sha256` | 使った kernel / rootfs の sha256（manifest と一致するはず） | host |
| `evidence.details.vcpus` / `mem_mib` / `function_drive_bytes` | machine-config と function drive のサイズ | host |
| `timings.boot_ms` | create 開始 → bridge の vsock 接続 accept | host 計測 |
| `timings.init_ms` | 接続 → Ready 受信（`init_ms_guest` は guest 申告、参考値） | host 計測 |
| `timings.handler_ms` | Invoke 送信 → Response/Error 受信（`handler_ms_guest` は参考値） | host 計測 |
| `outcome` | `response` / `error` / `timeout` | host 判定 |
| `terminate.was_running` / `terminate.cleaned` | terminate 時に生きていたか、削除したパス一覧（`process-group:<pid>`, `archived:<path>` を含む） | host |
| `observation_after_terminate` | `{"state":"not_found"}` になること | host |
| `leftovers` | `process_alive=false`, `env_dir_exists=false`, `sockets=[]` になること | host |

「microVM で動いた」の根拠は、(a) `guest_boot_id` が毎回異なる、(b) `host_pid` が Firecracker のプロセスである（`*-fc.txt` に同じ instance id が出る）、
(c) `*-console.txt` に guest kernel の起動ログと `/sbin/tachyon-init` の出力がある、の 3 点を合わせて読む。
guest 申告値（`*_guest`, `hello.*`）は参考値で、課金・timeout の根拠は host 計測側（architecture.md §5-3）。

## 5. macOS（Apple Silicon）で試す: Lima + nested virtualization（確認済み）

2026-09-15 に次の環境で `scripts/kvm/smoke.sh` と `TSLS_PROVIDER=firecracker scripts/e2e/demo.sh` が通った。記録は `docs/evidence/kvm-20260915T080221Z/`（smoke）と `docs/evidence/20260915T125610Z-firecracker/`（E2E、27/27 PASS）。

| 項目 | 値 |
|---|---|
| host | Apple M4、macOS（Darwin 25.6.0） |
| Lima | 2.2.0（`brew install lima`）、`vmType: vz`、nested virtualization 有効 |
| VM | Lima の `template:ubuntu`、4 vCPU / 8 GiB / disk 40 GiB、kernel `7.0.0-28-generic` aarch64 |
| Firecracker | v1.17.0（aarch64、bootstrap が取得） |
| guest kernel | Firecracker CI の 6.1 系列（`CI_VERSION=v1.15 GUEST_KERNEL_SERIES=6.1`。console に `Linux version 6.1.155+`） |
| microVM | 1 vCPU / 256 MiB、rootfs と function drive は read-only、NIC なし |

Lima の文書によると、vz の nested virtualization には Apple M3 以降と macOS 15 以降が必要（Intel Mac / M1 / M2 は対象外）。実際に確認したのは上の M4 だけ。

### 5.1 VM を作る（macOS 側）

```sh
brew install lima                       # 2.2.0 で確認
limactl create --name tsls-kvm template:ubuntu \
  --vm-type vz --nested-virt --cpus 4 --memory 8 --disk 40 \
  --mount "<repo の絶対パス>:w"
limactl start tsls-kvm
limactl shell tsls-kvm
```

`--mount` はリポジトリを VM から見えるようにするためのもの（clone 元と、evidence の持ち帰り先）。ビルドと `.kvm/` はこの共有ディレクトリ（virtiofs）の上に置かない。

### 5.2 VM 内の準備（初回だけ）

```sh
sudo apt-get update
sudo apt-get install -y build-essential e2fsprogs curl jq
curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain 1.95.0
. "$HOME/.cargo/env"
rustup target add aarch64-unknown-linux-musl

# VM のローカルディスクに clone する（virtiofs の mount 上ではビルドしない）
git clone "<repo の絶対パス>" ~/tsls
cd ~/tsls
git checkout <確認するブランチ>
```

`~/tsls` なら Unix socket の最長パスは 85 バイトで、上限 107 バイトに収まる（evidence の preflight `socket_path_length`）。

### 5.3 VM を起動するたびに

```sh
sudo chmod 666 /dev/kvm     # VM の再起動で権限が戻るので毎回必要
```

### 5.4 取得・smoke・E2E

```sh
cd ~/tsls
CI_VERSION=v1.15 GUEST_KERNEL_SERIES=6.1 bash scripts/kvm/bootstrap.sh
scripts/kvm/smoke.sh
TSLS_PROVIDER=firecracker scripts/e2e/demo.sh
```

- bootstrap は上の kernel 指定で確認した。既定の `CI_VERSION=auto`（日付 prefix の最新 kernel）はこの環境では確認していない。
- `demo.sh` は `config/gateway.firecracker.toml` を使って gateway を自分で起動する（§3.5）。
- evidence は `~/tsls/docs/evidence/` にできる。macOS 側に残すときは mount 先へコピーする: `cp -R docs/evidence/<dir> "<repo の絶対パス>/docs/evidence/"`。

### 5.5 実測値（nested virtualization 上の参考値。SLA ではない）

上の 1 環境で 1 回ずつ実行した記録の値。nested virtualization のオーバーヘッドを含み、性能の約束でも、`docs/inventory-tachyon-apps.md` §6 の baseline（x86_64 第一、N ≥ 20、中央値・p95）の測定でもない。時間はすべて host 計測で、括弧内の guest 申告値は参考値（課金・timeout の根拠にしない。architecture.md §5-3）。

| 経路 | boot | init | handler | 出典 |
|---|---|---|---|---|
| gateway 経由（E2E、11 attempt） | `environment_boot_ms` 3522〜4754 ms | `runtime_init_ms` 331〜410 ms（guest 申告 219〜274 ms） | hello / http-axum の `handler_ms` 73〜93 ms（guest 申告 26〜42 ms） | `docs/evidence/20260915T125610Z-firecracker/invocations.json` |
| fc-smoke（最初の 2 回の起動） | `boot_ms` 13468 / 11036 ms | `init_ms` 1341 / 1224 ms（guest 申告 991 / 850 ms） | hello の `handler_ms` 319 ms（guest 申告 116 ms） | `docs/evidence/kvm-20260915T080221Z/hello.json`、`timeout.json` |

- どちらも `console=ttyS0` でシリアルに kernel ログを出している（§6）。fc-smoke の hello では `/sbin/tachyon-init` の起動が kernel 時刻 9.04 s（`hello-console.txt`）。
- E2E の timeout ケース（`timeout_seconds = 2`、grace 1 s）は `total_ms` 7178 ms、CLI から見た壁時計 7060 ms（boot を含む）。

### 5.6 注意

- ビルド、`target/`、`.kvm/`、`data/` は VM のローカルディスク（`~/tsls`）に置く。共有ディレクトリの上では作業しない。
- VM 内のパスは短くする。`/Users/...` の mount 先で動かすと Unix socket の 107 バイト制限を超えやすい。
- macOS 側のディスクの空きが無くなると VM 内のファイルが壊れる（§7.1）。bootstrap と `target/` で数 GiB を使う。

## 6. 既知の制約

- host と同じアーキテクチャの guest のみ。`validate_artifact` は ELF の `e_machine` を revision の宣言と host の両方に照合し、`PT_INTERP` があるバイナリ（動的リンク）は `artifact rejected`（rootfs に libc が無い）。
- ネットワークなし。`EgressProfile::None` 以外の spec は `InvalidSpec`。
- 1 環境 1 実行、destroy-after-invoke。warm / snapshot は `Unsupported`。
- `ephemeral_storage_mib` は未制御（function drive は read-only、`/tmp` は guest の tmpfs で上限なし）。
- 課金・メータリング用の host 側計測は timings のみ（`host_metering = Unverified`）。
- jailer なし。Firecracker は実行ユーザーとして動く。seccomp は Firecracker 既定。
- `--id` は英数字と `-` のみのため、環境 id の `_` を `-` に置換して渡す（`env_01h...` → `env-01h...`）。
- `console=ttyS0` でシリアル出力するため boot は速くない（証跡のため）。`boot_args_extra` に `quiet` を足すと短縮できるが console.log は減る。
- terminate 後のログは `<workdir>/_archive/<env_id>/` に移し、**最新 50 環境分だけ**残す（古いものから削除）。
- 環境ごとの `mkfs.ext4` はイメージを sparse で作った上でサイズ（artifact + 8 MiB を 1 MiB 単位に切り上げ）を明示する。e2fsprogs ≥ 1.43 が必要。
- `fc.log`（`--log-path`）は Firecracker が作成しないため provider が空ファイルを先に作る。
- Firecracker の guest 発の vsock 接続は `<uds_path>_<port>` に **ハンドシェイクなし**で転送される。host 発の接続（`CONNECT <port>\n` / `OK <port>\n`）は使わない。
- Unix socket パス長 107 バイト（`sun_path`）。

## 7. 失敗時の切り分け

`create_environment` が失敗すると `ProviderError::Boot(...)` のメッセージに `console.log` の末尾（4 KiB）と `fc.log` の末尾（2 KiB）が付く。
smoke の場合は `docs/evidence/kvm-*/hello.stderr.txt` に出る。ログの元ファイルは `.kvm/run/_archive/<env_id>/` に残る。

| 症状 | 見る場所 | 主な原因 |
|---|---|---|
| `API socket ... did not appear within 2s` | `*-console.txt`（Firecracker の stderr も入る） | `firecracker` が起動できない（アーキテクチャ違い、実行権限、`/dev/kvm` 権限、seccomp） |
| `firecracker exited before configuration completed` | `*-console.txt` | 上と同じ。`Could not open /dev/kvm` などの行が出る |
| `PUT /boot-source -> 400 ...` | エラー本文の `fault_message` | kernel / rootfs のパス、権限、`vmlinux` が gzip 圧縮されている（非圧縮の ELF が必要） |
| `PUT /machine-config -> 400` | 同上 | vcpu / mem の値（`ResourceProfile`）が範囲外 |
| `timeout waiting for guest bridge on .../v.sock_5000` | `*-console.txt` | kernel が起動していない（cmdline、`keep_bootcon`）、rootfs に `/sbin/tachyon-init` が無い / 実行不可、guest kernel に `virtio_vsock` が無い、bridge が `tachyon.vsock_port` を読めていない。console に bridge のエラーが出ているはず |
| Hello は来るが `InitError` | `*.stderr.txt` の `[guest stderr init]` 行 | `/function/app` が動的リンク（`validate_artifact` を通していない）、`/dev/vdb` の mount 失敗（`function.ext4` の作成失敗）、entrypoint の権限 |
| `outcome=timeout` になるべきでないのに timeout | `*.stderr.txt` | handler が遅い / deadline が短い。`--timeout-seconds` を伸ばす |
| terminate 後に `leftovers.process_alive=true` | `scripts/kvm/teardown.sh` | SIGKILL がプロセスグループに届いていない（`fc.pid` と実プロセスの不一致）。teardown で回収し、`.kvm/run` を消す |
| `socket_path_length` FAIL | preflight | パスが長い。短い場所に clone するか `workdir` を短いパス（例 `/tmp/tsls`）にする |

デバッグ用オプション:
- `fc-smoke --boot-args-extra "loglevel=8"` で guest kernel のログを増やす（`config` では `boot_args_extra`）。
- `RUST_LOG=debug` で provider の tracing（spawn / InstanceStart / terminate）を stderr に出す。
- `fc-smoke --skip-preflight` は preflight の FAIL を無視して起動を試みる（原因を絞るとき用）。

### 7.1 Lima / nested virtualization で起きたこと

| 症状 | 原因 | 対処 |
|---|---|---|
| VM 内で ext4 の I/O error が出る。`target/` に 0 バイトのバイナリが残る。gateway が `.../data/state.json is not a valid state file ... Move it aside to start with an empty ledger.` で起動しない（`state.json` がゼロ埋め） | macOS 側のディスクが一杯になり、VM の disk への書き込みが失敗した | macOS 側の空きを作る → `limactl stop tsls-kvm && limactl start tsls-kvm` → `sudo chmod 666 /dev/kvm` → VM 内で `find target -type f -size 0 -delete` → `mv data/state.json data/state.json.broken` → bootstrap からやり直す。壊れた `state.json` を黙って捨てない仕様は `crates/application/src/repository.rs::corrupt_state_file_is_refused_with_a_hint` |
| preflight の `kvm` が FAIL、または firecracker が `/dev/kvm` を開けない | VM を起動し直すと `/dev/kvm` の権限が戻る | `sudo chmod 666 /dev/kvm`（§5.3） |
| guest の console に bridge の `--environment-id` 不足のエラーが出て、Hello が来ない | kernel は `init=/sbin/tachyon-init` を引数なしで起動する | 修正済み（commit `8e2fbc7`: PID 1 なら自動で `--init`）。古い rootfs を使っている場合は bootstrap を再実行して bridge と `.kvm/rootfs.ext4` を作り直す |
| Hello は来るが、user process が Ready の前に終了する（Runtime API への接続が `ENETUNREACH`） | 起動直後の guest では loopback（`lo`）が down のまま | 修正済み（commit `c0f0ebe`: init モードで `lo` を up にする）。対処は上と同じく rootfs の作り直し |
