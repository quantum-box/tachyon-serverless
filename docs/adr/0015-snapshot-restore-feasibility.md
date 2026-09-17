# ADR-0015: snapshot / restore / clone は Firecracker の VMM 単体でだけ成立を確認した。Kata 経由は未対応、Cloud Hypervisor はこの host で失敗。本番化には protocol・bridge・host 側の追加が要る

## ステータス

Accepted（2026-09-17、PLT-4652、X1 実験・非ブロック）。決定 4 の patch は PLT-4653 で実験経路として実装した（`docs/adr/0017-snapshot-manifest-and-clone.md`）。本 ADR は**実験の記録と、PLT-4653 が満たすべき条件**を決める。provider の挙動は変えていない（`Capabilities` の `snapshot_create` / `snapshot_clone` は引き続き `Unsupported`、ADR-0001 決定 5）。

実験の実装: `experiments/x1-restore/`（`x1-guest-init`: 実験用 rootfs の PID 1、`x1-host`: 1 VMM 分の host 側、`pump` / `control` / `uevent` / `clock`）、`scripts/x1/fc-restore.sh`、`scripts/x1/ch-restore.sh`。証跡: `docs/evidence/x1-restore-20260917T085700Z/`（読み方は同ディレクトリの `README.txt`）。

## コンテキスト

- PLT-4651 で SDK に初期化保存点（`bootstrap` → `checkpoint` → `continue` → `after_restore`）を入れたが、snapshot を取る provider は無く、`continue` は常に `cold` だった（`docs/protocol.md` §B-X1）。
- PLT-4652 の目的は「VMM 単体で復元できること」と「Kata が管理する関数環境を clone できること」を分けて確かめること。受入条件: VMM 単体と Kata 経由それぞれに成功 / 失敗 / 未対応の結果がある。メモリと disk / 差分領域を整合させ、clone 間で書込み領域を共有しない設計を示す。Kata 所有の VM を別の管理者が直接操作する回避策を採らない。current main の仕様を稼働版の能力と混同せず、必要な patch / 制約を ADR に残す。通常起動を復元成功として報告しない。
- 本リポジトリの実行 provider は Firecracker（ADR-0001）。Kata は検証 host に入っておらず、Kubernetes も無い。Kata は source と文書の調査だけで判定した。
- 検証 host: Apple M4 上の Lima VM（vz、**nested virtualization**）、Linux 7.0.0-31-generic aarch64、4 vCPU / 8 GiB。同一 host・同一 CPU での save / restore だけ。時間の値は nested 上の参考値で SLA ではない。

## 固定した version

| 対象 | version | 確認方法 |
|---|---|---|
| Firecracker | v1.17.0（binary sha256 `fe726e0b…756c`） | `.kvm/manifest.json`、`fc/versions.txt` |
| guest kernel | Firecracker CI `vmlinux-6.1.155`（`CI_VERSION=v1.15`、arm64 Image、sha256 `e3544b10…c591`） | 同上 |
| Cloud Hypervisor | v53.0（`cloud-hypervisor-static-aarch64` sha256 `f192b510…2850`、`ch-remote-static-aarch64` `ade26617…2596`）と v51.1（`9b405476…0bf7` / `92550c36…526c`）。GitHub release asset の digest と一致を確認 | `ch-v53.0/versions.txt`、`ch-v51.1/versions.txt` |
| Kata Containers | 3.32.0（commit `337b6002…`、最新の 3.x）。`main`（`68b56713d9fa`）と主要点を照合。4.2.0 とは未照合 | `kata-sources.txt` |
| guest | `x1-guest-init`（未変更の bridge session + pump）/ product bridge、`examples/restore-aware`、vCPU 1、256 MiB、scratch 64 MiB | `fc/versions.txt` |

## 結果

| 経路 | 結果 | 根拠（`docs/evidence/x1-restore-20260917T085700Z/`） |
|---|---|---|
| **Firecracker VMM 単体**: checkpoint 待ちで Full snapshot → 別 VMM で load → guest 再開 → restored identity → invoke | **成功**（実験 harness 付き） | `fc/summary.tsv`: `PUT /snapshot/create` 232 ms（256 MiB、paused）。`PUT /snapshot/load` の API 応答 8〜10 ms。load 要求 → `Ready` は doorbell ありで 334 / 303 / 259 / 255 ms（中央値 281 ms）、doorbell なしで 3952 ms。同じ guest の cold boot（InstanceStart → `Ready`、bootstrap 込み）は 2623 / 2763 / 2979 ms。restore の判定は「restore 先 VMM での最初の frame が `x1_reconnect` で、`guest_boot_id` が source と同じ」（`hello` が来たら cold boot として exit 3）。全 5 copy が `restored=true`・`generation=1`・別々の `instance_id` で応答 |
| Firecracker: 同じ snapshot から 2 VMM 同時 clone | **成功** | `fc/clones-concurrent.txt`: 2 つの firecracker が同じ `snap/mem` を `rw-p`（MAP_PRIVATE）で map し、`Shared_Clean` 26.7 MiB / `Private_Dirty` 2.2 MiB。`fc/clones-disk.txt`: 各 clone の scratch には自分の `instance_id` だけ、snapshot 側の scratch には marker 無し、snapshot 3 file の sha256 は clone 実行後も同一 |
| Firecracker: **product bridge**（本番 rootfs の `/sbin/tachyon-init`）を restore | **失敗**（想定どおり） | `fc/bridge-restore/`: load は 204、host への再接続 0、4966 ms 後に bridge が `host connection lost; killing user process reason=frame writer stopped` で終わり guest が power off（firecracker rc 0）。現行 bridge は vsock の reset を越えられない |
| Firecracker: drive が記録された相対 path に無い | 失敗（期待どおりの拒否） | `fc/neg-missing-drive`: HTTP 400 `Load snapshot error: … Block: Virtio backend error: Error manipulating the backing file: No such file or directory (os error 2) scratch.ext4` |
| **Cloud Hypervisor VMM 単体**（v53.0 / v51.1） | **失敗**（この host では guest が bridge の handshake に届かず、稼働中 guest の save / restore を測れない） | `ch-v53.0/`、`ch-v51.1/`、`ch-attempts/`。下の「Cloud Hypervisor の経過」。VMM の操作だけは通る: 止まった guest に対して `ch-remote pause` + `snapshot file://` が 382 ms で受理され（`state.json` / `memory-ranges` / `config.json`）、別 process の `--restore` + `resume` 後に VMM state `Running`。guest の生存は**未検証**で、成功には数えない |
| **Kata 経由: VM template（factory）** | **未対応**（関数環境の clone には使えない） | template は sandbox を作る前に boot して agent を idle にした VM で、container・app 状態を持たない。QEMU と Go runtime の CLH（3.32.0 から、CH の `vm.snapshot` / `vm.restore` を内部で使う）だけ。runtime-rs は QEMU のみ、FC は no-op。virtio-fs 不可、initrd（QEMU）、CLH は memory hotplug 不可、confidential 不可、共有メモリの side channel 警告。`kata-sources.txt` §1 |
| **Kata 経由: 稼働中 sandbox の checkpoint / clone（containerd shim v2）** | **未対応** | Go shim の `Checkpoint` は `ErrNotImplemented`、runtime-rs は未実装（ttrpc の NOT_FOUND）、`docs/Limitations.md`「checkpoint と restore を提供しない」、CRIU 無し。VM snapshot の呼び出しは template 経路の中だけ。`kata-sources.txt` §2 |
| Kata 所有 VM を VMM API で直接操作（`clh-api.sock` / FC socket） | **不採用**（回避策として採らない） | 文書化も supported もされていない。Kata の `persist.json`（VMM / virtiofsd の PID・socket・device）、shim の `hpid` と監視、agent の ttrpc session と sandbox id、per-sandbox の hybrid vsock / virtiofsd、pod netns の tap と tc mirroring、sandbox / overhead cgroup、containerd の task 記録のどれとも整合しない。MAC / IP / entropy も複製される。`kata-sources.txt` §3〜4 |

## Firecracker で分かったこと

1. **vsock**。`SnapshotCreate` の時点で guest に `VIRTIO_VSOCK_EVENT_TRANSPORT_RESET` が送られ、再開後に接続済み socket はすべて閉じる。listen socket は残る（upstream `docs/snapshotting/snapshot-support.md`「Vsock device reset」、v1.17.0 L667-L677）。実測:
   - 再開した guest がそれに気づくのは**次に vsock に書いたとき**だった（`ENOTCONN`、`lost: "write: io: Socket not connected (os error 107)"`）。bridge の heartbeat は 5 秒ごとなので、load → 再接続は 3830 ms（`restore-1`）、前回の実行では 5 copy とも 4013〜4256 ms（`fc-run2-no-doorbell/`）。
   - **doorbell** で解消した。guest の listen socket（port 5001）は restore を越えるので、restore 先の host が load の直後に `CONNECT 5001` すると guest は即座に古い接続を捨てて再接続する。load → 再接続は 108〜125 ms（host 側の接続試行 3〜6 回）。
   - source を resume すると、source の guest も同じく再接続した（`source-resume-vsock-reset`）。**host 側の古い UDS 接続は閉じられないことがある**（`fc-run2-no-doorbell/source/host.jsonl`: guest は resume の約 0.3 s 後に再接続したが、host の古い接続の読み取りは source の VMM を kill するまで返らず、新しい接続は backlog に残った。記録を残していない別の実行では resume の直後に閉じた）。host は「同じ環境からの新しい接続」を古い接続より優先しなければならない。
   - `vsock_override`（v1.17.0 swagger L1767-L1773）で UDS path を clone ごとに変えられた（`restore-3`）。
   - VM generation の uevent（`NEW_VMGENID=1`）は guest（kernel 6.1.155）で**観測されなかった**。kernel は `random: crng reseeded due to virtual machine fork` を出し、`vmclock0` は登録されていたが、`x1-guest-init` の netlink 監視には何も届かなかった。restore の検知を uevent に頼れるとは言えない。
2. **disk の path**。drive の path は snapshot に記録され、load で上書きできない（v1.17.0 と main の swagger に drive の override は無い。`network_overrides` と `vsock_override` だけ）。相対 path で記録し、clone ごとの作業ディレクトリ（本番なら jailer の chroot）で解決させると、同じ snapshot を複数の VMM が別々の disk で使える。path に file が無ければ load は 400 で失敗し、Firecracker process は終了する（`neg-missing-drive`）。
3. **時計**。guest の wall clock は snapshot を取った瞬間から続く（upstream L515-L519）。実測で restore 後の guest `wall_ms` は host より 9〜15 秒遅れ（snapshot を置いていた時間分）、`CLOCK_MONOTONIC` / `CLOCK_BOOTTIME` も snapshot 時点の値（約 3.1 s）から続いた（`fc/restore-clocks.jsonl`）。`after_restore` で読んだ `started_at_ms` / `connection_opened_at_ms` も古い。`clock_realtime`（load 時に kvmclock を進める）は **x86_64 のみ**（swagger L1774-L1781）で aarch64 には無い。
4. **乱数**。clone ごとに `/dev/urandom` の値も example の `random` も異なった（`fc/restore-clocks.jsonl`、`fc/responses.tsv`）。guest kernel が VMGenID で CRNG を reseed したため（upstream L598-L620）。user space の PRNG・一意 ID・token は reseed されない（同文書）ので、`after_restore` で作り直す規約（PLT-4651）は必要なまま。
5. **identity**。`guest_boot_id` は全 clone で source と同じ。ADR-0011 の boot identity（`same_boot` / `boot_changed`）は boot_id だけでは clone を区別できない。
6. **secret**。clone の `secret_present` は false。process の環境は snapshot 元（source には secret を渡していない）のもので、restore 後に secret を渡す経路は無い（PLT-4651 の既知の欠落）。

## 決定

1. **Firecracker の snapshot / clone を X1 の対象 VMM とする。** Cloud Hypervisor は本 host で guest が動かないので判断材料にしない（再試行の条件は下記）。Kata 経由の clone は採らない（未対応）。**Kata 所有の VM を VMM API で直接 snapshot / restore する回避策は採らない。**
2. **保存点は「paused な VM のメモリ + device 状態 + disk image を同じ pause 点で取ったもの」とする。**
   - 順序: 環境が checkpoint 待ち（`x1_waiting` 相当の通知）→ `PATCH /vm Paused` → `PUT /snapshot/create Full`（`vmstate` + `mem`）→ **paused のまま**書込み可能 drive（scratch）を copy → 3 つを 1 組として封印（sha256）。source はその後 resume しない（terminate する）。resume するなら、copy は source の drive と別 file である。
   - guest の page cache にある未 flush の書込みはメモリ側に含まれるので、同じ pause 点で取った disk と組み合わせる限り整合する。disk だけ・メモリだけを後から差し替えない。
   - memory file は load 後も不変でなければならない（MAP_PRIVATE で page cache から読まれる。upstream L78-L86・L496-L499）。snapshot の組は read-only で置き、clone が生きている間は消さない。
3. **clone は read-only の土台を共有し、書込み領域を共有しない。**
   - 共有: rootfs（read-only）、function drive（read-only）、memory file（MAP_PRIVATE で読み取りだけ共有。書込みは各 VMM の匿名 CoW）。
   - clone ごと: scratch drive の copy（実験は `cp --sparse`。大きい disk では reflink / device-mapper snapshot / overlay 相当の CoW を使う。どれでも「clone の書込みが snapshot の file と他 clone に届かない」ことを満たす）、作業ディレクトリ（jailer chroot）、vsock UDS（`vsock_override` か chroot の同じ相対 path）、API socket、cgroup。
   - **ネットワーク（未実測。egress `none` だけで実験した）**: clone ごとに新しい tap と /30 と nftables chain を作り（ADR-0005）、`network_overrides` で tap を差し替える。guest の IP・MAC・resolver は kernel の `ip=` 引数とドライバの状態として snapshot に入っているので、restore 後に guest 側で netlink により付け直す（`after_restore` より前、bridge の責任）。付け直す前に NIC を通信させない。
   - **identity と secret は snapshot に入れない。** source の `HelloAck.env` には secret を入れず、restore 後の host → guest 通知で instance id・epoch・secret・現在時刻を渡す。
4. **PLT-4653 が満たすべき条件（必要な patch）。**
   - protocol: restore を知らせる frame（`instance_id`、`generation`、新しい `epoch`、host の現在時刻、secret 付きの env 差分）。未知 type はエラーなので `PROTOCOL_VERSION` を上げる（`docs/protocol.md` §B-X1）。実験の `x1_*` frame はその下書きで、protocol ではない。
   - bridge: vsock reset を越えて session を保つ（実験の pump 相当）。restore の合図は write 失敗に頼らず、host からの doorbell（guest の listen socket）で受ける。`continue` を restore 通知まで保留する `RestoreSource`。restore 後に wall clock を host の時刻に合わせてから `after_restore` を呼ぶ（aarch64 には `clock_realtime` が無い）。
   - host / provider: snapshot 中は `Host.InitTimeout` を止めるか延ばす（実験は `init_timeout_ms = 600000`）。restore 先では「最初の frame が restore 通知への応答で、`guest_boot_id` が snapshot と同じ」ことを確認し、`hello` なら cold boot として扱う。同じ環境からの新しい接続を古い接続より優先する。clone の boot identity を `(guest_boot_id, instance_id)` にする（ADR-0011 の `same_boot` 判定を clone に適用しない）。snapshot の組・clone の作業ディレクトリ・cgroup・tap の cleanup と起動時 reconcile。
   - 条件: snapshot を取った host と同じ CPU・host kernel・Firecracker version・guest kernel の組でだけ load する（upstream「Where can I resume my snapshots?」）。組の識別子を snapshot に付けて load 前に比べる。
   - セキュリティ: snapshot の memory file は guest のメモリそのもの（bootstrap 時点のデータ、TLS 等を含みうる）。tenant をまたいで共有しない。同じ tenant の clone 間でも page cache を共有するので side channel の前提を threat model に書く（Kata の template 文書も同じ警告）。Firecracker が検査するのは vmstate の CRC だけなので、封印した sha256 を load 前に照合する。
5. **Cloud Hypervisor を再評価する条件。** bare metal か x86_64 の host、CH の aarch64 向け kernel config（PL011 console、virtio-pci、MSI）で build した guest kernel、または UEFI firmware（`CLOUDHV_EFI.fd`）+ disk image。その上で本 ADR と同じ `scripts/x1/ch-restore.sh` を通す。

## Cloud Hypervisor の経過（本 host、すべて cold boot 段階）

| # | 設定 | 結果 |
|---|---|---|
| 1 | `console=ttyAMA0`、`root=` なし（Firecracker と同じ引数） | guest が約 1.1 s ごとに reboot を繰り返す（device が作り直される）。CH は Firecracker と違い root drive に `root=` を足さない。記録は残していない（script を直す前の実行） |
| 2 | `root=/dev/vda ro`、`earlycon=pl011` | 0.176 s で guest が power off。Firecracker CI kernel に PL011 driver が無い（`# CONFIG_SERIAL_AMBA_PL011 is not set`）ので console は出ない。記録は残していない。原因は #4 と同じと推定 |
| 3 | `console=hvc0` | console は `virtio_blk virtio1: [vda] 131072 512-byte logical blocks` で止まり、60 s 無応答（`ch-attempts/1-v53.0-hvc0-console-stall/`） |
| 4 | `console=hvc0 loglevel=1` | init まで届き `init setup failed: mount ext4 on /tmp: I/O error (os error 5)`。CH は image type を自動判定した raw disk の sector 0 への書込みを禁止し（`Autodetected raw image type. Disabling sector 0 writes.`）、scratch の ext4 は block 0 に superblock があるため rw mount が EIO になる（`ch-attempts/2-v53.0-scratch-mount-eio/`） |
| 5 | disk に `image_type=raw` | vsock 接続は host に届く（`accepted` 179 ms）が `Hello` frame が来ないまま 60 s（`ch-attempts/3-v53.0-vsock-connected-no-hello/`） |
| 6 | guest console なし | vsock 接続も来ない（`ch-attempts/4-v53.0-console-off-no-connection/`） |
| 7 | v51.1（Kata 3.32.0 の pin）で #5 と同じ設定 | 同じく `Hello` が来ない（`ch-attempts/5-v51.1-no-hello/`、`ch-v51.1/`） |

virtio-pci の device は guest から activate され、最初の数回の I/O（console の出力、vsock の接続）は通ってからその後の I/O が進まない。nested virtualization 上の割り込み / ioeventfd の配送か、CH 向けでない guest kernel のどちらかが原因と考えるが、切り分けていない。

## upstream の主張と稼働版の区別

- Firecracker: 本 ADR が引く文書と API は **v1.17.0 tag** のもの。`vsock_override`・`clock_realtime`（x86_64 のみ）は v1.17.0 にあり、Kata 3.32.0 が pin する **v1.12.1 には無い**（`network_overrides` はある）。drive path の override は v1.17.0 にも main にも無い。VMClock の `vm_generation_counter` は Linux 7.0 か backport した kernel が必要と upstream が書いており、本実験では試していない。
- Cloud Hypervisor: `memory_restore_mode=ondemand`（userfaultfd）と offload daemon は v53.0 の文書にあり、v51.1 の文書には無い（v51.1 は `net_fds` の差し替えまで）。いずれも本 host では試せていない。
- Kata: CLH の VM template は 3.32.0 で入った新機能で、runtime-rs には無い。main も 3.32.0 と同じ。4.2.0 は未照合。どの版でも「稼働中 sandbox の checkpoint」は無い。
- 本リポジトリ: main の provider は snapshot を取らない。`experiments/x1-restore` は製品の経路ではなく、gateway / provider はこれを使わない。

## 却下した案

| 案 | 理由 |
|---|---|
| Kata の CLH socket（`/run/vc/vm/<id>/clh-api.sock`）を外から叩いて sandbox を snapshot / clone | 受入条件で禁止。Kata の状態・agent session・netns・cgroup・containerd と不整合（上表） |
| Kata の VM template で関数環境を clone | template は sandbox 作成前の idle VM で、関数の初期化状態を含まない。cold boot の短縮にしかならない |
| restore の合図を bridge の heartbeat（write 失敗）に任せる | 実測で最大 5 s。host 起点の doorbell で 108〜125 ms |
| restore の合図を VMGenID の uevent に任せる | 本 guest kernel で観測できなかった |
| disk を snapshot と別の時点で取る / clone 間で scratch を共有する | メモリ上の page cache と disk がずれる。clone の書込みが混ざる |
| `guest_boot_id` を clone の identity に使う | 全 clone で同じ値 |

## 再現

Lima VM（`docs/kvm.md` §5）で `sudo chmod 666 /dev/kvm` の後:

```bash
scripts/x1/fc-restore.sh docs/evidence/x1-restore-<UTC>/fc    # 約 2 分。exit 0 = 期待した結果をすべて観測
scripts/x1/ch-restore.sh docs/evidence/x1-restore-<UTC>/ch    # 本 host では exit 1（guest が handshake に届かない）
```

どちらも作業ファイルを `.kvm/x1/` に作り、終了時に消す（`KEEP_WORK=1` で残す）。
