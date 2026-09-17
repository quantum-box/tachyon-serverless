# ADR-0005: egress profile `restricted` / `public-web` は provider が作る tap と provider 所有の nftables table で強制し、検証済みの policy が無ければ起動しない

## ステータス

Accepted（2026-09-17、PLT-4622）。実装: `crates/providers/firecracker/src/network.rs`、`crates/providers/firecracker/src/egress_gate.rs`、`crates/domain/src/egress.rs`。実測: `docs/evidence/isolation-20260917T031126Z/`（`scripts/kvm/measure-isolation.sh` の NET）。

## コンテキスト

- PLT-4622 は `none` / `restricted` / `public-web` の egress profile を default-deny で強制し、管理網・node・metadata（169.254.0.0/16）・link-local・RFC1918 / CGNAT / loopback と IPv6 の同等範囲を遮断し、2 tenant 間・管理網・IPv4 / IPv6・DNS と直接 IP・redirect 経由の遮断を検証し、user code が policy の適用前に動かないこと（初期 egress の race）を求める。
- それまでの main は `none` だけを「NIC を付けない」ことで構造的に強制し（M8 実測）、`InstanceStart` の前に NIC / MMDS が無いことを確認する egress gate を持っていた。`restricted` / `public-web` の revision は deploy では受理され、環境作成で `InvalidSpec` になっていた（capability は `Unsupported`）。
- 検証 host は Lima VM（aarch64、nested virtualization）で、root になれる。開発機（macOS、非 root の Linux）では network を触れない。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | 環境ごとの tap + provider 所有の nftables table（本 ADR） | 採用 |
| B | 共有 bridge に全 guest を繋ぎ、bridge filter で分ける | 不採用。L2 を共有するので ARP / 近隣探索の偽装を別途塞ぐ必要があり、1 本の規則の誤りが全 tenant に波及する |
| C | guest ごとの network namespace + veth + iptables | 不採用。namespace と veth の後始末対象が増え、iptables-legacy / nft の混在を抱える。jailer 導入時に再検討する |
| D | guest 内（bridge の init）で firewall を張る | 不採用。guest の root（user code）が外せる |
| E | HTTP(S) proxy だけを許す | 不採用。TCP / UDP 一般の要件を満たさず、proxy 自体が管理網に置かれる |

## 決定

1. **`none` は従来どおり NIC を付けない。** tap も nftables も作らず、egress gate は NIC / MMDS の存在を拒否する。
2. **`restricted` / `public-web` の環境には provider が tap `tsls<sha256(env_id) 先頭 11 hex>` を 1 本作り**、`[provider.firecracker.network] guest_cidr`（既定 `172.30.0.0/16`、private 範囲であることを設定検査で強制）から /30 を割り当てる（host 側 `.1`、guest 側 `.2`、`<env_dir>/net.json` に記録）。tap は IPv6 無効、ICMP redirect の受信・送信なし、source route なし、`rp_filter=1`。guest の設定は kernel 引数 `ip=<guest>::<host>:255.255.255.252::eth0:off[:<resolver>]` と `ipv6.disable=1` だけで行い、MMDS は使わない。rootfs の `/etc/resolv.conf` は `/proc/net/pnp` への symlink。
3. **host firewall は provider 所有の `table inet tachyon_egress`（comment `tachyon-serverless egress v1`）1 つ。**
   - `forward`（filter、priority -10）: `iifname "tsls*"` は `guest_egress` へ、`tsls*` 宛ては `ct state established,related` だけ通し残りを drop（inbound なし、tenant 間なし）。
   - `guest_egress`: IPv6（`blocked_ipv6` 集合と全 IPv6）を drop → `iifname vmap @guest_taps` → **最後に drop**（map に載っていない tap は何も通らない）。
   - `input` / `output`: `tsls*` からの / への通信を drop（node 自身とその上の service に届かない）。
   - `postrouting`（nat）: `ip saddr <guest_cidr> oifname != "tsls*" masquerade`。
   - 環境ごとの chain `g_<tap>`: IPv4 以外を drop → 割り当てた guest IP 以外の送信元を drop → `blocked_ipv4`（`BLOCKED_IPV4`: 0/8、10/8、100.64/10、127/8、169.254/16、172.16/12、192.0.0/24、192.0.2/24、192.88.99/24、192.168/16、198.18/15、198.51.100/24、203.0.113/24、224/4、240/4）を drop → profile の規則。
     - `public-web`: 設定した resolver（公開 unicast であることを設定検査で強制、既定 1.1.1.1）への 53/tcp+udp だけ accept、それ以外の 53 は drop、残り（= 公開 IPv4 unicast）を accept。
     - `restricted`: revision の `egress_allow`（IPv4 CIDR × tcp/udp × port、1..=16 規則、各 1..=16 port）だけ accept、最後に drop。
4. **検証してから起動する（初期 egress race の排除）。** `HostNetwork::setup` は table の構造、環境 chain の規則数・各規則の verdict と必須リテラル（guest IP、`@blocked_ipv4`、resolver、許可 CIDR）、map の tap → chain を `nft -j list table` で読み戻して比較し、一致したときだけ `VerifiedPolicy` を返す（tap の link up はその後）。provider はその後にだけ `PUT /network-interfaces/eth0` を計画に入れ、egress gate は「NIC は検証済み policy の tap を指す eth0 1 本だけ、MMDS なし」を計画と `GET /vm/config` の両方で確認し、**`InstanceStart` の直前にもう一度読み戻す**。どれかが失敗すれば VM は起動せず、tap / chain / map / lease を消して `Boot` エラーになる。gateway.log には環境ごとに `egress policy installed and verified` が `InstanceStart accepted` より前に出る。
5. **後始末。** terminate は VMM を止めた後に map の要素・chain（削除前に counter 付きで log）・tap を消し、消えたことを読み戻して確認する。workdir に他の lease が無ければ table も消す。起動時の reconcile（`list_environments`）は、環境ディレクトリに対応しない tap・chain・map 要素を消し、使われていない table を消す（古い版の table も作り直される）。
6. **allowlist は revision spec の一部。** `RevisionSpec.egress_allow`（空なら serialize しないので既存 revision の digest は変わらない）。`restricted` は 1 規則以上必須、他の profile では拒否。`BLOCKED_IPV4` と重なる CIDR と IPv6 は deploy 時に 400。provider も作成時に再検査する。warm pool の `network_policy_version` は allowlist を含む。
7. **hostname の allowlist は実装しない。** 環境作成時に host が名前を解決して CIDR に落とす案は、TTL・CDN の複数アドレス・guest 側の解決（restricted では DNS を許さない）と噛み合わず、許可範囲が時間で変わる。必要になったら「作成時に解決した /32 の集合として記録する」形で別 ADR にする。
8. **権限。** tap と nftables には `CAP_NET_ADMIN` が要る。provider は構築時と環境作成時に Linux・`CAP_NET_ADMIN`・`nft` / `ip`・`/dev/net/tun`・`net.ipv4.ip_forward=1` を確認し、満たさない host では `egress_restricted` / `egress_public_web` を理由付きの `Unsupported` にし、その revision の環境作成を何も作らずに `Unavailable` で失敗させる。preflight には optional な `egress_network` check として出し、readiness（`ok`）には数えない。`none` は影響を受けない。`ip_forward` は host の設定であり provider は変更しない。

## 結果（consequences）

- Firecracker provider の capability `egress_restricted` / `egress_public_web` は、実測（`docs/evidence/isolation-20260917T031126Z/`）を経て、条件を満たす host で `Supported`。
- gateway は root（または `CAP_NET_ADMIN`）で動かす必要がある。`scripts/kvm/measure-isolation.sh` は NET の計測時に gateway を `sudo` で起動し、`ip_forward` を一時的に 1 にして戻す。
- host に別の firewall（Docker の `FORWARD` DROP、ufw など）があると、それが先に落とす場合がある。本 table は tap に関する通過を許すだけで、他の table の drop を上書きしない（nftables の仕様）。
- 同じ host で複数の provider（workdir 違い）を動かすと、tap 名 prefix と table が共有され、reconcile が互いの tap を orphan とみなす。1 host 1 Firecracker provider を前提とする。
- 帯域（Firecracker の `rate_limiter`）と接続数の上限は未設定。public-web の guest は許された範囲で帯域を使い切れる。
- DNS は resolver を 1 つに固定するだけで、DNS over HTTPS / 任意の 443 への tunnel は `public-web` の定義上防げない。`restricted` の guest には resolver が知らされず、allowlist に resolver の 53 番を入れない限り DNS は通らない。
- 実測は aarch64 の nested virtualization（Lima VM）1 host だけで、x86_64・bare metal・別の host firewall が同居する host では未確認。

## 検証

`scripts/kvm/measure-isolation.sh`（`docs/kvm.md` §3.6）の NET。`docs/evidence/isolation-20260917T031126Z/`（exit 0、M8 / M9 / DISK / NET すべて PASS）:

| 検査 | 結果 |
|---|---|
| public-web | 1.1.1.1:443・1.0.0.1:443 に接続、1.1.1.1 への UDP DNS と example.com の解決・接続が成功。169.254.169.254、10.0.2.2、100.64.0.1、192.168.0.1、管理網の gateway（192.168.5.2:22 / :53）、node（192.168.5.15:22 / :8080、host からは 22 が open）、node の tap（172.30.0.1:22、172.30.0.5:22）は timeout、IPv6（`[2606:4700:4700::1111]:443`、`[::ffff:169.254.169.254]:80`）は guest に IPv6 が無く失敗、8.8.8.8 と 192.168.5.2 への UDP DNS は無応答。`169.254.169.254.nip.io` は 169.254.169.254 に解決されるが接続は timeout。httpbin の 302（Location `http://169.254.169.254/latest/meta-data/`）の追従先も timeout。16/16 拒否・4/4 許可 |
| restricted（`1.1.1.1/32:443` のみ） | 1.1.1.1:443 だけ接続。1.0.0.1:443、1.1.1.1:80、metadata、管理網、node、node の tap、IPv6、1.1.1.1 / 8.8.8.8 への UDP DNS、example.com の解決はすべて失敗。11/11 拒否・1/1 許可 |
| 2 tenant 同時 | tenant B（public-web、guest 172.30.0.2 が 8080 で listen）と tenant A（public-web、172.30.0.6）を同時に起動し、`net-cross-during.txt` に 2 つの lease・2 つの tap・map の 2 要素を記録。A から B の guest（:8080 / :22）と B の tap（172.30.0.1:22 / :8080）は timeout、B が受け付けた接続は 0、A の 1.1.1.1:443 は成功 |
| 初期 race | policy を付けた 4 回の起動すべてで `egress policy installed and verified`（setup から読み戻しまで 61〜150 ms）が `InstanceStart accepted` より前 |
| 後始末 | 実行後に `tsls*` の tap 0、`table inet tachyon_egress` なし、`net.json` 0。teardown 時の counter は `net-counters.txt`（例: public-web の環境で `@blocked_ipv4` drop 19 packets、resolver 以外の DNS drop 1） |

この計測の最初の試行では、2 環境を同時に起動したときに tenant A の起動が `firecracker API: Connection refused` で失敗した（API socket のファイルが listen 前に見えた。policy は作られ、`InstanceStart` 前に消された）。provider は socket への接続が成功するまで待つように直し、上の記録はその修正後の再計測である。
