# ADR-0017: snapshot は署名した manifest で「どこに load してよいか」を固定し、1 field でも違えば拒否する。clone は Firecracker の jailer 下で新しい jail・cgroup・scratch copy に load し、source の boot id で再接続した guest だけを restored と数える（PLT-4653、X1 実験）

## ステータス

Accepted（2026-09-17、PLT-4653、X1 実験・非ブロック）。前提は ADR-0015（PLT-4652 の実験結果と決定 2〜4）と PLT-4651 の SDK lifecycle。**既定では何も変わらない**: revision の `restore.policy` は `disabled`、gateway の `[snapshots]` は無効、bridge と Firecracker provider の `experimental-restore` feature は off。

実装:

- manifest・互換検査・署名・restore policy: `crates/domain/src/snapshot.rs`（テスト `snapshot_tests.rs`）、`RevisionSpec.restore`
- protocol v3: `crates/protocol/src/{lib.rs, wire.rs}`（`CheckpointWaiting` / `Reconnect` / `Restore`、`HelloAck.snapshot_hold`、`MIN_PROTOCOL_VERSION` / `host_accepts`）、golden fixture `crates/protocol/tests/golden/wire/{guest_hello_v3, host_hello_ack_snapshot_hold, guest_checkpoint_waiting, guest_reconnect, host_restore}.json`
- bridge: `crates/runtime-bridge/src/restore_link.rs`（feature `experimental-restore`: hold、doorbell、再接続、restore frame、時計合わせ）
- provider port: `crates/provider-port/src/restore.rs`、`ExecutionProvider::{restore_profile, snapshot_dir, snapshot_environment, clone_environment}`（既定は `Unavailable`）
- Firecracker: `crates/providers/firecracker/src/provider/restore.rs`（feature `experimental-restore`）、fake: `crates/providers/fake/src/lib.rs`（`snapshot_root`）
- host session: `crates/application/src/bridge_session/restore.rs`（`handshake_for_snapshot`、`wait_checkpoint`、`restore_handshake`）
- 保管・catalog・計画: `crates/application/src/snapshot/{store.rs, service.rs, mod.rs}`（`[snapshots]`）
- invoke: `crates/application/src/services/invoke/restore.rs`（`prepare_cold` の先頭から、policy が `disabled` でないときだけ呼ぶ）
- API / CLI: `apps/gateway/src/snapshot_handlers.rs`（`POST|GET /v1/functions/{id}/snapshots`、`POST .../{snapshot_id}/revoke`）、`CreateRevisionRequest.restore`、`tsls functions deploy --restore-policy --synthetic-init-sample`
- example: `examples/restore-aware`（scratch drive に自分の instance id を書き、前の値と一緒に返す）
- E2E: `scripts/x1/clone-e2e.sh`、証跡 `docs/evidence/x1-clone-<UTC>/`
- テスト: `crates/application/tests/restore.rs`（fake provider）、security regression list の PLT-4653 節

## コンテキスト

- ADR-0015 で「Firecracker の VMM 単体で、checkpoint 待ちの Full snapshot を別 VMM に load し、doorbell で再接続させて restored identity を渡す」ことが実験 harness 付きで成立し、product bridge はそのままでは restore できないことが分かった。本 Issue は harness を製品の経路（bridge・provider・gateway）に入れ、**安全に識別・保管し、対応する構成でだけ復元を試せる**ようにする。
- 受入条件: 不一致 profile・壊れた artifact・他 tenant の snapshot を拒否する。本番 Secret / 顧客の実行済みデータを保存せず合成 sample だけを使う。clone ごとの identity と書込み領域を分離し、期限切れ / 更新済み artifact を使わない。prefer の fallback と require の失敗を区別し、cold 起動を restored と計測しない。

## 決定

1. **manifest（version 1）に load 可否を決める事実をすべて固定し、HMAC-SHA256 で署名する。**
   - tenant・function・revision id と spec digest、runtime（provider 種別と version、VMM / kernel / rootfs の sha256、bridge protocol version、jailer mode、cgroup mode、host kernel release）、host CPU（arch、cpuinfo の model / flags の hash、KVM API version と `KVM_CHECK_EXTENSION` 0..=255 の hash）、device model（drive の id / chroot 内 path / ro / root、vsock CID・port、doorbell port、NIC 数）、memory・vCPU、scratch の大きさ、egress と restricted の allowlist digest、暗号鍵世代（鍵の fingerprint）、secret 世代、SDK lifecycle version、checkpoint phase、source の環境 id と boot id、created / expires、4 artifact（memory・vmstate・scratch・function drive）の sha256 と大きさ。
   - canonical form は宣言順の compact JSON（map を含まない型）。署名は `label ‖ key id ‖ canonical bytes` の HMAC-SHA256。検証は key id → HMAC（定数時間）→ digest → parse → 再 serialize が署名した bytes と一致（canonical でなければ拒否）の順。
   - 互換検査 `check_compatibility(manifest, state, target, now)` は純関数で、**1 field でも違えば** `Incompatible{reasons}`。他 tenant は他の比較をせず `tenant_mismatch` 1 つだけを返す。state が `active` 以外、`now >= expires_at`、manifest version・checkpoint phase の不一致、egress が `none` 以外も拒否。「近いから可」は無い。
   - property test（外部 crate を使わない seeded generator）で、同一 target は常に互換・任意の 1 field の変更は理由付きで非互換・他 tenant は常に単独で拒否・期限切れ / 失効 / 隔離は常に拒否・改ざん / 他鍵 / 非 canonical は検証失敗、を数百件ずつ確かめる。
2. **artifact は data_dir に暗号化して保管し、load の直前に平文の digest を照合する。**
   - `<data_dir>/snapshots/<snapshot_id>/`: `manifest.json`（署名済み）、`record.json`（状態と restore 回数）、`<artifact>.sealed`（4 MiB chunk ごとの AES-256-GCM。AAD は tenant・snapshot・artifact 名・chunk 番号・最終 chunk flag なので、他 tenant / 他 snapshot への流用・並べ替え・切り詰め・追加は復号できない）。鍵は `[snapshots] key_file|key_env`、署名鍵は `signing_key_file|signing_key_env`（どちらも 64 hex、file は 0600 必須）。
   - Firecracker は memory file を MAP_PRIVATE で読むので、clone 用の平文を provider の `<workdir>/_snapshots/<id>/` に置く（directory 0700、memory / vmstate は 0640 root:<jail gid>、hard link で clone の chroot に入れる）。**毎回の restore 計画で 4 file の sha256 を manifest と照合し**、無い file は sealed から復号して照合し、違えば `artifact_corrupted` で拒否して snapshot を `quarantined` にする（隔離は後の状態変更で解除されない）。
   - 状態は `active` / `revoked` / `quarantined` / `expired`。期限到達で `expired`、暗号鍵・署名鍵・secret 世代の変化で `revoked`（stale）、`POST .../revoke` で `revoked`。revision の更新は manifest の revision / spec digest の不一致で拒否する（別 revision の snapshot を revoke はしない。alias で旧 revision に戻せば使える）。
3. **restore policy は revision ごと（`disabled` 既定 / `prefer` / `require`）で、結果を区別して記録する。**
   - `prefer`: 計画・clone・再接続・ready のどこで失敗しても clone を止めて terminate し、新しい環境 id で cold start する。attempt は `start_kind = cold`、環境の evidence に `restore_policy` / `restore_fallback`（code）/ `restore_fallback_detail`。
   - `require`: 同じ失敗は invocation の `init_error` / `Host.RestoreRequiredUnavailable`（message に code）。**cold にしない**。snapshot が無ければ何も起動しない。
   - `start_kind = restored` は provider が clone した VMM で、**最初の frame が `Reconnect` で、その environment id と `guest_boot_id` が manifest の source と一致し**、`Restore` を送った後に `Ready` が来たときだけ。`Hello` は `cold_boot_detected`（`HelloReject` して terminate）。evidence に `snapshot_id`、manifest digest、instance id、generation、`restore_{verify,load,doorbell,reconnect,ready}_ms`。boot identity は `<source boot id>/<instance id>`（ADR-0011 の `same_boot` を clone に適用しない、ADR-0015 決定 4）。
   - metrics は既存の `start_kind` label（`restored`）をそのまま使う。restored になるのは上の条件を満たした attempt だけ。
4. **合成 sample だけ、secret を保存しない。** `prefer` / `require` の revision と snapshot の作成は `restore.synthetic_init_sample = true`・secret binding なし・egress `none` の revision だけ（revision 作成と snapshot 作成の両方で検査）。source の `HelloAck.env` は revision の非 secret env だけ。snapshot は guest が `CheckpointWaiting{phase = checkpoint, after_restore_ran = false}` を報告した後にだけ取り、`Ready` が先に来た source は拒否する。source は resume せず terminate する（ADR-0015 決定 2）。restore 後に secret を渡す frame は作らない（必要になったら `Restore` に env を足し、version を上げる）。
5. **protocol v3 は restore frame だけを足し、v2 と共存させる。** host は 2〜3 を受け入れ guest の version で話す。v3 の frame と `snapshot_hold` は v3 の guest にだけ。bridge は feature `experimental-restore` でだけ 3 を名乗る（既定の build は v2 と byte 単位で同じ Hello）。v2 の golden fixture は変えない。bridge の restore link は hold が無ければ透過（接続が切れれば従来どおり exit 4）で、hold 中だけ再接続し、再接続後の `Restore` だけを受け付け、`CLOCK_REALTIME` を host の時刻に合わせてから `continue` に `restored` を答える。
6. **clone は Firecracker の jailer 下だけ。** snapshot に記録される drive / vsock の path を chroot 相対（`/scratch.ext4`、`/v.sock`）にするため。jailer が無ければ capability は `Unsupported`（理由付き）。
   - snapshot: `PATCH /vm Paused` → `PUT /snapshot/create`（chroot 内に先に作った `snapshot.mem` / `snapshot.vmstate` へ）→ paused のまま scratch を `cp --sparse=always --reflink=auto`、function drive は hard link → memory / vmstate を `_snapshots/<id>/` に移す。
   - clone: 新しい環境 directory・cgroup（`cpu.max` / `memory.max` / `pids.max`）・jail（chroot・uid 64000・PID / mount namespace）・vsock listener、scratch は private copy、function drive・memory・vmstate は hard link（read-only 共有）→ jailer で VMM 起動 → `PUT /snapshot/load{resume_vm: false}` → **egress gate**（`GET /vm/config` に NIC と MMDS が無い）→ `PATCH /vm Resumed` → doorbell（`CONNECT 5001`）→ guest の再接続を accept。egress `restricted` / `public-web` の clone は拒否（NIC の付け直しは ADR-0015 決定 3 のまま未実装）。
   - capability は `Unverified`。KVM で E2E を通したが、nested virtualization の 1 host・1 CPU・aarch64 だけで、x86_64・bare metal・host 間を測っていないので `Supported` にしない。使うには `[snapshots] allow_unverified`（計測 run）が要る。
7. **snapshot の作成は operator 操作（API）で、invoke は作らない。** `require` の revision が snapshot の無いまま invoke されたら失敗する。source の起動は admission の予約を通らない（残存リスク）。

## 結果（KVM）

`docs/evidence/x1-clone-20260917T114231Z/`（`scripts/x1/clone-e2e.sh`、commit `12bae8f`、18 check のうち FAIL 0）。同じ commit で `TSLS_PROVIDER=firecracker scripts/e2e/demo.sh` が 28/28 PASS（`docs/evidence/20260917T114908Z-firecracker/`）。Lima VM（Apple M4 上の vz、**nested virtualization**、aarch64、Linux 7.0.0-31、4 vCPU / 8 GiB）、Firecracker v1.17.0、jailer（uid 64000、new PID ns）、cgroup `required`、gateway は release build を root で起動、`examples/restore-aware`（256 MiB、1 vCPU、scratch 64 MiB、egress none）。時間は参考値で SLA ではない。

| check | 結果 | 値 |
|---|---|---|
| provider | PASS | `snapshot_create` / `snapshot_clone` = `unverified`（`allow_unverified` の計測 run） |
| cold 1〜3（policy なし） | PASS | boot + init から Ready まで 2794 / 2782 / 3122 ms（`total_ms` 2875 / 2856 / 3221） |
| require・snapshot なし | PASS | 502 `Host.RestoreRequiredUnavailable`（`no_snapshot`）、何も起動しない |
| snapshot 作成 | PASS | API 6.6 s。pause 0 ms、`snapshot/create` 354 ms、paused 中の scratch copy 13 ms、封印（AES-GCM + sha256、342 MiB）3092 ms。source の VMM は残らない |
| 2 clone 同時 | PASS | 2 invocation とも `start_kind = restored`、別の環境 id・instance id（generation 1 / 2）、各 scratch marker が自分の instance id で「前の値」は空、`secret_present = false`、guest 時計と host の dispatch 時刻の差 125 / 133 ms（1 秒精度）、clone 開始 → Ready 543 / 511 ms（load 20 / 20 ms、doorbell 97 / 103 ms、再接続 198 / 197 ms）、その前の verify 872 / 871 ms |
| memory の共有 | PASS | 2 つの VMM が同じ `/snapshot.mem`（同じ inode）を `rw-p`（MAP_PRIVATE）で map（`clones-maps.txt`） |
| 逐次 restore 1〜3 | PASS | clone 開始 → Ready 338 / 295 / 339 ms（load 25 / 22 / 23、doorbell 86 / 74 / 75、再接続 152 / 137 / 143）。その前の verify（256 MiB の sha256 を含む 4 file）942 / 734 / 708 ms。`total_ms` 1355 / 1097 / 1134 ms（cold 2856〜3221 ms） |
| revision 更新 | PASS | 新 revision（require）は旧 snapshot を `revision_mismatch` で拒否（502） |
| 1 byte 破損・require | PASS | memory の 1 bit 反転で `artifact_corrupted`（502）、snapshot は `quarantined` |
| prefer・正常 | PASS | restored（Ready まで 625 ms） |
| 1 byte 破損・prefer | PASS | 200、`start_kind = cold`、`restore_fallback = artifact_corrupted` |
| 後始末 | PASS | gateway 停止後に firecracker・jail・cgroup・環境 directory・snapshot directory が 0（`leftovers.txt`） |

読み方:

- VM 側（clone 開始 → Ready）は cold の boot + init の中央値 2794 ms に対して中央値 338 ms。client から見た `total_ms` は verify を含めて約 1.1〜1.4 s（cold 2.9〜3.2 s）。verify を省くと改ざん検出ができないので、fs-verity・起動時 1 回の検証 + immutable 化などは未解決（残存リスク）。
- 2 clone 同時の時は verify・scratch copy・load が並ぶので逐次より遅い。
- 同じ Issue の作業中、rebase 前の commit で 2 回走らせた。1 回目は debug build の gateway で封印 262 s・verify 18〜23 s となり、client 側で時計を比べた harness の誤りで 1 check が FAIL した（製品の check は全 PASS）。release build に変え、時計の比較を attempt の dispatch 時刻にした 2 回目は 18/18 PASS。証跡は最終 commit の上の run だけを残した。

## 却下した案

| 案 | 理由 |
|---|---|
| 最初の invoke で snapshot を自動作成する | 「いつ・どの revision の・何を」保存したかが operator に見えない。`require` の失敗が「snapshot 作成に失敗」と「互換でない」で混ざる |
| host 固有の値（CPU・KVM・kernel）は「近ければ可」とする | Firecracker 自身が同一 host・同一 version でしか load を保証しない（upstream）。部分一致を許す根拠が無い |
| artifact を ObjectStore（`[objects]`）に入れる | 1 object を memory に載せる API で、256 MiB 以上の memory file には向かない。tenant / region の境界は AAD で同じ性質を得た |
| 平文 cache の検証を作成時だけにする | 1 byte の改ざんを検出できない。restore の時間は増える（下の表の verify） |
| 別 revision の stale snapshot を自動で revoke する | alias を旧 revision に戻せば正しく使える snapshot を壊す |
| restore 後に secret を渡す | 渡す経路（frame・SDK API）が無く、snapshot に secret が残らないことの確認も要る。X1 では secret binding のある revision を対象外にした |
| jailer 無しでも clone する | drive の path が source の環境 directory の絶対 path で snapshot に入り、load で上書きできない（ADR-0015 §「Firecracker で分かったこと」2） |

## 再現

Lima VM（`docs/kvm.md` §5）で、通常ユーザーから:

```bash
scripts/x1/clone-e2e.sh    # build（target/x1-restore、release）→ sudo で gateway と試験 → docs/evidence/x1-clone-<UTC>/
```

exit 0 は全 check PASS。`summary.tsv` / `summary.json` / `invocations/*.json` / `clones.jsonl` / `clones-maps.txt` / `snapshot-files.txt` / `leftovers.txt` / `gateway.log` / `gateway.toml` / `versions.txt`。
