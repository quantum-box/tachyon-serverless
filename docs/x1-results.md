# X1 結果: Rust 関数の復元後の identity・接続・整合性と first response（PLT-4654）

実験・非ブロック。**P0〜P4 の受入とは別に判定する**（`docs/acceptance.md`「PLT-4654 (X1)」）。前提は ADR-0015（VMM 単体の保存復元）、ADR-0017（manifest・clone 経路、PLT-4653）、PLT-4651 の SDK lifecycle。

- 対象: `examples/restore-verify`（合成データだけ）、`scripts/x1/restore-verify.sh`（KVM 計測と検査）、`scripts/x1/restore-verify-report.sh`（集計。offline で再実行可）
- 証跡: `docs/evidence/x1-restore-verify-20260917T131439Z/`（本記録。検査 20 件 FAIL 0、exit 0）。途中で止まった 1 回目: `docs/evidence/x1-restore-verify-20260917T130328Z-aborted/`（§6）
- host: Apple M4 上の Lima VM（vz、**nested virtualization**、aarch64、Linux 7.0.0-31、4 vCPU / 8 GiB）、Firecracker / jailer v1.17.0、guest kernel 6.1.155、jailer + cgroup `required`、gateway release build を root、profile `dev`（`[snapshots] allow_unverified` の計測 run）。commit `dd45f2e`（rebase 前。`origin/main` `093a46f` に rebase した同内容の commit は `df27e61`。rebase で入った main の変更は usage journal / ledger・SQLite queue・failpoint・`examples/idempotent-async` で、restore の経路・provider・sample には触れていない。rebase 後の再計測はしていない）。**値は SLA ではなく、この 1 host の参考値**。物理 host（開発用 Mac）は他の作業と共用で、1 分 load average は p50 5.0 / 最大 43.5（`physical-host-load.tsv`）。
- **判定の範囲**: この sample・この host で「保存状態から正しく再開できるか」「通常起動より速いか」。一般的な任意の Rust アプリが restore-safe になるとは主張しない。

## 1. sample（`examples/restore-verify`）

| hook | すること |
|---|---|
| bootstrap（checkpoint 前） | `splitmix64(SEED ^ i)` の 64 MiB 表（`RESTORE_VERIFY_DATASET_MIB`）、その sha256（`e5aa88d0…1c94`、unit test で固定）、1 pass の digest chain（`RESTORE_VERIFY_PRECOMPUTE_PASSES`）。scratch の `rv-bootstrap.log` に 1 行追記し、process 内の実行回数を数える。source env id と時刻を含む marker 文字列を作る（grep の陽性対照） |
| after_restore | `getrandom` から instance id と session token、`getrandom` seed の userspace RNG、wall / monotonic / `/proc/uptime` の読み取りと 50 ms の Tokio timer、10 ms interval の常駐 task、loopback の「DB」server（`AUTH <token>` → session id）と認証済み session、**restore 後に rcgen で作る自己署名証明書**の loopback TLS server と rustls client の session（exporter を両側で比較）、scratch に `rv-instance-<instance id>` |
| handler | 表の lookup を生成式と照合、64 箇所の spot check、`{"verify":true}` なら表全体の sha256 を再計算。instance id・token・RNG・時計と timer の差・DB session（`WHOAMI`、切れていれば再接続して再認証）・TLS の PING/PONG・scratch の一覧と `rv-bootstrap.log`・guest boot id・generation を返す |

外部の DB / TLS への再接続は**試験していない**。clone は egress `none` だけ（PLT-4653 は他の egress の clone を拒否する）なので guest から外に出られない。loopback は「restore 後に作った接続が動き、copy ごとに別物であること」の代わりで、「snapshot 前に作った外部接続が restore 後にどうなるか」の代わりではない。

## 2. 検査（`checks.tsv`、すべて PASS）

| 検査 | 内容 | 結果 |
|---|---|---|
| restored-all | S1 から restore した 59 attempt（first 1・逐次 30・cache drop 20・同時 4×2）が HTTP 200・`start_kind = restored`・guest `restored = true` | 59/59 |
| fixed-data | 全 clone の checksum が 1 種類で、cold の値と一致。lookup と spot check が全件正しく、`verify = true` の 8 clone で全体の再計算も一致 | PASS |
| bootstrap-not-rerun | 全 clone で `bootstrap_env` = snapshot の source env、process 内の bootstrap 実行 1 回、`rv-bootstrap.log` は source の 1 行だけ、bootstrap 時刻 < snapshot 作成時刻。cold は自分の env で bootstrap（p50 3401 ms） | PASS |
| identity-diverges | 59 clone で環境 id・instance id・host の instance id・token・RNG の初値・DB session id・DB server nonce・TLS 証明書・TLS exporter がすべて 59 種類、generation 重複なし。guest boot id は全 clone で source と同じ（ADR-0015 の既知の事実） | PASS |
| clock-timer | guest の wall clock が client の要求時間窓の中（逸脱 0 ms、許容 1000 ms）。50 ms timer は monotonic 50〜57 ms、wall と monotonic の差 ≤ 1 ms、after_restore 以降の wall と monotonic の進みの差 0〜1 ms。`/proc/uptime` は restore 後も source の値から続く（11.8〜27.9 s）: monotonic は snapshot 時点から継続し、wall だけが restore frame で合わせられる | PASS |
| connections | 全 clone で loopback DB は自分の token で認証（server が見た token の sha256 が一致、handshake 1 回、再接続不要）、TLS 1.3（`TLS13_AES_256_GCM_SHA384`）の exporter が両側一致、PING/PONG 成功。DB 接続 13〜448 ms、TLS handshake 92〜2501 ms（最大値は host 負荷の山、§4） | PASS |
| scratch-isolation | 各 clone の scratch は `rv-bootstrap.log`（source の書込み）と自分の `rv-instance-<id>` だけ。自分の書込み前も後も他 clone の file は無い | PASS |
| auth-not-in-snapshot | 59 clone の instance id・token・session id・nonce・exporter（354 値）が S1 の平文 4 file（memory・vmstate・scratch・function drive）と封印 file・manifest・record のどれにも 0 件。陽性対照: bootstrap の marker は平文 memory に 1 件、`memory.sealed` に 0 件。S1 の平文 4 file の sha256 は 59 clone の前後で不変 | PASS |
| reconnect-series | 逐次 30 restore で `restore_reconnects = 1`・doorbell 1 回・再接続 130〜161 ms・失敗 0 | PASS |
| plaintext-cache-miss | S1 の平文を消して restore → 封印から復号・照合して restored（verify + 復号 2868〜3536 ms） | PASS（**修正後**。§5） |
| revoked | `POST .../revoke` → `revoked`、require は `Host.RestoreRequiredUnavailable`（`revoked`）で何も起動しない | PASS |
| corrupt-require | 平文 memory の 1 bit 反転 → `artifact_corrupted` で拒否、`quarantined` | PASS |
| corrupt-sealed | 平文を消し `memory.sealed` の 1 bit 反転 → AES-GCM で復号できず `artifact_corrupted`、`quarantined` | PASS |
| corrupt-prefer | 正常な prefer は restored。1 bit 反転後は HTTP 200・`start_kind = cold`・guest `restored = false`・`restore_fallback = artifact_corrupted`（restored に数えない） | PASS |
| revision-stale | active な snapshot があっても新 revision の deploy 後は `revision_mismatch` で拒否 | PASS |
| require-no-snapshot / warm / provider / cleanup | snapshot 無しの require は何も起動しない、warm 20/20、capability `unverified`、実行後に gateway・VMM・jailer・jail・cgroup・env dir・snapshot が 0（`leftovers.txt`） | PASS |

## 3. first response・memory・storage

client = curl の `time_total`（gateway 経由、handler の結果を検証、失敗を含む全 attempt の nearest-rank）。payload は `{"verify": false}`（lookup + spot check）、`restored-concurrent` だけ `{"verify": true}`（64 MiB の sha256 を handler で計算）。revision は 256 MiB・1000 m・scratch 64 MiB・egress none、pool 無効（warm だけ pool 有効の gateway）。

| scenario | n | 失敗 | client p50 / p95 / p99 ms | 内訳 p50（ms） | cgroup memory.peak p50 MiB | VMM RSS / Private_Dirty p50 MiB |
|---|---|---|---|---|---|---|
| cold（page cache hit） | 20 | 0 | **5969 / 9572 / 10546** | boot 2547、init 3223（うち guest bootstrap 2748、after_restore 167）、handler 130 | 107.2 | 104 / 103 |
| cold（`drop_caches` 直後） | 10 | 0 | 7474 / 15820 / 15820 | boot 2617、init 4633（bootstrap 4118） | 136.9 | 105 / 103 |
| warm（pool、250 ms 間隔） | 20 | 0 | **119 / 141 / 142** | resume 1、readiness 3、handler 99 | 107.7 | 105 / 105 |
| restored（逐次、page cache hit） | 30 | 0 | **1555 / 1674 / 1861** | **verify 702**、clone 開始 → Ready 477（load 22、doorbell 63、再接続 131、guest after_restore 203）、handler 137 | 10.2 | 38 / 6 |
| restored（`drop_caches` 直後 = cache miss） | 20 | 0 | 1659 / 7766 / 21095 | verify 747、Ready 489、after_restore 206 | 15.0 | 87 / 6 |
| restored（平文 cache なし = 封印から復号） | 5 | 0 | 3853 / 4297 / 4297 | verify + 復号 2982、Ready 422 | 15.2 | 44 / 6 |
| restored（snapshot 作成直後の 1 回目） | 6 | 0 | 1722 / 2573 / 2573 | verify 790、Ready 507 | 10.0 | 37 / 6 |
| restored（同じ snapshot の 2 回目） | 5 | 0 | 1554 / 4643 / 4643 | verify 701、Ready 487 | 10.0 | 39 / 6 |
| restored（4 同時 × 2 回、verify = true） | 8 | 0 | 7552 / 26936 / 26936 | 2 回目: verify 2372、boot 760〜857、handler 512〜555（全体 sha256 345〜363）。1 回目は 26.9 s（§4） | 9.9 | 99 / 7（PSS 30、Shared_Clean 93） |

storage と作成（`snapshots.jsonl`、9 回作成して失敗 0）:

| 項目 | 値 |
|---|---|
| snapshot 1 つの封印 file（AES-256-GCM + manifest + record） | 348,140,096 bytes（332 MiB） |
| 同じ snapshot の平文 cache（`_snapshots/<id>/`、memory 256 MiB + vmstate + scratch 64 MiB + function drive） | apparent 348,133,817 bytes、実割当 276,877,312 bytes（264 MiB、scratch は sparse） |
| **snapshot 1 つあたりの host disk** | 約 **596 MiB**（封印 332 + 平文 264）。clone 数によらず 1 つ。clone ごとの scratch copy は sparse copy（`scratch_drive_copy_ms` p50 0〜1 ms） |
| snapshot 作成（API、source の起動 + bootstrap + snapshot + 封印） | client 7.6〜17.5 s。`snapshot/create` 242〜2297 ms、paused 中の scratch copy 9〜150 ms、封印 2856〜5322 ms |
| verify の費用（毎 restore） | p50 702 ms（逐次）= restored client p50 の **45%**。4 同時では 2372 ms（4 つの verify が並ぶ）。封印からの復号は +2.3 s |
| gateway の RSS | phase A（restore と verify を繰り返した後）100.8 MiB（HWM 106 MiB）、phase B（pool に 1 環境）31.6 MiB（`host-samples.jsonl`） |

## 4. 読み方と注意

- **cold より速い**: restored の client p50 1555 ms は cold 5969 ms の 26%（p95 1674 vs 9572）。cold は guest kernel の起動（boot 2547 ms）と bootstrap（2748 ms）に時間の大半を使い、restored はどちらも払わない。代わりに verify 702 ms と clone → Ready 477 ms を払う。
- **warm より遅い**: warm の client p50 119 ms に対し restored は 13 倍。restore は warm pool の代わりにならない。restore が意味を持つのは「warm な環境が無い（scale to zero、pool の上限超え、別 revision への切替直後）」ときの cold の置き換えだけ。
- **host の memory**: 生きている環境 1 つあたり、cold / warm は VMM の Private_Dirty 103〜105 MiB（64 MiB の表を含む）、restored は Private_Dirty 6〜7 MiB で、表は snapshot memory file の page cache（4 同時で Shared_Clean 93 MiB を共有、PSS 30 MiB）として共有される。**cgroup の memory.peak は restored で 10〜15 MiB と小さく出るが、これは host 原価ではない**: 共有 page cache は最初にその page を読んだ cgroup（推定: verify で全体を読む gateway 側）に課金され、clone の cgroup には入らない。host 全体で見ると snapshot ごとに memory file 分の page cache（最大 256 MiB）と平文・封印の disk 596 MiB を持つ。
- **cache miss**: `drop_caches` 直後の restore は p50 で +104 ms（1659 vs 1555）、VMM RSS 87 MiB（file page を読み直す）。p95 / p99 の 7.8 s / 21.1 s は物理 host の負荷の山（13:20〜13:23 UTC、load average 17〜43）と時刻が一致し、cache miss そのものの値ではない。cold の cache miss も p50 +1.5 s で同じ時間帯の影響を含む。
- **snapshot 作成直後の 1 回目と 2 回目**: p50 1722 vs 1554 ms で差は小さい（n = 6 / 5）。作成時に平文 file を書いた直後なので page cache に載っている。2 回目の p95 4643 ms も負荷の山の時刻。
- **同時 4 clone**: 1 回目（13:21:06、load average 約 40）は 26.9 s、2 回目は 7.5 s。4 vCPU の VM で、4 つの verify（各 332 MiB の sha256）と 4 つの handler の全体 sha256 が並ぶ。逐次より大きく遅く、verify を restore ごと・clone ごとに行う設計がこの条件で効いている。
- **外れ値を捨てていない**。負荷の山は `physical-host-load.tsv` と `attempts.jsonl` の `started_ms` で照合できる。

## 5. 見つかった不具合と修正

- **平文 cache が無いときの restore が必ず失敗していた**（PLT-4653 の経路。ADR-0017 決定 2「無い file は sealed から復号して照合」）。snapshot service は復号した file を root 0600 で作り、clone は function drive をそのまま chroot に hard link するので、jail の検査が `function.ext4 (mode 600) is not world-readable` で拒否していた（`restore_required_unavailable: clone_failed`）。PLT-4653 の E2E は平文を消す経路を通っていなかった。修正: `crates/providers/firecracker/src/provider/restore.rs` の clone が link の前に 4 file の owner と mode を snapshot 作成時と同じ値（memory / vmstate は root:<jail gid> 0640、function drive は 0644、scratch は 0600）に揃える（`snapshot_file_access`、テスト `snapshot_files_are_readable_by_the_jail_but_never_writable_or_public`）。KVM で 5/5 restored を確認（`plaintext-cache-miss`）。
- 修正前の smoke run（記録は残していない）で確認した失敗の文言は上のとおり。

## 6. 除外した実行

- `x1-restore-verify-20260917T130328Z`（commit `4076afe`、最初の full run）: first / second の 2 周目で **snapshot の作成が失敗**した（`snapshot source checkpoint: timed out waiting for checkpoint`。source の VMM は起動したが 30 s 以内に bridge が接続しなかった。物理 host の load average が 8→11 に上がった時刻）。script がその後の空の snapshot id で止まった（exit 2）ため、script を「作成失敗を記録して 1 回だけ再試行し、以降の検査を止めない」に直して full run をやり直した。止まるまでの 14 検査は PASS、`cycle-2`（作成失敗）が FAIL（§5 の修正を含む commit なので平文 cache なしの restore も 5/5、verify + 復号 3.3〜14.5 s）で、値は本記録と同程度（client p50: cold 7783、restored 逐次 1460、cache miss 1703、4 同時 5946 ms）。表・checks・snapshot 作成記録を `docs/evidence/x1-restore-verify-20260917T130328Z-aborted/` に残した（raw の attempts と log は残していない。warm と後半の負の検査は実行されていない）。
- **snapshot 作成は host 負荷に弱い**（source の handshake を `[snapshots] handshake_timeout_ms` 30 s で打ち切る）。本記録では 9/9 成功したが、1 回目の run では 3 回中 1 回失敗した。

## 7. 判定

| 問い | 判定 | 根拠 |
|---|---|---|
| 保存状態から正しく再開できるか（この sample） | **成立** | 固定データ・bootstrap 非再実行・instance 固有状態の分岐・時計と timer・loopback DB / TLS・scratch 分離・per-clone 値が snapshot に無いこと・壊れた / 失効 / revision 違いの拒否がすべて PASS（59 clone） |
| cold より有益か（first response） | **改善**（この host） | client p50 5969 → 1555 ms（−74%）、p95 9572 → 1674 ms、失敗 0。verify 702 ms を含む |
| warm より有益か | **悪化** | 119 → 1555 ms。restore は warm の代わりにならない |
| page cache miss | 小さな悪化 | p50 +104 ms。tail は host 負荷と重なり判断できない |
| 平文 cache miss（封印から復号） | 悪化だが cold よりは速い | p50 3853 ms（cold 5969） |
| 同時 clone | **悪化**（逐次比） | 4 同時 p50 7.5〜26.9 s。verify が clone ごとに並ぶ |
| memory 原価 | 改善（生きている環境の private memory） | Private_Dirty 6 MiB vs 103 MiB。ただし snapshot ごとに共有 page cache 最大 256 MiB と disk 596 MiB を持ち、cgroup には課金されない |
| 外部 DB / TLS の再接続、secret の受け渡し | **未成立** | egress none の clone しか無く、restore 後に secret を渡す経路も無い |
| x86_64・bare metal・同一 CPU の別 host | **未成立** | 検証 host は 1 台（nested aarch64）だけ |

**損益分岐（推定、この host の 1 点からの外挿で未実測）**: restored の client ≈ 約 850 ms（clone・再接続・after_restore・handler・HTTP）+ verify（≈ 2.1 ms / MiB × snapshot の file 合計。本 sample は 332 MiB で 702 ms）。cold ≈ 約 3.2 s（boot 2.5 s + bootstrap 以外の init・handler・HTTP）+ bootstrap（本 sample は 64 MiB の表と 2 回の sha256 で 2.7 s、約 43 ms / MiB）。したがって**この nested host では bootstrap が 0 でも snapshot が約 1.1 GiB 以下なら restore の方が速い**。これは guest kernel の起動が 2.5 s かかる nested virtualization の性質に強く依存する。bare metal で cold boot が数百 ms なら、分岐は「bootstrap の時間 > verify + 約 0.6 s」になり、verify を毎回行う限り小さな初期化では restore は得にならない見込み（未測定）。

## 8. 追加設計が要るもの

1. **verify の費用**: restore ごと・clone ごとに snapshot 全体の sha256（p50 702 ms、4 同時で 2.4 s）。案: 平文 cache を root-only の read-only file system に置き fs-verity / dm-verity で page 単位に検証する、または「作成時と起動時に 1 回照合 → inode・mtime・ctime を記録 → immutable 属性」で以後は metadata の照合だけにする。どちらも改ざん検出の範囲が変わるので threat model（`docs/threat-model.md` §14-19）の更新が要る。
2. **memory の課金**: snapshot の page cache が clone の cgroup に入らない。snapshot ごとの cgroup で page cache を持つ（最初の読み込みをその cgroup で行う）か、admission が snapshot ごとの常駐分を別に予約する。
3. **外部接続と secret**: egress `restricted` / `public-web` の clone（NIC の付け直し、ADR-0015 決定 3）と、`Restore` frame での secret 受け渡し（protocol の version を上げる）。これが無い限り「DB / TLS の再接続」は loopback の代替でしか確かめられない。
4. **snapshot 作成の頑健性**: source の handshake が host 負荷で 30 s を超えて失敗した。作成の再試行・source 起動の admission 予約・handshake budget を bootstrap の実測に合わせる設定。
5. **同時 restore**: 同じ snapshot の verify を 1 回にまとめる（single-flight）。
6. **別 host での再現**: x86_64 / bare metal / 同じ CPU の 2 台目で同じ script を通す（`scripts/x1/restore-verify.sh`、所要約 11 分 + build）。

## 9. 再現

Lima VM（`docs/kvm.md` §5）で `sudo chmod 666 /dev/kvm` の後、通常ユーザーから:

```bash
scripts/x1/restore-verify.sh                         # build → sudo で計測 → docs/evidence/x1-restore-verify-<UTC>/
scripts/x1/restore-verify-report.sh docs/evidence/x1-restore-verify-<UTC>   # 表だけ作り直す（jq のみ）
```

exit 0 = 全検査 PASS、1 = 検査失敗、2 = 前提 / 準備の失敗、3 = 残留物あり。git checkout でない tree では `RV_COMMIT=<sha>`、物理 host のメモは `RV_HOST_NOTE`。件数などは script 先頭の `RV_*`。物理 host の load average は外側で `physical-host-load.tsv` に取り、evidence に置いてから report を再実行する。

evidence のファイル: `checks.tsv`（検査）、`attempts.jsonl`（1 request 1 行、handler の出力と `restore_*` の内訳を含む）、`invocations.jsonl`、`resources.jsonl`（0.5 s ごとの環境 cgroup と VMM の smaps_rollup）、`teardown-stats.jsonl`（環境ごとの cgroup 統計）、`snapshots.jsonl`（作成と storage）、`snapshot-grep.txt`、`host-samples.jsonl`、`calibration.jsonl`、`physical-host-load.tsv`、`versions.txt` と `profile/`（host・version・commit・kernel / rootfs / binary / config の sha256）、`gateway-{cold,warm}.toml`、`gateway-{a,b}.log`、`leftovers.txt`、`summary.{md,json}`。
