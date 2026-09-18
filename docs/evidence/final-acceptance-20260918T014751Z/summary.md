# PLT-4649 プロトタイプ最終受入 — 2026-09-18

判定と受入条件ごとの対応は [`docs/acceptance.md`](../../acceptance.md) の「PLT-4649」節。ここは実行した順の記録と、この directory の読み方。

## 環境

| 項目 | 値 |
|---|---|
| host | Apple M4（macOS、Darwin 25.6.0、**共有の開発機**）上の Lima VM `tsls-kvm`（vmType vz + nested virtualization、aarch64、4 vCPU / 8 GiB、Linux 7.0.0-31-generic） |
| provider | Firecracker / jailer v1.17.0、cgroup v2 `required`、gateway は root（`profile = "production"`）。故障マトリクスの process provider は一般ユーザー |
| guest kernel | lab: 6.1.155（`deploy/lab/versions.lock`）、`scripts/kvm/*`: 6.18.48（`.kvm` の firecracker-ci 資産） |
| 版 | nats-server v2.14.7、rustc 1.95.0 |
| repository | 作業ツリーではなく VM の `~/lab` への clean clone。commit は実行ごとに下表 |
| 物理 host の負荷 | `mac-load.tsv`（5 秒ごと、1834 sample）。1 分 load average は窓ごとに大きく違う: lab・隔離・warm（02:10〜03:00）p50 69.2 / max 194.3、benchmark（03:06〜03:49）p50 8.3 / max 58.8、故障マトリクス（03:50〜04:50）p50 2.1 / max 14.4 |

## 実行（順に）

| # | 実行 | commit | 結果 | 出力 |
|---|---|---|---|---|
| 1 | lab（firecracker）1 回目 | `eb21e14` | demo all で 7 失敗（restart phase の cron がバックログを作り P3 の async が待たされた） | `runs/lab-run1/` |
| 2 | lab（firecracker）2 回目 = 本番 | `4afd8c1` | preflight → bootstrap → up → status → **demo all 64/64** → status → teardown `orphan check: clean` | `runs/lab-run2/` |
| 3 | `scripts/e2e/demo.sh`（firecracker） | `4afd8c1` | 29/29 PASS | `runs/e2e-demo/`、`../20260918T021321Z-firecracker/` |
| 4 | `scripts/kvm/measure-isolation.sh` 1 回目 | `4afd8c1` | exit 5（NOISY 判定の jq が落ちた。検査そのものは基準内） | `runs/isolation-run1/`、`../isolation-20260918T021513Z/` |
| 5 | `scripts/e2e/zero-scale.sh`（firecracker）1 回目 | `4afd8c1` | 17/18（`2-burst-coalesced` は `promised` の二重計上） | `runs/zero-scale-run1/`、`../20260918T022153Z-zero-scale-firecracker/` |
| 6 | `scripts/kvm/measure-warm.sh` | `4afd8c1` | 9/9 step、6 回中 5 回 warm（resume 中央値 3 ms、cold total 19 114 ms） | `runs/warm/`、`../warm-20260918T022403Z/` |
| 7 | `usage-e2e.sh` / `budget-e2e.sh`（firecracker） | `4afd8c1` | usage 18/18、budget 19/19 | `runs/usage/` |
| 8 | `scripts/kvm/bench.sh` 1 回目 | `4afd8c1` | 拒否（release binary 不足と calibration 254 ms > 150 ms） | `runs/bench/`、`../bench-20260918T022840Z/` |
| 9 | `measure-isolation.sh` 2 回目 | `bbd650e` | 21/21 step、M8 / M9 / DISK / NET / HOST / NOISY すべて PASS | `runs/isolation/`、`../isolation-20260918T024419Z/` |
| 10 | `zero-scale.sh` 2 回目 | `bbd650e` | 18/18（`max provisioned=3`） | `runs/zero-scale/`、`../20260918T024902Z-zero-scale-firecracker/` |
| 11 | `async-dispatch-e2e.sh` / `triggers-e2e.sh`（firecracker） | `bbd650e` | dispatch 40 ok、triggers 39 ok | `runs/queue/` |
| 12 | `chaos/matrix.sh --only`（firecracker 8 シナリオ） | `bbd650e` | 6 pass、1 FLAKY、`worker_user_process_kill_sync` は firecracker 非対象と判明 | `runs/chaos-firecracker/` |
| 13 | `chaos/matrix.sh --only worker_user_process_oom_sync`（firecracker） | `b29eeec` | pass | `runs/chaos-firecracker-oom/` |
| 14 | `chaos/matrix.sh`（process、root で実行） | `b29eeec` | 19 pass、`object_store_unavailable` は root では故障が入らず 3 回とも失敗 | `runs/chaos-process-root/` |
| 15 | `chaos/matrix.sh --only object_store_unavailable`（process、一般ユーザー） | `b29eeec` | pass | `runs/chaos-process-objectstore-retry/` |
| 16 | `chaos/matrix.sh`（process、一般ユーザー、全 21 シナリオ） | `af0d9ab` | 20 pass、1 FLAKY（`stale_owner_frozen_in_transaction` は 2 回目で pass）、exit 0 | `runs/chaos-process/` |
| 17 | `scripts/kvm/bench.sh` 2 回目 | `bbd650e` | 41/41 step、714 request、失敗 30（pool を切った並列 8 の queue timeout 504 のみ） | `runs/bench/`、`../bench-20260918T030637Z/` |

## この directory の読み方

- `runs/<名前>/stdout.txt` — その実行の全出力。`host.txt` は実行前後の VM の負荷・memory・disk と、実行後の残留監査（process / cgroup / jail / tap / nft / netns）。`vm-load.tsv` は 5 秒ごとの VM の load average。
- `runs/lab-run1`・`runs/lab-run2` — lab の各コマンドの出力（`01-preflight.txt` … `07-teardown.txt`、`09-host-after-teardown.txt`）と `demo/` 以下の `results.txt`。
- `runs/chaos-*/matrix/` — `summary.md`（シナリオ表）、`results.jsonl`、`profile.json`、`scenarios/<id>/attempt-<n>/`。
- `scripts/` — この受入で使った VM 側の実行 script の写し。CI の shellcheck が拾わないよう `*.sh.txt` にしてある（`docs/evidence/kvm-final-scripts/` と同じ扱い）。
- `mac-load.tsv` — 物理 host（Mac）の 1 分 load average。時間の値を読むときの前提。
- 各 script が書いた evidence は `docs/evidence/` の元の名前のまま置いてある（上表の「出力」列の `../`）。

## 注意

- 単一 host・aarch64・nested virtualization での記録。複数 host の HA、x86_64、bare metal、電源断・disk 喪失は含まない。
- 時間の値は共有 host の負荷に強く影響される。性能の判定は `../bench-20260918T030637Z/` を使う。
- 実請求・公開・本番移行・購入は行っていない。
