# docs 索引

Tachyon Serverless プロトタイプ（Linear P0〜P1、PLT-4613〜PLT-4630）の文書一覧。読む順は上から。

## 契約（先に読む）

| 文書 | 内容 |
|---|---|
| [architecture.md](architecture.md) | crate 構成、依存方向、同期 Invoke の流れ、設定、実装者が守る決め事、P1 の非対象 |
| [protocol.md](protocol.md) | host ↔ bridge の frame、bridge ↔ user process の Runtime API、Firecracker guest 規約（rootfs、drive、cmdline、API 順序、terminate） |
| [threat-model.md](threat-model.md) | 信頼境界、資産、前提、guest 自己申告を根拠にしない原則、2 tenant + operator のアクセス制御マトリクス、deadline / OutcomeUnknown / Idempotency-Key / 上限の意味、process provider が守らないもの、非目標 |

正本のコード: `crates/domain`（ID、entity、状態遷移、`ErrorClass`、`Limits`）、`crates/protocol`（frame、Runtime API）、`crates/provider-port`（`ExecutionProvider` ほか port）、`crates/api-types`（DTO、`ErrorCode`）。文書とコードが食い違ったらコードを正とし、文書を直す。

## 決定（ADR）

| 文書 | 内容 |
|---|---|
| [adr/0001-execution-provider-firecracker-first.md](adr/0001-execution-provider-firecracker-first.md) | Firecracker / Cloud Hypervisor / Kata の比較、Firecracker 第一・trait の背後・CH fallback・Kata は後続 adapter、残る測定 M1〜M13、fake の結果で決めない受入規則 |
| [adr/0002-process-provider-dev-only.md](adr/0002-process-provider-dev-only.md) | 隔離なし process provider の存在理由、`profile = "production"` での拒否、証明するもの / しないもの |
| [adr/0003-execution-state-persistence.md](adr/0003-execution-state-persistence.md) | 実行状態の永続化。P1 の in-memory + `state.json` が P2 の複数プロセス調整に足りない理由、選択肢の比較、決定と移行手順、埋め込み SQLite（`state.db`）の実装メモと TiDB で変わる点（Accepted） |
| [adr/0004-public-ingress.md](adr/0004-public-ingress.md) | 安定した public ingress。loopback listen・KVM 必須・egress none という既存の制約、named tunnel / 自前 VM + reverse proxy / Cloud App front door / overlay funnel / ephemeral tunnel の比較、推奨と owner に要る供給物、公開してよい面（invoke）と出さない面（管理 API・`/readyz`）の hostname 分離、demo より長く上げる前に要るもの |

## 棚卸し・受入

| 文書 | 内容 |
|---|---|
| [inventory-tachyon-apps.md](inventory-tachyon-apps.md) | tachyon-apps（commit `ae727f1f7`）の棚卸し。領域ごとの reference / adapter candidate / 不要、port への対応、能力表（すべて unverified）、baseline KVM 測定プロファイル |
| [acceptance.md](acceptance.md) | PLT-4613〜PLT-4630 の受入条件ごとの状態（実装済み / 実装済み・KVM実測あり / 未検証 / 未着手 / 対応中）と証跡（テスト名、スクリプト、evidence）、ADR-0001 の残る測定 M1〜M13 の状況、2026-09-16 のレビュー指摘 |

## 運用・利用

| 文書 | 内容 | 担当 |
|---|---|---|
| [kvm.md](kvm.md) | KVM 検証 host の要件、preflight / bootstrap / smoke / teardown、gateway での利用、証跡の読み方、macOS での Lima + nested virtualization 手順（確認済み）と実測値、失敗時の切り分け | PLT-4615 / PLT-4621 |
| [api.md](api.md) | 管理 API と Invoke API の使い方、認証 header、エラー応答、OpenAPI の場所 | PLT-4619 / PLT-4626 |
| [cli.md](cli.md) | `tsls deploy / invoke / logs / rollback / dev` の使い方と設定 | PLT-4629 |

## 実行記録（evidence）

`scripts/e2e/demo.sh` は `evidence/<UTC>-<provider>/`、`scripts/kvm/smoke.sh` は `evidence/kvm-<UTC>/` に書く。smoke の読み方は [kvm.md](kvm.md) §4、E2E の中身はリポジトリの [README](../README.md)「E2E デモ」。

| ディレクトリ | 内容 | 結果 |
|---|---|---|
| [evidence/20260915T073238Z-process/](evidence/20260915T073238Z-process/) | E2E デモ、process provider（macOS arm64、隔離なし）。`summary.json`、`provider.json`、`invocations.json`、`gateway.log`、`steps/` | 27/27 PASS |
| [evidence/kvm-20260915T080221Z/](evidence/kvm-20260915T080221Z/) | fc-smoke、Firecracker v1.17.0 microVM（aarch64、Apple M4 上の Lima VM、nested virtualization）。`summary.txt`、`hello.json`、`timeout.json`、guest console、Firecracker log | hello 応答、timeout demo、残骸なし |
| [evidence/20260915T125610Z-firecracker/](evidence/20260915T125610Z-firecracker/) | E2E デモ、Firecracker provider（上と同じ VM、`config/gateway.firecracker.toml`） | 27/27 PASS |
| [evidence/20260915T171415Z-process/](evidence/20260915T171415Z-process/) | E2E デモ、process provider、レビュー指摘修正の統合後（secret 値の検査ステップを含む） | 28/28 PASS |
| [evidence/20260915T171631Z-firecracker/](evidence/20260915T171631Z-firecracker/) | E2E デモ、Firecracker provider、レビュー指摘修正の統合後 | 28/28 PASS |

記録の時間は nested virtualization 上の参考値で、SLA ではない。process provider の記録は microVM の証跡として使わない。
