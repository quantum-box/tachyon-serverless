# ADR-0011: 再利用とスケールの観測は、operator 専用の `GET /metrics` と状態機械から読む gauge・発生箇所で数える counter で行い、検出器と負荷ハーネスで回帰にする

## ステータス

Accepted（2026-09-17、PLT-4637）。実装: `crates/application/src/metrics/`（`catalog.rs` が family の一覧、`render.rs` が exposition、`mod.rs` が event の registry・boot identity・idle CPU）、`crates/application/src/services/admission/state.rs`（`AdmissionCounters`、`AdmissionState::metrics`）、`crates/application/src/services/invoke.rs`（attempt・gate 拒否の hook）、`crates/application/src/app.rs`（`render_metrics`、`reuse_report`、heartbeat の計数）、`crates/provider-port`（`ExecutionProvider::environment_stats`）、各 provider、`apps/gateway`（`GET /metrics`、`GET /v1/capacity` の `reuse`）、`deploy/prometheus/alerts.yml`、`apps/load`（`tsls-load`）、`scripts/load/scenarios.sh`。catalog と使い方は `docs/metrics.md`。

## コンテキスト

- PLT-4632〜4636 で pool・warm 再利用・admission・zero-scale・設定 cache を実装した。観測の手段は `GET /v1/capacity`（呼び出し元 tenant の分だけの snapshot）と log だけで、時系列も、tenant をまたぐ全体像も、「再利用が本当に起きたか」の証跡も無かった。
- PLT-4637 の受入条件: 0 → 負荷増 → 上限 → 減少 → 0 → 再起動をグラフ / 機械可読結果で示す。同一環境の再利用を boot ID で確認し、未対応 profile は毎回起動と表示する。起動予約超過・starvation・idle 中の想定外 resource 利用を検出する。宣言した検証負荷上限を越えず、本番 / 外部 host へ負荷を送らない。固定 profile・commit・seed を記録した再現試験。数値は性能実績で SLA ではない。
- 前提（ADR-0006 と同じ）: 1 gateway = 1 node。状態はプロセスのメモリ。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | `prometheus` / `metrics` crate の registry に、各所で gauge を inc / dec する | 不採用。admission の状態は 1 つの lock の下の状態機械にあり、別の場所で gauge を増減すると 2 つ目の台帳になって drift する（ADR-0006 決定 2 の「全 counter を予約の集合から再計算して比べる」性質を失う）。約 60 family のために依存を増やす利点も小さい |
| B | 状態は scrape 時に状態機械・pool・cache から読み（gauge）、状態機械に無い event だけを発生箇所で数える（counter / histogram）。exposition は手書き（本 ADR） | 採用 |
| C | OpenTelemetry で push する | 不採用。collector を置く前提になり、prototype の 1 host 構成に合わない。exposition を後から OTLP に変換する余地は残る |
| D | `GET /v1/capacity` を拡張して全 tenant を返す | 不採用。tenant の token で他 tenant の id・待ちが見える（T25 の方針に反する） |

## 決定

1. **`GET /metrics` は operator 専用の credential で守る。** `[metrics] bearer_token`（16 bytes 以上、定数時間比較、tenant の token や `internal_token` と同じ値は設定検証で拒否）。未設定なら 404。tenant の token は operator role でも 401（operator role は自 tenant に閉じるので、全 tenant を含む metrics の権限にはならない）。別 listener にはせず、gateway の listen を共有する（`[control_plane] internal_token` の `GET /v1/internal/config` と同じ形）。
2. **tenant / revision を識別する label は、この endpoint にだけ、上限付きで出す。** `[metrics] max_revision_series`（64）・`max_tenant_series`（32）・`max_environment_series`（128）を超えた分は `_other` に合算し、畳んだ数を `tsls_metrics_series_truncated` で出す。`GET /v1/capacity` には tenant をまたぐ情報を足さない（足すのは node 全体の `reuse` だけ）。
3. **gauge は状態機械から読む。** `AdmissionState::metrics(now)` が node・revision・tenant の状態別環境数、予約、queue、待ち時間、breaker を 1 回の lock で返す。状態機械に monotonic な `AdmissionCounters`（到着、grant の種類、合流で待った数と起動を省いた数、起動結果、breaker の open、scale event の kind / reason、tenant ごとの grant）を足した。revision の entry は 0 で忘れられるので、counter は revision に置かない。
4. **event は発生箇所で数える。** attempt の終了（start kind・status・`AttemptTimings` の phase histogram。warm の boot / init の 0 は histogram に入れない）、invoke gate の拒否（`error_type`）、dispatcher の heartbeat の結果。`Metrics` は admission controller が持ち、invoke・pool・gateway はそこから使う（依存を増やさない）。
5. **boot identity で再利用を確認する。** attempt ごとに環境の `guest_boot_id` を、その環境で最初に見た値と比べる（`first_boot` / `same_boot` / `boot_changed` / `unreported`）。`boot_changed` は 0 のままであるべき不変条件で、ERROR log と alert にする。provider に warm の段階が無い（process、pool 無効、idle capability が Supported でない）構成は `every_invocation_boots` と `tsls_environment_reuse_mode` と `GET /v1/capacity` の `reuse.mode` で表示する。
6. **host の使用量は provider の読み取り専用 port で取る。** `ExecutionProvider::environment_stats`（既定は `None` = 取れない）。Firecracker は VMM の cgroup v2、process は bridge プロセスの procfs / `proc_pid_rusage`、fake は設定値。provider の挙動は変えない。idle CPU は scrape ごとの sample の差分（前回も今回も idle の環境だけ）で出す。測定窓は scrape 間隔。
7. **検出器は 2 か所に同じ条件で置く。** Prometheus の alert rule（`deploy/prometheus/alerts.yml`）と、負荷ハーネスが記録した sample に対する Rust の検出器（`apps/load/src/detect.rs`）。starvation は「待っている tenant が何も in flight に持たず、待ちの間に grant も無いのに、他 tenant が grant された」とする（自分の quota や fair share で待つのは starvation ではない。最初の mixed 実行で quota に当たった長時間 tenant を誤検出したため、この定義にした）。
8. **負荷ハーネスは上限を宣言し、守り、送り先を限定する。** 同時数・件数・期間を宣言し（compile 時の天井 64 / 2000 / 1800 s）、計画が超えれば 1 件も送らない。送り先は loopback か明示した lab host だけ（名前解決しない、redirect を追わない）。seed・commit・設定（token は伏せる）・上限を `run.json` に残し、`summary.json` に観測した同時数・件数・時間と `respected` を書く。`scripts/e2e/` に置くと KVM gate を要求するので `scripts/load/` に置く。

## 結果（consequences）

- 既存の設定はそのまま動く（`[metrics]` 省略時は `/metrics` が 404）。`GET /v1/capacity` に `reuse` が増え、OpenAPI snapshot が変わった。
- scrape は live 環境ごとに provider を 1 回読む（最大 1024）。Firecracker では cgroup の file 読み取り 3 つ。scrape 間隔を極端に短くすると idle CPU の窓も短くなり、ratio の誤差が増える。
- counter はプロセスのメモリにだけあり、gateway の再起動で 0 に戻る（Prometheus の `rate()` / `increase()` は reset を扱う）。boot identity の比較は再起動を跨がない。
- metrics は tenant を識別する label を含むので、`bearer_token` の漏洩は全 tenant の id・revision id・待ち時間の漏洩になる（T34）。
- process provider の CPU / memory は bridge プロセスだけ（user の handler を含まない下限値）。
- 数値は観測値で、SLA・SLO ではない。alert の閾値（starvation 5 s、idle CPU 0.05）は出発点。

## 検証

- fake clock: `services::admission::tests::{metrics_follow_admission_state_transitions_on_a_fake_clock, metrics_count_coalesced_starts_and_breaker_opens}`。
- registry と exposition: `metrics::tests::*`（histogram、boot identity、idle CPU の差分、exposition の構文・family の連続・catalog との一致、cardinality の上限、label の escape、catalog の全 family が `docs/metrics.md` にあること）。
- pipeline（fake provider、warm pool）: `crates/application/tests/scaling.rs::metrics_show_zero_to_cap_to_zero_and_boot_identity_proves_warm_reuse`（0 → burst → cap 2 → idle → 0、`same_boot` = warm attempt 数、`boot_changed` 0、idle 環境の CPU を検出）。
- gateway: `apps/gateway/tests/gateway_integration.rs::{metrics_require_the_operator_credential_and_never_leak_tenants_without_it, metrics_scrape_reflects_a_scripted_invoke_sequence}`。
- 検出器と上限: `apps/load` の unit test、`apps/load/tests/alerts.rs`（YAML の parse、catalog にある family だけを参照）。
- 実 gateway（process provider、macOS arm64 1 host）: `scripts/load/scenarios.sh` の 6 シナリオ（`docs/evidence/load-*-20260917T07*-process/`）。
- **Firecracker 上の負荷シナリオ・`environment_stats`（cgroup）・warm pool での boot identity は未検証**（検証 VM を benchmark 作業が使用中のため実行していない）。promtool による rule の検証は未実施（環境に無い）。
