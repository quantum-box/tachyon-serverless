# ADR-0009: 環境のゼロスケール・min_ready・cooldown と、Revision 切替 / Secret 世代 / 削除の drain を 1 つの scale reconciler と admission の判定で行う

## ステータス

Accepted（2026-09-17、PLT-4635）。実装: `crates/application/src/services/scaling.rs`（`ScaleController`、`[scaling]`）、`crates/application/src/services/admission/state.rs`（`try_scale_down`・`try_prestart`・`begin_drain` / `end_drain`・scale event）、`crates/application/src/services/pool.rs`（sweep の判定の委譲、drain 中 revision と古い reuse key の pool 入り拒否、`release_for`）、`crates/application/src/services/invoke.rs`（受付時の route generation、dispatch 直前の削除確認、`CancelKind::Drain`、`prestart`）、`crates/application/src/control/cache.rs`（`scale_view`、`function_deleted`）、`crates/domain`（`ExecutionPolicy` の scale policy、`Function::drained_at`、`Invocation::alias_generation`）、`apps/gateway`（reconcile loop、`GET /v1/capacity` の `scaling` と revision ごとの scale 欄）、`apps/cli`（`functions deploy --min-ready / --idle-ttl-seconds / --scale-down-cooldown-seconds`、`tsls capacity`）。E2E: `scripts/e2e/zero-scale.sh`。

## コンテキスト

- PLT-4634（ADR-0006）までで、環境は invocation の driver が admission の grant を得て起動し、pool（PLT-4632/4633、既定 off）の idle 環境は `[pool] idle_ttl_seconds` の sweeper が一律に終わらせていた。`min_ready` は 0 に固定（revision 検証で拒否）、先行起動はしない、と ADR-0006 決定 5 が PLT-4635 に残した。
- 足りなかったもの:
  - **sweeper が admission を見ていない。** 待機中の invocation がある revision、warm の約束（`Promised`）が付いた idle 環境、起動直後（活性化直後）の環境も TTL だけで消せた。claim との race は台帳の CAS（`take_idle_for_termination` / `claim_for_reuse`）で「負けた側が cold に落ちる」ことまでは保証されていたが、約束済みの環境を消して cold start を 1 回余計に払う経路が残っていた。
  - **hysteresis が無い。** 活性化の直後でも TTL を過ぎれば消え、次の到着で再起動する（振動）。
  - **Revision 切替・Secret 世代変更・削除で環境を片付けない。** alias を移しても旧 revision の idle 環境は TTL まで残り、実行中の環境は終了後に再び pool に入った。secret を rotate した後の旧世代の idle 環境は二度と claim されないのに TTL まで memory を持った。関数を削除しても、削除より前に受け付けて待機中だった invocation はそのまま起動した（新規と `Idempotency-Key` の再送は受付前の `resolve` が 409 で拒否していた）。削除が「終わった」ことを示す状態も無かった。
- PLT-4635 の受入条件: `minReady = 0` で無負荷後に環境数 0、次の Invoke が cold 起動で成功。未処理 queue / Busy 環境を idle 判定で破棄しない。Alias 切替後の新規受付だけ新 Revision、既存実行をすり替えない。通信再開 / タイマー競合で環境数が振動せず、削除後の新規受付を拒否する。
- 前提（ADR-0006 と同じ）: 1 gateway = 1 node、環境 1 つに invocation 1 つ、pool は provider の `idle_quiesce` / `idle_resume` が `Supported` かつ `[pool] enabled = true` のときだけ動く（process provider は `Unsupported`、firecracker は計測済みの `Supported`）。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | sweeper・min_ready・drain をそれぞれ別の timer（sweeper は pool、min_ready は admission の ticker、drain は alias / function の書き込み hook）で持つ | 不採用。3 つの timer が同じ環境について別々に判断し、「約束済み・cooldown 中・min_ready に必要」を 1 か所で判定できない。書き込み hook は同じプロセスの管理 API にしか効かず、data plane（PLT-4636）は alias の変更を配信でしか知らない |
| B | 1 つの reconciler（`ScaleController`）が周期的に「route の観測 → drain の開始 / 終了 → drain timeout → idle sweep → min_ready の先行起動 → 削除の確定」を行い、個々の環境を消してよいか・先行起動してよいかは admission の lock の下の同期判定（`try_scale_down` / `try_prestart`）で決める（本 ADR） | 採用 |
| C | Kubernetes の HPA のように desired を別ループが計算し、足りなければ所有者の無い起動、多ければ停止する | 不採用（ADR-0006 の案 C と同じ理由）。起動は所有者（待機中の invocation）がする。例外は `min_ready` の分だけで、それも admission の予約を通す |
| D | 削除を hard delete（行を消す）にする | 不採用。実行中の invocation・環境・`Idempotency-Key` が関数を参照している。`deleted_at`（受付停止）と `drained_at`（何も残っていない）の 2 段にする |

## 決定

1. **scale policy は revision の `ExecutionPolicy` に置く。** `min_ready`（既定 0、`0..=16` かつ `max_concurrency` 以下）、`idle_ttl_seconds`（省略時 `[pool] idle_ttl_seconds`、`1..=86400`）、`scale_down_cooldown_seconds`（省略時 `[scaling] scale_down_cooldown_seconds`（既定 30）、`0..=3600`）。環境数の上限は既存の `max_concurrency`（`concurrency_per_environment = 1` なので環境数の上限と同じ。別の `max_environments` は作らない）。新しい 2 つは未設定なら serialize しないので、既存 revision の spec digest と reuse key は変わらない。

2. **idle 環境を消してよいかは admission が決める（`AdmissionState::try_scale_down`）。** pool の sweeper は `Idle` の行を列挙し、その環境の予約（`Grant`）に「消してよいか」を問う。admission は 1 つの lock の下で次を確認し、よければ**同じ操作で**予約を `Draining` に移す（以後その環境は warm の約束に使われない）:
   - revision に待機中の invocation が無い（`Queued`）。
   - revision の idle 環境がすべて約束済みではない（`Idle > Promised`、`Promised`）。
   - 予約が `Idle`（`Busy`・`Starting`・`Parking` は対象外。sweeper はそもそも `Idle` の行しか見ない）。
   - drain 中でない revision では、さらに: idle になってから `idle_ttl` 以上（`IdleTtl`）、最後の cold start（活性化・scale-up・先行起動）から `cooldown` 以上（`Cooldown`）、alias が route している revision なら残りが `min_ready` 以上（`MinReady`）。
   
   その後で pool が台帳の CAS（`take_idle_for_termination`、`Idle` の行だけを `Draining` にする）を取り、勝ったら terminate する。**CAS に負けた場合**は claim が先に行を取っており、その claimer が `adopt` で予約を `Busy` に戻す（sweeper は何もしない）。**sweeper が先に決めた場合**、後から来た invocation はその環境を約束されず、合流（ADR-0006 決定 5）に従って cold start する。どちらの順でも「使用中の環境の terminate」「約束済み環境の消滅」は起きない（`admission::tests::an_arrival_between_the_sweep_decision_and_the_terminate_aborts_the_scale_down`、`tests/scaling.rs::sweeps_racing_invocations_never_terminate_an_environment_in_use`）。pool を admission 無しで作った場合（単体テスト）は従来どおり TTL だけで判断する。

3. **scale-to-zero は `min_ready = 0` の帰結であり、特別な経路を持たない。** 上の判定で最後の idle 環境が消えると event `scale_to_zero` を記録する。**環境数 0 は host 費用 0 ではない**: gateway プロセス、`state.db`、node（Firecracker の場合は KVM host）は動き続け、`GET /v1/capacity` の `scaling.at_zero` もそう述べる。pool が無い構成（process provider、`[pool] enabled = false`）では環境は invocation と一緒に終わるので、無負荷なら常に 0 である（idle の段階が無い）。

4. **0 からの活性化は ADR-0006 の合流に従う。** 0 の revision への N 件の burst は `desired`（backlog を含む）を上限に cold start し、`max_concurrency`・tenant quota・node の資源・start rate の範囲でしか起動しない。残りは公平 queue で待ち、起動した環境が pool に戻ればそれを warm で使う（`Parking` 中の環境は待つ）。**遅延の目安**: 0 からの最初の応答は cold start（boot + init）ぶん遅い。process provider で数十〜数百 ms、Firecracker（aarch64 nested virtualization）では `environment_boot_ms` 3.5〜4.8 s・`runtime_init_ms` 0.3〜0.4 s の実測がある（`docs/acceptance.md` M2/M3、参考値）。burst の後続は起動済み環境の空きを待つので、最悪で「cold start + 先行する handler の時間 × 待ち順」になり、`queue_timeout_seconds` を超えれば 504。cold start を避けたい revision は `min_ready` を使う。cooldown は活性化の直後に消して再起動する往復を防ぐ。

5. **`min_ready` は reconciler の先行起動で満たす（`AdmissionState::try_prestart`、`InvokeService::prestart`）。** 対象は alias が route していて drain 中でない revision。provisioned（`Starting + Busy + Parking + Idle`）が `min_ready` 未満のとき、1 回の reconcile で不足分だけ予約する。予約は:
   - **待機中の invocation が 1 件でもあれば行わない**（`WaitersFirst`。先行起動が invocation より先に容量を取らない）。
   - node の資源と `max_concurrency`、tenant quota、revision の `max_concurrency`、start rate、breaker（closed のときだけ。probe には使わない）をすべて満たすときだけ（`Blocked`）。queue には入らず、容量不足で idle の追い出しも頼まない。
   - 予約は通常の cold start と同じ `Starting` で、capacity と quota に数える。起動した環境は `Ready`（epoch 0）のまま pool に渡し、台帳は「一度も割り当てられていない `Ready`（epoch 0）」に限って `Idle` にする（`release_to_pool`、`repository::contract_tests::*::a_never_assigned_ready_environment_can_be_pre_started_into_the_pool`）。
   - 起動・usage（`EnvironmentStarted` / `EnvironmentStopped`、`invocation_id` なし）・ログは実際の環境として記録する。先行起動した環境は invocation が無くても費用がかかる。
   - pool の per-key 上限は `max(max_idle_per_revision, min_ready)` に引き上げ、`max_total_idle` に達していれば起動しない（起動 → pool が拒否 → terminate → 再起動の往復を作らない）。起動に失敗したら `[scaling] prestart_backoff_seconds` 待つ。
   - `min_ready` が満たされた後は、sweeper が `MinReady` で残すので作り直しは起きない（`tests/scaling.rs::min_ready_pre_starts_into_the_pool_and_keeps_them_without_flapping`）。全体として provisioned は `max(min_ready, desired)` に収束する（先行起動は `min_ready` まで、それ以上は所有者のいる起動）。
   - **pool が無い構成では `min_ready` は満たされない**（idle で置いておけないため）。revision の検証では拒否せず（data plane ごとに pool の有無が違いうる）、`GET /v1/capacity` の `scaling.warm_pool = false` で分かるようにする。

6. **route は受付時に 1 回だけ解決し、invocation に generation を記録する。** `InvokeGate::resolve` が返す alias の generation を `Invocation.alias_generation` に保存する（`accepted_at` が route の解決時刻）。受付後に alias が動いても、queue 中・実行中の invocation の revision は変わらない（従来からの不変条件を記録で示す）。

7. **drain は reconciler が route の変化から始める。** 設定 cache が**有効なときだけ**（`ConfigCache::scale_view`）、route されている revision の集合と削除済み関数を読む。
   - **alias 切替（`alias_switch`）**: 前回の有効な観測で route されていて今回されていない revision。drain 中は、idle 環境を TTL・cooldown・`min_ready` に関係なく terminate し（ただし待機者と約束は守る）、実行中の環境は終了後に pool に戻さず terminate する（`release_for` が拒否）。先行起動は止める。旧 revision の環境の reuse key は新 revision と必ず違う（`revision_id` が key に入る）ので、新 revision が旧環境を使うことはもともと無い。何も残らなくなったら drain を忘れる（以後の pin 指定の invocation は通常どおり）。alias が戻れば（rollback）drain を終える。
   - **Secret 世代変更（`reuse_key_superseded`）**: invocation（と先行起動）が計算した reuse key を revision ごとの「最新の key」として pool が覚え（`note_current_key`）、別の key（古い `secret_binding_generation` など）の idle 環境を次の sweep で TTL に関係なく terminate し、実行中の環境は pool に戻さない。secret の値の変化は invocation が解決したときに初めて分かるので、変化の後に invocation も先行起動も無い revision では、古い idle 環境は TTL まで残る。
   - **関数削除（`function_deleted`）**: `DELETE /v1/functions/{id}` で `deleted_at` が付く（`deletion_state = deleting`）。受付は `resolve` で 409 `function_deleted`（`error_type = Host.FunctionDeleted`）。reconciler は関数の全 revision の drain を始め、**待機中の invocation を即座に同じ理由で終える**（起動していないので `Failed{platform_error, Host.FunctionDeleted}`、HTTP 409、attempt なし）。admission の再キュー（cold のやり直し）も拒否し、driver は環境を用意する前と dispatch の直前にもう一度削除を確認する（その間に削除されたら handler を起動せず環境を terminate して同じ失敗にする）。`Idempotency-Key` の再送は `resolve` が先に拒否するので、完了済みの invocation の再生も含めて 409 になる。実行中の invocation は完了まで待つ。何も残らなくなったら（この gateway の in-flight 0、admission の予約・待機 0、台帳の最近 1000 件の invocation がすべて terminal、関数の revision の active な環境 0）、管理 store を持つ gateway（`combined`）が `drained_at` を記録する（`deletion_state = deleted`）。削除済みの関数に許す書き込みはこの 1 回だけ（`repository/guard.rs`）。
   - route の観測は**遷移**で判断する。起動直後の最初の観測より前に行われた切替は drain されない（その時点でこのプロセスに環境は無い）。

8. **drain timeout。既定は「どの revision の timeout より長い」値にする。** `[scaling] drain_timeout_seconds` を省略すると、gateway は `limits.max_execution_timeout_seconds`（revision が設定できる最大の timeout、既定 900）+ cancel grace（秒に切り上げ）+ 60 s（既定では 961 s）を使う。drain（alias 切替・secret 世代変更・関数削除のどれも同じ値）はこの既定では、自分の timeout の内側にいる invocation を決して止めない。drain が止めるのは、それより長く残っているもの（通常はありえない。handler の時間は host が execution deadline で強制する）だけで、既定の drain timeout は「既存実行をすり替えない・切らない」ための安全網にすぎない。`drain_timeout_seconds` を明示的に「最大 timeout + grace」以下に設定するのは、drain で長い handler を途中で止めることを受け入れる設定なので、`[scaling] allow_short_drain = true` が無ければ設定検証で起動を拒否する（`services::scaling::tests::the_drain_timeout_defaults_past_the_longest_revision_timeout_and_short_ones_need_a_switch`）。削除にだけ短い timeout を持たせる設定は作らない（削除中の in-flight も同じ規則で完了を待つ）。

   drain を始めてから drain timeout を過ぎても、drain を始める前に受け付けた invocation がまだ走っていれば、reconciler はそれを止める（`CancelKind::Drain`）。handler 実行中なら通常の timeout と同じく `Cancel` → grace → terminate で、結果は `Failed{timeout, Host.DrainTimeout}`（HTTP 504）、環境は `Failed{drain timeout}`。まだ dispatch していなければ同じ error_type で起動しない。接続が切れて結果が分からなければ従来どおり `OutcomeUnknown`（自動再実行しない）。1 つの drain について 1 回だけ行う。

9. **振動しない。** (a) scale-down は cooldown と待機者・約束で止まる。(b) 先行起動は `min_ready` まで、待機者がいれば行わず、失敗したら backoff。(c) 設定 cache が期限切れ（control plane 停止）の間、route の集合は**前回の有効な観測のまま保持**し（`view = held`）、drain を始めない・終えない・`min_ready` の環境も route されたものとして守る。再接続の嵐（切断と接続の反復）でも、有効な観測の内容が変わらない限り何も起こらない（`tests/scaling.rs::an_outage_holds_routes_and_a_reconnect_storm_does_not_flap`）。(d) 1 回の reconcile は 1 つずつ（`tokio::sync::Mutex`）。

10. **観測（PLT-4637 への hook、最小限）。** `GET /v1/capacity` の各 revision に `min_ready`・`idle_ttl_seconds`・`scale_down_cooldown_seconds`・`route_state`（`routed` / `unrouted` / `superseded` / `deleting`）・`last_scale_event`（`kind`: `activation` / `scale_up` / `prestart` / `scale_down` / `scale_to_zero` / `drain`、`reason`、`at`）を足す。環境数 0 になって admission が revision を忘れた後も最後の event は残す（revision ごとに 1 件、最大 4096）。node 全体には `scaling`（reconcile 間隔、既定の TTL・cooldown、drain timeout、`warm_pool`、`at_zero`）。「ready」は `idle`（pool にあってすぐ使える環境）に当たる。metrics と履歴は PLT-4637（`docs/adr/0011-reuse-and-scaling-metrics.md`、`docs/metrics.md`）。

## 結果（consequences）

- 既存の設定はそのまま動く。`[scaling]` は省略可能（reconcile 1 s、cooldown 30 s、drain timeout は最大 revision timeout + grace + 60 s（既定 961 s）、backoff 5 s）。gateway は reuse の有無にかかわらず reconcile loop を回す（drain と削除の確定は pool が無くても必要）。旧来の「`idle_ttl/2` ごとの sweeper」はこの loop に置き換わった。
- pool が有効な構成では、TTL を過ぎた idle 環境の終了が cooldown（既定 30 s）だけ遅れうる。活性化の直後に消えなくなった代わりである。
- alias を別 revision に移すと、旧 revision の idle 環境は次の reconcile（既定 1 s 以内）で terminate される。pin 指定で旧 revision を使っている呼び出し元は、その間 warm を失う。
- 削除後、待機中だった invocation は 409 で終わる（以前は起動していた）。
- 状態（route の観測、drain、scale event、backoff）はプロセスのメモリにだけある。再起動で消えるが、その時点で環境も in-flight も無い（起動時 reconcile が片付ける）。`drained_at` は台帳に残る。
- 同じ `data_dir` の複数 gateway はそれぞれ自分の環境だけを scale / drain する（1 host 1 gateway が前提、ADR-0006 と同じ）。削除の確定は台帳を見るので、別 gateway の in-flight が台帳に残っていれば待つ。
- `min_ready > 0` の revision は、invocation が無くても node の容量と費用を使い続ける。

## 検証

- fake clock（`crates/application/src/services/admission/tests.rs`）: sweep 判定と到着の race（両方の順序）、活性化後の cooldown、待機者による保持、`min_ready` の収束と上限・待機者優先・保持、削除による待機者と到着の拒否と TTL を無視した drain、alias 切替の drain と約束の保護・rollback、scale 操作を混ぜたランダム操作での台帳の不変条件（8 seed × 2000 操作）。
- pipeline（fake provider、`crates/application/tests/scaling.rs`）: 0 → burst → idle → 0 → 再アクセス（cold）、0 からの burst の合流、Busy 環境を消さない、sweep と invocation の race 25 回、alias 切替中の長い処理（旧 revision で完了・route generation・旧 revision の drain）、drain timeout（`Host.DrainTimeout`）、削除中の実行・待機・再送（`Host.FunctionDeleted`、`drained_at`）、`min_ready` の先行起動と非振動、secret rotate による drain、data plane の停止中の保持と再接続の嵐。
- 台帳の契約（memory / SQLite）: 先行起動した `Ready`（epoch 0）の pool 入り。
- 実 gateway（process provider）: `scripts/e2e/zero-scale.sh`（`docs/evidence/*-zero-scale-process/`）。process provider は `idle_quiesce` を持たないので pool は無く、「idle → 0」は destroy-after-invoke による 0 である（script はそう記録する）。
- 既定の drain timeout で、alias 切替から 400 s・900 s 経っても長い handler は止まらず元の revision で完了する: `tests/scaling.rs::under_the_default_drain_timeout_an_alias_switch_never_stops_a_long_handler`。短い drain timeout（`allow_short_drain = true`）で止める経路: `a_drain_timeout_stops_what_still_runs_on_a_drained_revision`。
- **Firecracker（実 microVM、pool 有効）での zero-scale E2E は未検証**（検証 VM を別作業の benchmark が使用中）。script は `TSLS_GATEWAY_CONFIG` で任意の provider の設定を受け取る。
