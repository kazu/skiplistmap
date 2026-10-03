# Go 1.27 と型パラメータ版への移行案

## 対象と現在地

対象は `Map[K, V]` / `New[K, V]()` への移行、既存の不安定な動作の修正、テスト・ベンチマークの拡充。キー・値の内部保持まで型付きにする。既存 API の互換維持は要求されておらず、型なし API を包むだけの実装にはしない。任意のユーザー定義 struct にリンクを埋め込み、その実体を直接つなぐコンセプトを必須条件とする。まず現行実装を安定化し、独立した完了地点を作ってから型移行する。

2026-09-22 時点の最新安定版 Go 1.27.1 を確認し、同じバージョンを取得して調査した。作業ブランチは `modernize/go-generics`、比較元は `b7e996b5d3c05607ec437371746cdbbaf539664a`。現時点では調査文書のみを追加し、本体・依存・テスト・go.mod は未変更。以下は実装前の協議用である。

## 確認できた問題

| 検証 | 結果 | 修正・検証の接続点 |
| --- | --- | --- |
| Go 1.27.1 で通常の全パッケージテスト | 成功。ただし不具合を取り逃している | 既存テストに値と状態遷移の検証を追加 |
| 同じテストを `-race` で実行 | `checkptr` が相対ポインタ計算で停止 | `elist_head.ListHead.diffPtrTo` / `InitAsEmpty` |
| 原因切り分けのため checkptr だけ無効化して `-race` | bucket 初期化と探索が競合 | `Map.makeBucket2` → `ListHead.Init` と `Map.bucketFromPoolEmbedded` → `DirectPrev` |
| `go vet ./...` | unsafe.Pointer の警告、ベンチでロックを含む構造体の値コピーを検出 | nil の定義、`syncMap.Get/Set`、`cMap.Get/Set` |
| 空の `rmap.New().Get("missing")` | panic | `RMap.initDirty` / `Get2` の read 初期状態 |
| `rmap.Set("key", 42)` 後の Get | `42` ではなく `atomic.Value` を返す | `RMap.Get2` の値取得 |
| `Map.Set(42, "value")` | string への型アサーションで panic | `Map.Set` と `SampleItem.K` |
| 1 件 Set → Delete → Len | Get は不在になるが Len は 1 のまま | `Map.Delete`、削除状態の更新、件数管理 |

通常テストは `go test ./... -timeout 30s`、race は `go test -race ./... -timeout 30s`、静的検証は `go vet ./...`。すべて `GOTOOLCHAIN=go1.27.1` の環境で実行した。checkptr 無効化は原因切り分けだけであり、修正後の合格条件には使わない。

API の症状は一時的な独立 Go プログラムから現行パッケージを呼び出して確認した。既存の rmap テストは `ok` しか検証していない。削除・同一キーへの同時挿入・ハッシュ衝突・GC とプール拡張については、これから恒久的な再現テストを作る。

既存ベンチの実測値を性能基準に採用していない。上記の値コピーに加え、計測中の `fmt.Sprintf`、goroutine 数で決まる読み書き比率、初回追加後に更新へ変わる挿入ケースを整理しないと、意図した比較にならない。

## 設計の方針

2026-09-22の追加指示により、elist_headのgenerics化を013として先行する。型移行全体でinterface{}／型消去したanyを可能な限り排除し、残す必要がある経路には理由を記録する。型制約のanyや利用者が明示的に選ぶinterface型と、ライブラリ内部の不要な型消去を区別する。依存の型付き実体復元と最小の寿命条件は013で確定し、010ではMap側のAPIへ発展させる。

1. **型と基本操作。** `Map[K comparable, V any]`、`New[K, V]()`、`Set(K, V)`、`Get(K) (V, bool)`、`Delete(K)`、`Len()`、`Range(func(K, V) bool)` を中心にする。`rmap` も同じ型を伝播させる。内部の item と pool に K/V を保持し、通常の検索・更新で `MapItem` の型アサーションを経由しない。欠損時は V のゼロ値と false を返す。nil が表現可能な V は nil を格納できるようにし、削除状態と値を別に扱う。
2. **メソッドの型パラメータ。** リンクヘッダから型付きの実体を取得する処理など、intrusive API の具体的な接続点で generic method を使う。最初に検討した `asItem[K,V]` は固定 Item 型に寄りすぎており、最終 API として採用済みではない。任意のユーザー定義 struct を扱う署名・型制約・offset と所有者の渡し方を task 010 で確定する。Go 1.27.1 でメソッド自身の型引数とインスタンス化後の offset を使う最小コードは `go run -race` に通ったが、任意 struct に対応する設計の検証は未実施である。
3. **キーと同一性。** ハッシュ順の構造を維持する。自然なキー順への変更ではない。`K comparable` に対するハッシュを評価し、ハッシュ一致後は実キーの同値性を確認する。`maphash.Comparable` は標準 API の候補として現行ハッシュと測定比較する。事前計算ハッシュの経路も実キーの照合を省略しない形へ移す。浮動小数点を含む comparable の意味は Go の map と揃え、NaN・±0・名前付き型も検証する。
4. **配置と所有権。** 階層 bucket、連続した item pool、相対リンクによる局所性を出発点とする。GC から実体が追える所有者、pool を拡張しても旧 reader が参照できる期間、削除済み領域の再利用条件を明文化する。相対値を偽のポインタとして作らないことだけでは GC 安全性の証明にならない。公開済みの atomic フィールドを含む item のコピーも監査する。
5. **更新と削除。** 同じキーの挿入が重複しないこと、値の読み取りが途中状態を見ないこと、削除の件数減算が一度だけであることを、各操作の成功が確定する一点に結び付ける。既存の bucket 単位の同期をまず精査する。値を `atomic.Pointer[V]` にする案は更新ごとの確保を増やしうるため、自動的には採用せず値サイズ別に評価する。
6. **低レベル API と設定。** 埋め込み item の利用、pool/search mode、事前計算ハッシュの用途を棚卸しする。API の型変更と機能削除を混同せず、必要な機能を型付き経路に接続する。汎用化のための interface 呼び出しをホットパスに戻さない。`sharedSearchOpt` や `DefaultModeTraverse` などの共有設定変更を監査し、複数 Map や並行探索が互いの設定を変えないようにする。

マップ全体を一つのロックで囲う変更や、別の hash map への置換は、現在の並行性・局所性を大きく変えるため第一案としない。相対リンクの維持に必要な同期・所有権が性能と両立しない場合は、その再現結果と測定値を示して配置方式を協議する。

## 実装順序と完了条件

親 task 001 は安定化、002 は型移行。登録した具体的な依存・接続点・完了条件は各 git_task 本文を正とし、[再開手順とタスク一覧](session-handoff.md)から参照する。実作業はタスクごとのbranch/worktreeで行い、git_task make_prでレビューに出す。[共通作業手順](task-workflow.md)を全タスクに適用する。

| Task | 必要な動作 | 先行条件 |
| --- | --- | --- |
| 013 | elist_head の型付きAPI・内部経路・本体への接続 | なし。最初に着手 |
| 003 | テスト・ベンチの比較基盤 | 013の統合・完了。旧版と依存型移行後を区別 |
| 004 | 逐次の基本操作・rmap の取得を修正 | 003 |
| 005 | 任意 struct の所有権・寿命を具体化 | 003。004 と独立 |
| 006 | リンクと bucket の公開・探索を修正 | 003、005 |
| 007 | pool の拡張・分割・再利用を修正 | 003、005、006 |
| 008 | Map の並行操作・同一性・設定干渉を修正 | 004、006、007 |
| 009 | rmap を安定化し、全体性能と安定化の区切りを検証 | 003–008 |
| 010 | 任意 struct のMap側型付き intrusive API と実例を確定 | 親001。013のAPIを引き継いで詳細化 |
| 011 | Map/item/pool を内部まで型付きにする | 010、親001 |
| 012 | rmap・利用例の型移行と最終検証 | 011 |

各段階は、先に動作を検証するテストを追加し、失敗を確認してから本体を修正する。修正と構造変更を分けて差分を読めるようにする。各動作の完了時と本体追加 100 行ごとに累積差分の必要性をレビューし、依頼・結果・対応をファイルに残す。実装の誤答・panic・安全性・指示違反がなくなった時点をレビュー終了条件とする。

## 追加するテスト

| 対象 | ケースと検証する結果 |
| --- | --- |
| 基本操作 | 空・1 件・欠損、上書き、削除、二重削除、再挿入、Range の停止と件数、Len と生存要素数の一致 |
| 型 | string、名前付き整数、比較可能 struct、pointer のキー。整数・大きい struct・slice・pointer・interface の値。ゼロ値、typed nil、nil 更新 |
| 同一性 | 同じハッシュを持つ異なるキー、同一キー、ハッシュ 0、±0・NaN。異なるキーの値が混ざらない |
| 境界 | bucket 分割と pool 容量の直前・一致・直後、深い階層、偏ったキー、全削除後の再利用 |
| 並行操作 | 同一キーの同時 Set、Get/Set/Delete 競合、別 bucket の更新、Range と更新、同時 pool 拡張、異なる設定の複数 Map。値の破損・重複・消失・設定干渉を検証 |
| GC と寿命 | 強制 GC を挟んだ追加・拡張・削除・再利用、pointer を含む K/V、reader が旧 pool を保持している間の更新 |
| モデル検証 | 逐次操作列を通常の map と比較する fuzz。小さな並行履歴は操作の開始・終了を記録し、合法な逐次順序が存在するか検証 |
| rmap | read/dirty の移行前後、miss、promotion、callback、上書き・削除の可視性と実値 |

通常テストと race だけで正しさを断定しない。並行テストは sleep に依存せず開始同期を使い、seed とタイムアウトを記録する。`GOMAXPROCS=1,2,4` と実機コア数を使い、race と checkptr を有効にした反復も行う。fuzz は決定的な回帰 seed を残し、CI の短時間実行と手元の長時間検証を分ける。

## ベンチマークと性能の判断

キー・値・操作列を計測前に作り、読み書き比率は worker 数ではなく操作数で決める。読み取り成功/失敗、既存更新、新規挿入、削除と再挿入、全件走査/早期停止を別に測る。新規挿入は既存キー更新に変質させず、データ量と定常状態が比較できる測定にする。結果の妥当性を計測外で確認する。

主な軸は、要素数 1K/64K/1M、短い/長い string と整数キー、値のサイズ、均等/一部集中/同一キー集中、read:write=100:0/95:5/50:50/0:100、goroutine 数、pool/search mode。全組合せを毎回回さず、代表セットと境界・競合の重点セットに分ける。

比較対象は修正済み測定器で動く旧実装、移行後の型付き実装、`sync.Map`、`map` + `RWMutex`。ハッシュだけ、検索だけ、pool 拡張、値更新の測定で全体時間の内訳も調べる。外部比較ライブラリは正しい使い方と保守状況を確認できたものを補助に使う。

同じ Go・CPU・GOMAXPROCS・入力・ビルド設定で旧新のバイナリを交互に複数回測り、ns/op・B/op・allocs/op を保存する。拡張や churn では保持メモリ・GC 負荷も見る。小さい差を一回の測定から断定しない。旧実装が失敗するケースは比較不能とし、正しく動かない処理の速さを維持目標にしない。

許容低下率は勝手に固定しない。検索時の追加 allocation を避け、再現する低下はプロファイルで原因を調べる。安全性の修正に費用が必要な条件は、率だけでなく allocation・同期・コピーの増分を示して判断する。

## 参照した設計資料

- [作成時の記事](https://mmap.dev/posts/concurrent_skip_list_map/)：単一 item list、dummy bucket、階層探索、固定長 array と allocation。
- [最適化その2](https://mmap.dev/posts/tuning-skiplistmap2/)：必要な下位 bucket のみを保持する階層構造。
- [最適化その3](https://mmap.dev/posts/tuning-skiplistmap3/)：連続 pool と相対リンク。GC が相対リンクを追跡しないこと、拡張時のコピーと局所性。
- [最適化その4](https://mmap.dev/posts/tuning-skiplistmap4/)：bucket に pool を接続した探索と bucket ごとの書き込み同期。
- [type parameter と linked list](https://mmap.dev/posts/type-parameter-linked-list/)：埋め込みの offset、interface 変換のコスト、型付きリスト。
- [type parameters と slice](https://mmap.dev/posts/slice_vs_generics/)：型付き callback と allocation の検証。
- [slice の atomicity](https://mmap.dev/posts/slice_atomicity/)：slice header と並行更新の問題。
- [Go 1.27 リリースノート](https://go.dev/doc/go1.27)、[配布中の安定版](https://go.dev/dl/?mode=json)、[maphash](https://pkg.go.dev/hash/maphash)、[atomic.Value](https://pkg.go.dev/sync/atomic#Value)。
