# Dependency investigation

1. 調査対象は `go.mod` が固定する `github.com/kazu/elist_head v0.2.8` と `github.com/kazu/loncha v0.4.11` の `lista_encabezado`、および本体の pool 接続部分。モジュールキャッシュを読み取り、実装・依存の変更、テスト・ビルド・lint は実施していない。通常テスト成功、checkptr の初期化時停止、checkptr を外した診断での loncha の競合は母艦の実測情報であり、この調査による追加再現ではない。

2. 最初の checkptr 停止には局所的な原因がある。elist_head の `elist.go` にある `ListHead.diffPtrTo` は `unsafe.Add(t, -int(uintptr(p)))` を返し、呼び出し側がその架空のポインタを `uintptr` の差分として保存する。差分計算の返却型を整数にし、ポインタ同士のアドレス差を整数演算だけで計算すれば、この架空ポインタの生成は不要になる。既存の `diffPtrToHead` とその呼び出し側が変更対象。ただしこれだけで依存全体を checkptr 対応済みとはできない。

3. 相対リンクの復元にも別の問題がある。elist_head の `ListHead.directNext` / `directPrev` は、atomic に取得した差分を現在ノードのアドレスへ `unsafe.Add` する。`samepleItemPool.freeHead/freeTail` と `items`、複数 pool、map/bucket の境界を越える接続では元と先が別 allocation になり得る。差分計算だけの変更や `//go:nocheckptr` の追加では、この構造上の問題は解消しない。`ElementOf` のような同じ包含オブジェクト内部の変換とは区別して対応する必要がある。

4. elist_head の `ListHead` は `prev/next uintptr` のみで、リンク先を GC に保持させるポインタを持たない。所有者が `[]SampleItem` や pool ポインタを保持している間は配列が生存するが、リンク自体は所有権にならない。`NewEmpty` / `_InitAsEmpty` は独立 allocation のノードを相対差分で接続する API で、一般利用時にも所有者が必要となる。`initedListHead.Head` / `Tail` / `Insert` は値 receiver なので呼び出しごとの配列コピーを扱い、同一 list の head/tail を取得する意味にも問題がある。単純な GC 保持用フィールド追加はノードサイズと走査コストを増やすため、pool 単位の所有権で解く案を先に検討する。

5. 移動後修復は同期と公開順の設計変更が必要。本体の `samepleItemPool._expand` は `append(nPool.items, sp.items...)` で公開済み要素を複製し、elist_head の `RepaireSliceAfterCopy` で外側のリンクを差し替える。同関数は移動先取得にも `unsafe.Add(cur, moved)` を使用し、外側リンクの CAS の後で `dHead.prev/next` を通常書き込みで補正する。外側から新配列へ到達した reader が補正前の差分を見る可能性がある。また途中の CAS 失敗に rollback がなく、部分的に公開された新配列が caller の error return で所有者を失う可能性を調べる必要がある。pool 自身の mutex だけでは、別経路の reader、旧ノードの参照、移動と同時の値更新まで保護できるとは確認できなかった。

6. `_split` は `_expand` と異なり既存配列の範囲を共有する。`itemSlice.CopyFrom` と len/cap の個別 CAS による slice header 更新には `//go:norace` もあり、単一 snapshot の公開と所有権を確認する必要がある。`_split` 冒頭の CAS はローカル変数 `nlen` が対象で、共有 pool の排他にはならない。型付き内部保存へ変更する際も、生きた要素をコピーする方式をそのまま一般の `V` に広げると、copy 非対応の値や更新中の値の扱いが問題になる。移動時にどの参照が無効になるかも決める必要がある。

7. loncha の `ListHead` は実ポインタの `prev/next` を持つため elist_head と GC 条件が異なるが、`Init` が通常書き込み、`DirectNext` / `DirectPrev` が通常読み取りである。母艦の競合報告は本体 `Map.makeBucket2` の再初期化と検索側の `DirectPrev` に一致する。既存の `InitAfterSafety` / `IsSafety` は接続状態の確認であり、既にノードを得た全 reader の終了を保証する仕組みとしては確認できない。公開済みノードの再初期化を避けることと、共有リンクの読み書きを一貫した同期へ揃えることが最小修正候補。初期化行だけ atomic に変えても公開と再利用の意味までは直らない。削除処理の low-bit pointer marking と package-global の `MODE_CONCURRENT` / traverse 設定も同じ監査範囲になる。

8. 相対配置と連続 pool を保つ最小設計候補は、各 pool 内のリンクを index/offset として所有配列から解決し、pool 間接続には GC が追跡する明示的な pool 参照を使う方式。固定サイズ chunk を増設すれば公開済み配列を移動せずに済み、全要素の所有ポインタを追加せずに寿命を pool 単位で管理できる。ただし現在の `ListHead.Next()` 単独 API は所有配列を知らないため、owner を引数か別の構造に持つ変更が必要。chunk 境界の検索と index 解決の費用は未計測。今の copy-and-repair を維持する案は旧新配列の保持、reader の終了確認、コピー中更新の扱い、公開順、失敗時処理が必要で、差分計算の修正より大きい。全リンクを通常ポインタへ置換する案は単純化できる一方、相対リンクの特性を捨てるため最初の採用案にはしない。

9. 2026-09-22の追加指示で、elist_headのgenerics化を013として先行する方針へ更新した。以前の「両依存の全面generics化は不要」という範囲判断は撤回する。本体内のsubmoduleとローカルreplaceでelist_headを型付きにし、任意structの復元と必要なowner/lifetime条件、本体との接続を検証する。lonchaは必要な変更箇所を調査して対応する。モジュールキャッシュは変更しない。型消去を可能な限り除く一方、generics化自体を既知のGC・race・checkptr問題の解決と同一視しない。具体的な範囲は[先行タスク](elist-head-generics.md)とgit_task 013を参照する。

10. 実装前の検証計画には、独立 pool 境界と同一配列内リンク、強制 GC 下の保持、拡張・分割・削除再利用中の並行検索と更新、修復 CAS 失敗、checkptr を有効にした race 実行を含める。性能は pool 拡張直前直後、小規模・大規模、検索と更新の比率、allocs/op と GC 費用で比較する。この報告は静的調査であり、GC 回収や部分公開の具体的再現、最小候補の性能、安全なロック範囲は未検証。測定前に性能低下や修正完了を断定しない。
