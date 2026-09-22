# Typed elist_head migration results

旧Owner案の記録。現在の実装と測定は[List[T]の結果](task-013-list-results.md)を参照。OwnerとListHead[T]は撤去済み。

この文書は初回実装の記録。性能悪化のため013をin_processへ戻した。[ベンチ修正・再測定・申し送り](task-013-performance-reopen.md)が新しい状況であり、初回レビューを性能受容と扱わない。

013は任意structに埋め込む相対リンクを型付きにし、所有配列を保持して安全に辿る経路を追加した。既存Mapは型付きリンクへ接続したが、Map全体の所有権・並行性を安全化した結果ではない。

## 比較点と作業場所

- 親開始点: `3847649e3c5d8f8271579d4c69a5ea03735d963c`、統合先 `modernize/go-generics`。
- 作業: `../skiplistmap-task-013`、branch `task/013-elist-generics`。
- 依存原版: elist_head v0.2.8、`eefadded7d74d2d4badce57bbf9541264d386ded`。
- 依存変更: `be6e62b`。安全性修正 `506e1b7`、型移行 `fe1aabd`、sample方向修正 `6ba21cc`、型付き修復の順にcommitした。
- 旧測定器: 依存branch `baseline/013` の `ef00561`。worktreeは `../elist-head-task-013-baseline`。元実装に測定器だけを追加した。
- Go 1.27.1を[公式配布情報](https://go.dev/dl/?mode=json)で再確認。linux/amd64、AMD Ryzen 9 8945HS、比較は `-cpu 1`。

依存は `deps/elist_head` submoduleとgo.modのlocal replaceで選択する。依存git-dirは当該worktreeの管理領域にあり、メインcheckoutに既存submoduleはなかった。上流へのpushはしていないため、依存commitと比較用branchはこのローカルobject storeにある。別マシンへの移送時は依存commitも別途移す必要がある。

## APIと寿命

| 接続点 | 変更後 |
| --- | --- |
| ListHead | `ListHead[T]`。prev/nextは相対整数のまま、型付きOwner参照を1語追加 |
| 要素保持 | `NewOwner[T](offset, chunks...)` / `Register(chunks...)` が元の `[]T` を保持。要素コピーなし |
| 要素復元 | `ElementOf(*ListHead[T]) *T`。Owner必須、sentinelはnil、包含オブジェクト内のoffsetで同一実体を復元 |
| callback | `Owner[T].Each(func(*T) bool)`。falseで早期停止 |
| NewEmpty系 | 型パラメータを追加。旧Map用のowner無し経路として残す。NewEmptyListのhead/tail取得はpointer receiverへ修正 |
| slice修復 | free関数はtyped src/dstとtyped field selector、Ownerメソッドはtyped src/dst。修復規則を共有 |
| Map接続 | `ListHead[MapHead]` を一時的に使用。FromListHeadの返値を `*MapHead` にし、6箇所の型アサーションを除去 |

offsetは実際の `ListHead[T]` フィールドの `unsafe.Offsetof` を渡す契約であり、任意offsetの型を検証するAPIではない。所有者が配列をGCに保持し、相対リンクの整数アドレスは登録されたallocation内の実ポインタから解決する。要素を固定Itemへ詰め替えず、配列配置を維持する。owner無しのallocation越境復元は安全化済みと扱わない。

Owner操作の更新には全reader/writerの外部排他が必要。読取だけなら並行実行できる。異なるOwnerやraw/ownedの混在を更新前に拒否する。登録済み要素とOwnerを通常の値コピーやappendで移動しない。削除後も登録は維持され、同じ実体を再挿入できる。登録された要素への参照もOwnerを保持するため、削除は配列の解放ではない。

明示的な移動では、排他中に同長の別領域へコピーしてからOwner.RepaireSliceAfterCopyを呼ぶ。内部相対offsetは再設定せず、内部の逆リンク整合性はtyped src indexで検査する。範囲外リンクをコピー先で準備してから外部backlinkをCASで公開する。部分rangeの前後は登録を維持し、元rangeを未登録化する。成功後、以前返したsrcへのポインタは旧実体のままでlist要素としては失効する。拡張先の余剰部分はRegisterで追加できる。

Owner版の検査エラーではリンク・登録を変更しない。旧Map用raw版は従来同様CAS競合時に部分公開があり得るため、旧新allocationの保持が必要。このMap側の保持・reader寿命問題は005–008に残る。現在のMapHead型は移行用であり、任意structを受けるMapの最終型設計を確定したものではない。010/011でこの境界を除去する。

## 検証

| 対象 | 結果 |
| --- | --- |
| 依存通常テスト / vet | 成功 |
| Owner、typed sample、CAS競合、terminatorのrace / checkptr=2 | 無効化なしで成功 |
| READMEの独立main実行 | `alpha` / `beta` を出力 |
| 異なるstruct配置、複数allocation、同一実体、強制GC | 成功 |
| 部分移動、pool拡張、双方向走査、削除・再挿入、修復失敗時不変、混在拒否 | 成功 |
| Map全パッケージ通常テスト | 成功 |
| Map race / checkptr | 失敗継続。架空pointer生成は除去されたが、raw resolveのallocation越境で停止 |
| Map vet | 従来のunsafe.Pointer 3件とベンチのlock値コピー4件が残存 |

通常テストの失敗を隠すためのvet/race/checkptr無効化はしていない。go.mod更新で表面化したLogの非定数format 2箇所は `%s` に直した。旧Delete→再挿入の失敗、レビューで発見した外部更新の上書きは、それぞれ修正前の失敗と修正後の成功を確認した回帰テストがある。親のGo用統合ゲートは003の範囲であり、別用途のgit_task ciを合格扱いにはしていない。

[検査ログ](benchmarks/task-013-checks.txt)には終了コードと先頭の診断を保存した。依存のREADMEに再実行コマンドと実行済みの利用例がある。

## 性能

旧・新を別バイナリにし、旧→新→新→旧→旧→新の順で各200ms、各3回測定した中央値。入力・件数・操作を揃え、Owner登録は計測外。旧経路は所有者解決がなくcheckptr不適合なので、同じ安全性の実装同士の比較ではない。性能低下を許容したとの判定はしていない。

| 要素数 / 操作 | 旧 ns/op | 新 ns/op | 旧→新 B/op / allocs/op |
| --- | ---: | ---: | --- |
| 16 / 全走査 | 6.471 | 45.56 | 0 / 0 → 0 / 0 |
| 16 / 先頭で停止 | 0.392 | 2.816 | 0 / 0 → 0 / 0 |
| 16 / 実体復元 | 1.359 | 1.386 | 0 / 0 → 0 / 0 |
| 16 / 削除・再挿入 | 223.3 | 281.8 | 352 / 11 → 352 / 11 |
| 1024 / 全走査 | 997.3 | 2995 | 0 / 0 → 0 / 0 |
| 1024 / 先頭で停止 | 0.3895 | 2.843 | 0 / 0 → 0 / 0 |
| 1024 / 実体復元 | 1.355 | 1.373 | 0 / 0 → 0 / 0 |
| 1024 / 削除・再挿入 | 224.2 | 279.6 | 352 / 11 → 352 / 11 |
| 16 / コピー＋修復 | 35.46 | 198.0 | 8 / 1 → 0 / 0 |
| 1024 / コピー＋修復 | 1316 | 11911 | 8 / 1 → 0 / 0 |

削除・再挿入は1組を1opとする。旧デフォルトDeleteが失敗するため、旧新とも排他中の明示的Init callbackで測り、デフォルト削除経路の同等比較とは扱わない。コピー＋修復は実コピーを含み、サイズ増加と新たな整合性検査・登録更新の費用も含む。単独Insert/Deleteの時間には分離していない。

1024要素を1/8/64配列へ分けた全走査は、新経路で3071 / 5122 / 22886 ns/op、いずれも0 B/op・0 allocs/op。所有配列の線形検索が多配列時の制限となる。binary/hybrid検索も測ったが、小配列の速度低下と新たなsort不変条件が必要となり、独立レビューで今回は線形を維持した（013 comment109）。内部リンクまで修復する初稿は1024要素で約31300nsだったため、不要な復元・再設定を削減した（comment110）。

amd64のサイズはListHead 16→24B、MapHead 40→48B、SampleItem 72→80B。Owner本体80Bに加えて登録slice headerを保持する。挿入ごとの要素allocationは増えない。実体復元は同程度だが、走査・修復の速度低下は残っている。

生データ: [基本操作](benchmarks/task-013.txt)、[複数配列](benchmarks/task-013-chunks.txt)、[コピー修復](benchmarks/task-013-repair.txt)。基本操作・複数配列は依存7bb97d1、追加の修復最適化を含むコピー修復はbe6e62bで測定。後者で前者の経路は変更していない。

## 型消去の残存と003への引き継ぎ

| 残存箇所 | 理由 / 後続 |
| --- | --- |
| 依存のerrorとListHeadErrorアサーション | 標準errorの識別。要素の型消去ではないため維持 |
| 依存のHead[T] / List[T]とany制約 | 要素型はTを通す。空interface値の保存はない |
| raw Ptr / OuterPtrs / StoreListHead、owner無しresolve | 旧Map・診断との接続。005–008/011でowner経路へ移す際に除去・限定 |
| Map.Get/Set/each、entryHMap、SampleItem、MapItem、KeyToHash | 既存K/Vと要素dispatch。010/011で型付きMap・任意structの設計へ移行 |
| poolのSampleItem assertion、atomic.Value/sync.Map等の読出しassertion、slice/mutex操作 | 既存pool表現と標準API境界。005–008で寿命修正、011で型消去を除去 |
| rmapの値・callback | 012で型移行 |
| Log / debug / DumpExpandInfoの可変引数 | fmtとの異種値表示境界として維持。通常の要素格納経路ではない |
| lonchaのList/Traverse等 | bucket/poolの既存接続。今回の変更は不要、並行安全性は後続で検証 |

[親側の検索結果](benchmarks/task-013-erasure-inventory.txt)は119a4b6のGoソースを対象に保存した。依存通常経路にはinterface{}値・型消去したanyを追加していない。Map全体の比較基準は未整備なので、003で元のb7e996bと013統合後を別の比較点として測る。依存単体の成功でMap安定化の完了条件を満たしたとはしない。
