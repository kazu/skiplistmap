# List[T]の実装とREADME方式との比較

この文書は終端を保持してnilへ変換していた旧版の記録。修正前レビューはcomment141で未承認。[終端判定を除いた修正版と再測定](task-013-offset-results.md)を最新資料とする。

元の埋め込みListHeadを維持し、任意のstructに対する型付き操作をList[T]へまとめた。Owner・chunks・ListHead[T]は削除した。依存の実装は11cac48、保持する参照の説明だけを直した最終依存は8785631。計測binaryは11cac48のものを保存している。

## 変更内容と接続

ListHeadは元と同じprev/nextのuintptr2個。List[T]は既存のhead/tail pointerとoffsetを持つviewで、要素の格納や配列検索をしない。終端pointerのallocationは参照するが、別allocationの要素群を保持・管理する仕組みはない。保持・拡張・コピー修復・同期は呼び出し側の責務。

NewList[T]、Link、Element、First/Last、Next/Prev、DirectNext/DirectPrev、InsertBeforeを追加した。元のraw APIは残す。旧List interfaceの名前はgeneric structへ変わり、SampleEntry.FromListHeadは*SampleEntryを返す。SampleEntryごとの手書き変換は任意となり、READMEには手書き方式とList[T]で同じ要素を読む実行済みmainを載せた。

skiplistmapはraw ListHeadと元のpool修復呼び出しへ戻した。MapHeadの実体復元だけにList[MapHead].Elementを使用し、具体型の戻り値で6か所の型アサーションを除去した。ここでは一時的な定数設定のviewがインライン化され、永続的なviewや要素ごとのメモリは追加しない。Mapの全走査をList.Nextへ置き換えてはいない。K/Vのinterface{}やHMapEntry経由の型消去は010/011の対象として残る。

## メモリと速度

amd64でListHead16B、SampleEntry40B、MapHead40B、SampleItem72B。以前のOwner案にあったノードごとの8B増加はなくなった。保存するList[T]は1リスト24Bで、そのうちoffsetは8B。構築・走査・実体復元の測定はすべて0 B/op・0 allocs/op。viewをheapへ置くなど呼び出し側の保存方法まで無条件に0allocと保証するものではない。

Go1.27.1、linux/amd64、Ryzen9 8945HS、cpu1。同じSampleEntry配列・順序・終端・合計計算を使い、README/Typed/RuntimeOffsetを交互順で各3回、300ms、中央値を比較した。profileは別実行2s。

| 操作 / 要素数 | README ns/op | List[T] ns/op | List/README |
| --- | ---: | ---: | ---: |
| Next走査 / 16 | 31.23 | 42.02 | 1.35 |
| Next走査 / 1024 | 2170 | 2549 | 1.17 |
| DirectNext走査 / 16 | 10.98 | 21.71 | 1.98 |
| DirectNext走査 / 1024 | 1228 | 1837 | 1.50 |
| 先頭だけ / 1024 | 1.632 | 2.55 | 約1.56 |
| 実体復元 / 1024 | 1.358 | 1.511 | 約1.11 |

**List[T]の走査は速度同等ではない。** README方式は型付き終端と比較し、List[T]はnilへ変換する。追加の終端判定とgenericラッパーのインライン展開の違いがある。同じNextを実行時offsetだけで書いた比較は1024要素2145ns対README2170nsで、この測定ではoffset保持だけによる大きな遅延は観測しなかった。微小差の優劣は判断しない。

最終CPU profileではtyped走査のElement部分が17.48% flatで、rawリンク処理とループ以外の仕事も見える。初稿ではList.Nextのinline cost101がbudget80を超えることも確認した。方向別に変換を複製した試行は十分な改善にならず戻している。削除マークの復号実装でも比較比率が変わるため、初稿の約1.5倍を最終値として流用していない。

Map側の実体復元だけを測る比較は旧1.358ns、List経由中央値1.359ns、両方0allocだった。Map全体の性能同等を証明する測定ではないが、List.Nextの走査コストをMapへ持ち込んだ変更ではない。

## 元のraw実装との比較

比較元は元本体eefadded＋修正ベンチ5a278c4。200ms、旧新新旧旧新の各3回中央値。新側は11cac48。Owner案の約3倍・約9倍という結果とは比較点を分ける。

| 操作 | 元 ns/op | 今回 ns/op | 今回/元 |
| --- | ---: | ---: | ---: |
| 10000回Next | 14111 | 16120 | 1.14 |
| 1024単純DirectNext走査 | 994.4 | 998.3 | 1.00 |
| 1024値を読むDirectNext走査 | 1014 | 1183 | 1.17 |
| 1024コピー＋修復 | 1143 | 1328 | 1.16 |

既知の削除マーク復号修正は残している。毎回のmaskを、マーク付きの場合の補正へ変えることで単純走査を約1200nsから約1000nsに戻したが、全操作が元と同速になったとは言えない。コピー修復は元のraw処理に戻したため、旧新とも8B/1allocへ戻っている。測定は速度劣化を許容した判断ではない。

## 検証と残存問題

- 依存通常テスト・vet、異なる配置/同一実体/終端/双方向/削除再挿入/並行reader/README例のrace・checkptr=2に成功。呼び出し側が保持した同一allocationで、検査を無効化せず確認。
- README mainはmanual/typed双方でalpha、betaを出力した。既存ベンチのb.NとKeepAlive修正を維持。
- Map通常テストは成功回がある一方、最終全体実行でSIGSEGVを記録した。Test_HMapの5回繰り返しはその後成功。元親3847649＋元依存でも同系統のSIGSEGVを再現したので、既存の不安定さがあることは確認できるが、全体成功とは報告しない。
- 元版の再現は一時modfileで言語版を1.27.1に揃え、元のprintf vet失敗を通過するため診断実行に限ってvetを外した。これは合格ゲートではない。元ソースは変更していない。
- Mapのrace/checkptr/vetは既存経路で失敗。別allocation間の相対リンク、pool/reader寿命、並行更新は003/005–008に残る。

再現手順は[比較ツール](../tools/elist-perf/README.md)。[raw生出力](benchmarks/task-013-final-raw.txt)、[typed生出力](benchmarks/task-013-final-typed.txt)、各summary/manifest.nuon、[Map実体復元](benchmarks/task-013-map-recovery.txt)、[Map検証失敗](benchmarks/task-013-final-map-checks.txt)、[元Mapの障害再現](benchmarks/task-013-original-map-fault.txt)を保存した。binary/profileは/tmp/elist-013-final-rawと/tmp/elist-013-final-typedに保持。

本体有効Go行数は元比で親3696→3697、依存821→849。Owner案の親3691・依存977からは縮小した箇所と戻した箇所を区別しており、差分を積み上げて数えていない。

## 申し送り

元のrelative link設計に所有配列検索を持ち込んだ判断と、性能悪化を残してout_reviewへ進めた判断は誤りだった。最新の承認は「元のListHead＋薄いList[T]」と、その追加コストの調査。List[T]を使えば手書きメソッドを減らせるが、走査コストを隠して一律の置き換えを勧めない。終端判定を不要として勝手に落としたり、GC保持をelist_headへ再導入したりしない。
