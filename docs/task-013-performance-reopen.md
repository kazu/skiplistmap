# elist_headの性能回帰を再調査する

ここはベンチ比較基盤を整えた時点の記録。その後、ユーザー承認に従ってOwnerを撤去し、元のListHead＋List[T]へ実装を変更した。[最新の実装・測定・残存問題](task-013-list-results.md)を参照。

013は性能要件を満たさず、out_reviewからin_processへ戻した。前回のレビュー完了は性能低下を受け入れたという意味ではない。まず比較用ベンチを修正し、本体の性能修正は未着手の状態へ戻している。

## 比較基盤の修正

元の`Benchmark_Next`はb.Nを使わず固定10000回だけ走査していた。旧新とも1opを10000リンクの走査としてb.N回繰り返し、結果をsinkに残すよう修正した。繰り返し測定で露呈した元ベンチのslice寿命不足も、呼び出し側のKeepAliveで修正した。GCは無効にしていない。

値を読む全走査とコピー単独を両版へ追加。元本体eefaddedにベンチだけを加えた比較commitは5a278c4、変更後は3a11215。本体の差分は前回be6e62bから変えていない。ベンチ修正・採取script・再現patchと操作説明は[比較手順](../tools/elist-perf/README.md)にある。

## 再測定

Go1.27.1、linux/amd64、Ryzen9 8945HS、cpu1、各200ms、旧新新旧旧新の交互順、各3回の中央値。profile採取は別プロセス、各2s。時間比は変更後÷元。

| 操作 | 元 ns/op | 変更後 ns/op | 時間比 |
| --- | ---: | ---: | ---: |
| 既存Next / 相対リンク10000回、Ownerなし | 14113 | 23520 | 1.67 |
| 既存Next / 通常ポインタ10000回 | 17584 | 15696 | 0.89 |
| 1024全走査 / 変更後はOwnerあり | 996.5 | 2982 | 2.99 |
| 1024値を読む走査 / 変更後はOwnerあり | 1013 | 3378 | 3.33 |
| 1024コピー単独 | 304.6 | 406.4 | 1.33 |
| 1024コピー＋修復 | 1161 | 11246 | 9.69 |

通常ポインタ版は同じソースでも約11%違っており、allocation配置やコード配置等を分離できていない。この実行だけで小さな速度差を断定しない。元からある通常版と相対版の格納配置も異なる。他実装比較はこのloncha/lista_encabezadoに限り、container/listやMap全体の比較をしたことにはしない。

走査・コピーは0 B/op、0 allocs/op。コピー修復は旧8B/1alloc→新0B/0alloc。全操作の結果は[集計](benchmarks/task-013-profile-summary.nuon)、[生出力とprofile集計](benchmarks/task-013-profile-comparison.txt)、[条件とcommit](benchmarks/task-013-profile-manifest.nuon)に保存した。バイナリとCPU・memory profileは`/tmp/elist-013-comparison`に保持している。再現patchも別worktreeへ実際に適用し、通常テストと既存ベンチを実行した。

## 原因の確認範囲

旧1024走査はdirectNextのinline部分がCPU sampleの59.82%、ベンチループ40.18%。変更後はresolveが73.20% flat / 76.40% cumulative、DirectNext自体16.40%。元に無かったOwner解決と関数呼び出しが主因である。

コピー修復の旧版はnoInnersが70.50% flat、memmove20.06%。変更後はrepairSlicesが56.13% flat / 88.54% cumulative、locate closure20.16%、Owner修復6.72%。コピー単独の増加だけでは修復全体の約9.7倍を説明できない。変更後に追加した検証・走査の処理を見直す必要がある。generic化でsymbol名とinline範囲が変わるため、profileの割合を単純に差し引いて正確な費用とは扱わない。

DirectNextのwrapper順を変える試行は1024走査で改善しなかったため戻した。未測定の検索fast-path案も、ベンチ比較を先行するユーザー指示で戻した。試行結果と判断は013 comments115–121に記録している。

## 申し送り

元のREADMEはsliceに要素を置く設計を明示していたが、元にOwnerや所属slice一覧の検索はなかった。相対オフセットで直接たどり、コピー時に修復する設計である。今回、寿命保持・checkptr対応を理由に所有配列検索を独自に加え、性能要件と元の設計を崩した。テスト成功と性能低下の記録だけでout_reviewへ進めた判断も不適切だった。

今後は元のデータ配置・相対リンク・呼び出し側のslice保持を起点に、型付けに必要な変更と追加要件を分離する。元に無い検索・検査を当然の前提にして最適化しない。比較基盤ができたことを性能問題の解消と取り違えず、013はin_processのまま維持する。003は引き続き013の完了待ち。
