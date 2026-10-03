# 型付きMapの比較を再現する

`Benchmark_MapPerOperation`は操作単位で読み書きを切り替える変更後の測定方式。masterの`Benchmark_Map`とは読み書きの割当・キー走査・作業者数の扱いが異なる。値を`*lista_encabezado.ListHead`に固定した比較対象も含む。`skiplistmap4`・`skiplistmap5`は今回の本体に対する`Map[StringKey, any]`のアダプタ、末尾`_typed`は`Map[StringKey, *ListHead]`のアダプタである。旧版と新版の速度差を直接測る比較ではない。旧実装の掲載値はREADMEの過去の測定に分けて残す。

## 条件

- Go 1.27.1、GOMAXPROCS 16、初期キー100,000件。
- 並列数の指定は64。GOMAXPROCS 16の4倍で、実際も64 goroutineを使う。
- 既存のキー生成（`fmt.Sprintf`）、値生成、インターフェース呼び出しを含む。同じ実行関数をすべての比較対象に使う。
- 読み取り100%、読み取り50%・既存キー書込み50%、読み取り50%・別キー書込み50%を測る。
- 別キー書込みは`xx0xx`から`xx99999xx`を循環する。初回は挿入、その後は更新になり、無限に新しいキーを増やす測定ではない。
- mode 4はbucket 16・32、mode 5は16・32・64・128。別キー書込みは従来有効だったmode 5の64・128を維持する。mode 4のコメントアウト済み挿入行は追加しない。
- `cmap`を含む40ケースを、ケースごとに別プロセスで500msずつ5回測定する。偶数回はケース順を反転する。
- 仮想メモリ上限8GiB。全ケースを発見する試行が4GiBではメモリ不足となったため増やした。各プロセスの最大RSS・時間は`resources.tsv`に記録する。
- `ns/op`は成功した更新だけの時間ではなく、試行1回当たりの時間。失敗したSetの割合は`failed-writes/op`に別記する。

## 実行

Go、Git、Bash、GNU time、awk、lscpuが必要。集計にはNushellを使う。リポジトリのルートで、ほかのテスト・ベンチが終了してから実行する。次のコマンドはBash・Nushell共通。

```sh
bash tools/bench-release.sh ../skiplistmap-bench-027
nu tools/summarize-release.nu ../skiplistmap-bench-027/results.txt
```

`environment.txt`には本体commit、作業ツリーの状態、依存commit、GoとCPUの情報を保存する。`cases.txt`はベンチ自身から取得したケース名、`results.txt`は各回の出力である。集計は中央値・最小値・最大値、割当、失敗割合をCSVで返す。条件の変更には`GOMAXPROCS`、`BENCHTIME`、`BENCH_COUNT`、`BENCH_MEMORY_KB`を使い、異なる条件の結果は分けて扱う。

## 今回の結果

失敗後のユーザー指示により、新方式からcornelk/hashmapの3ケースを除外した。以下の保存結果は除外前の43ケースを対象とする。

64作業者の測定はreadonlyが全85測定完了、50/50は66測定でhashmapのnil型変換panicにより停止した。合計151/215測定。測定commitは`de44f86`で、当時の関数名は`Benchmark_Map`だったが、測定後に`Benchmark_MapPerOperation`へ改名した。原ログ・環境・途中集計はgit_task 027の`attachments/bench-64-per-operation/`に保存済み。README掲載対象は未決定。

## master方式

`legacy_map_bench_test.go`の`Benchmark_Map`はmaster `b7e996b`のケース・表示・測定処理を復元したもの。読み書き専任goroutine、ビットマスクによるキー選択、未使用のconcurrent=100指定、操作結果を検査しない挙動を保つ。module importとgeneric型引数・内部識別名を現行APIへ適応した。実装自体は現在のMapであり旧製品実装を復元したものではない。

旧sync.Mapアダプタの値receiverも保持したため、`go vet`はlockの値コピーを2件報告する。元方式はこの問題を含む歴史的な測定器として保存しており、修正済み方式と混同しない。ビルド確認には`go test -vet=off -run "^$" ./...`を使った。復元後のベンチ本測定は行っていない。
