# elist_headの変更前後を測定する

最新実装は元のListHead＋offsetだけのList[T]。[最新結果](../../docs/task-013-offset-results.md)を参照。以下のOwner版commitは比較基盤を作った時点の履歴で、現行の設計ではない。

## README方式とList[T]の比較

Nushellで親worktreeから実行する。全24ケース（16/1024要素、Readme/Typed/RuntimeOffset/Local、対象操作）を検査し、各3回の交互測定と別実行のCPU profileを保存する。

```nu
nu tools/elist-perf/typed.nu deps/elist_head /tmp/elist-013-offset-final --benchtime 300ms --profiletime 2s
```

サイズ・allocationはsizes.txt、中央値はsummary.nuon、条件はmanifest.nuon、profileと対応binaryは同じ出力先に保存する。全方式が型付き終端と比較する。Typedは外で構築したviewを渡し、Localは計測関数内で定数offsetからviewを構築する。RuntimeOffsetもWalk/DirectWalkを測り、動的offsetの影響を切り分ける。遅い条件も残し、どの呼び出し方でも同等とは扱わない。

元の実装と変更後を別バイナリで交互に測り、操作別のCPU・メモリプロファイルを保存する。性能修正前の比較点として使う。依存本体の最適化はこのベンチ修正に含めない。

## 測定対象

- 元の本体: `eefadded7d74d2d4badce57bbf9541264d386ded`。ベンチ修正済み比較branchは`baseline/013`、`5a278c4`。
- 変更後: `deps/elist_head`の`task/013-generics`、`3a11215`。本体は`be6e62b`と同一。
- `Benchmark_Next`: 元からあるloncha/lista_encabezadoとの比較を修正。1opは10000回のNext、これをb.N回行う。通常版は個別allocation、相対版はsliceのため、格納配置を揃えた比較ではない。相対版はOwnerを使わない。
- `BenchmarkMigration`: 16/1024要素の全走査、値を読む走査、先頭停止、実体復元、削除・再挿入。変更後はこちらがOwner経路。登録・構築は測定外。
- `BenchmarkCopyRepair`と`BenchmarkCopyOnly`: コピーを含む修復と、同じ要素型のコピー単独。要素サイズの差も費用に含む。

旧新ともベンチがsliceをKeepAliveで保持する。結果をsinkへ残し、値を読む走査は合計も検査する。削除・再挿入は旧デフォルトDeleteの不具合を避けるため両方で同じInit callbackを使う。安全性が同一の実装という主張ではない。

## 実行

Nushell用。Go、Git、外部unameが必要。Go 1.27.1をGOTOOLCHAINで指定する。013の親worktreeで以下を実行する。

```nu
let old = ('../elist-head-task-013-baseline' | path expand)
let new = 'deps/elist_head'
let out = '/tmp/elist-013-comparison'
nu tools/elist-perf/compare.nu $old $new $out --benchtime 200ms --profiletime 2s
nu tools/elist-perf/summarize.nu $out
nu tools/elist-perf/summarize_test.nu
```

比較元のworktreeがない環境では、依存repoに元commitがあることを確認し、次で比較元を作れる。既存ディレクトリには実行しない。パッチは本体を変更せず、元のテスト修正と追加ベンチだけを含む。

```nu
let old = ('../elist-head-task-013-baseline-reproduce' | path expand)
let patch = ('tools/elist-perf/original-benchmarks.patch' | path expand)
git -C deps/elist_head worktree add --detach $old eefadded
git -C $old apply $patch
```

作成後は実行例のoldにその絶対パスを指定する。比較scriptは元の本体がeefaddedから変更されていないことを検査する。修正ベンチを含む状態とcommitをmanifestへ保存する。

## 出力と読み方

`manifest.nuon`にsource commit、dirty状態、Go・OS条件、測定順を保存する。時間測定はoriginal/modified/modified/original/original/modifiedの順で各3回、cpu1。profileは時間測定と別プロセスで採取し、時間表に混ぜない。

- `timing-*.txt`: ベンチの生出力。`summary.nuon`: 中央値、比率、B/op、allocs/op。
- `*.test`: profileと対応するバイナリ。
- `*-original.cpu.pprof`と`*-modified.cpu.pprof`: 同じ操作のCPU profile。
- `*.mem.pprof`: Goデフォルトのサンプリングによるheap profile。主にfixture構築も含むため、1操作の割り当て比較にはベンチのB/op・allocs/opを使う。
- `*-cpu-top.txt`と`*-mem-top.txt`: 両版の読みやすい集計。メモリ集計はデフォルトのinuse_space。

Nushellで同じ操作の上位関数を並べて確認する例:

```nu
let out = '/tmp/elist-013-comparison'
open --raw ($out | path join 'walk1024-original-cpu-top.txt')
open --raw ($out | path join 'walk1024-modified-cpu-top.txt')
```

旧新でgenericのsymbol名とinliningが変わるため、pprofの関数名ベースの単純な差分を速度差とは扱わない。ns/opと各profile内の割合を併せて読む。profiletimeは必要なら長くする。

既存の通常ポインタ版にもバイナリ間の変動がある。微小な差の優劣はこの測定だけで断定しない。元の性能に対する数倍の悪化を、他実装との比較で許容したことにはしない。
