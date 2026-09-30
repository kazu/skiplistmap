# Stability checks

リポジトリと elist_head submodule を checkout し、隣の `../loncha` に
対応する loncha の変更を checkout した状態で、ルートから実行する。
009 の検証対象は loncha の `task/009-list-stability`、コミット `1a71ffe`。

```bash
make ci
```

Nushell でも `make ci` を実行する。

対象は本体の全パッケージ、`deps/elist_head` の全パッケージ、本体が import する
`../loncha/lista_encabezado`。Go 1.27.1、Bash、GNU time が必要。
デフォルトの `GOMAXPROCS` は4、プロセスの仮想メモリ上限は4 GiB。

ゲートは vet、通常、race、checkptr、`stephook` タグ付き race の順に確認する。
タグ付き検証は009の変更に関連する RMap の全テスト、バケットの J51/J56、
lista の `TestLenRestartsAfterCurrentNodeIsDeleted` を対象にする。
通常・race・checkptr・vet は上記の全対象パッケージを検証する。
各ビルドの Test・Example・Fuzz seed を一つずつ別プロセスで実行し、いずれかが
失敗するとその出力と終了コードを表示して終了する。race 検出器のメモリが
全テスト分蓄積しないようにするための分離であり、検出器は無効化しない。
既存の `//go:nocheckptr` は残っているため、その関数内の安全性まで保証する
検査ではない。checkptr は `-gcflags=all=-d=checkptr` を使用する。

一つのビルドだけ再確認する場合も、同じスクリプトを使える。

```bash
bash tools/ci.sh race
```

Nushell でも `bash tools/ci.sh race` を実行する。

rmap の追加 fuzz 検証は、Bash で次のように実行できる。

```bash
(
    ulimit -v 4194304
    export GOTOOLCHAIN=go1.27.1 GOMAXPROCS=4
    go test ./rmap -run '^$' -fuzz '^FuzzOperations$' \
        -fuzztime=20s -parallel=2 -timeout=60s
)
```

Nushell では次のように実行する。

```nu
ulimit -v 4194304
with-env {GOTOOLCHAIN: go1.27.1, GOMAXPROCS: '4'} {
    go test ./rmap -run '^$' -fuzz '^FuzzOperations$' -fuzztime=20s -parallel=2 -timeout=60s
}
```

使用例は README の `basic usage`、`example_test.go` の `ExampleMap_StoreItem`、
`rmap/example_test.go` の `ExampleRMap`。テストにある二つの例は上記ゲートで
実行する。README のコードは `main.go` として保存して実行できる。
