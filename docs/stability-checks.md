# Stability checks

この開発ブランチはGo 1.27.1と隣の`elist_head`・`lista_encabezado`のcheckoutを使う。
elist_headは`65e5ae1c06ca91ef1e66c655233d6d0934d9c8a9`、lista_encabezadoは
`611497635248e41c2685729b1b6619919a978179`を本体の依存として使う。
型移行前の比較ではelist_headの`cab6a1bff65b540c8a777c20e9a41cb8899b6d4c`を使用した。
024で専用の`ReplaceWith`を廃止し、Mapの更新は既存の挿入・削除を組み合わせる。
`deps/elist_head`のsubmoduleは使わない。

既存checkoutを切り替えず依存側の専用worktreeで検証する場合は、Go workspaceで
対象を選ぶ。本体のビルドはworkspaceによる依存の選択に従う。
GitHub Actionsは上記のcommitを隣のディレクトリへ
checkoutする設定であり、依存commitがremoteで取得可能になるまでは実行できない。

リポジトリの隣の `../elist_head` と `../lista_encabezado` に上記commitをcheckoutした状態で、
リポジトリのルートから実行する。
旧lonchaの`task/009-list-stability`の修正は、lista_encabezadoへ移設・統合済み。

```bash
make ci
```

Nushell でも `make ci` を実行する。

対象はskiplistmapリポジトリ内の全パッケージ。依存リポジトリの単体テストは実行しない。
Go 1.27.1、Bash、`xargs` が必要。

ゲートは vet、race、`stephook` タグ付き race の順に確認する。
タグ付き検証は RMap の全テスト、バケットの J51/J56、型付きMap・キー照合・
slot再利用・pool中間挿入・コピー更新・外部Entry混在・Update callback・分割と更新の並行テストを対象にする。
通常の検証と checkptr は race 付きの `go test -v` に含める。
負荷の大きいテストは Makefile の `STRESS_TESTS` で分け、`make ci` では実行しない。
`make stress` はそれらを race 付きで実行し、分割中の並行更新は `stephook` 付きでも検証する。
vet は全パッケージで copylocks 以外の検査を行い、copylocks は本体ソースで検査する。
履歴ベンチマークの値レシーバーによる意図的な lock のコピーは変更しない。
既存の `//go:nocheckptr` は残っているため、その関数内の安全性まで保証する
検査ではない。

通常のテストだけ再確認する場合は、Bash と Nushell のどちらでも次を実行する。

```bash
make test
```

rmap の追加 fuzz 検証は、Bash で次のように実行できる。

```bash
go test ./rmap -run '^$' -fuzz '^FuzzOperations$' \
    -fuzztime=20s -parallel=2 -timeout=60s
```

Nushell では次のように実行する。

```nu
go test ./rmap -run '^$' -fuzz '^FuzzOperations$' -fuzztime=20s -parallel=2 -timeout=60s
```

使用例は README の `basic usage`、`example_test.go` の `ExampleMap_StoreItem`、
`rmap/example_test.go` の `ExampleRMap`。テストにある二つの例は上記ゲートで
実行する。README のコードは `main.go` として保存して実行できる。
