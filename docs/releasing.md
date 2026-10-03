# 型付きAPIを利用者へ配布する

次のバージョンは`VERSION`の0.8.0。0.7.3からMap/EntryおよびRMapの公開APIが
型パラメータを取る形に変わるため、pre-1.0のminor versionを上げる。
このファイルとVERSIONは公開済みという意味ではない。tag、push、公開リリースは人が行う。

go.modのrequireは、elist_headの`65e5ae1c06ca91ef1e66c655233d6d0934d9c8a9`と
lista_encabezadoの`611497635248e41c2685729b1b6619919a978179`のpseudo-versionに固定している。
elist_headの新しい境界置換APIは未公開のローカルcommitに含まれる。
配布検証とCIの前に、人がこの依存commitを公開する。本体だけを先に公開しない。
隣のcheckoutへのreplaceは開発用で、利用者側では依存モジュール内のreplaceは適用されない。
配布検証ではGo workspaceを無効にし、ローカルreplaceなしでこのrequireを取得して実行する。

別ディレクトリの新しいcloneで通常・race・checkptr・vet・
停止テストとfuzzを実行し、READMEのMapとRMapの例、および外部Entryの実行例を確認する。
依存のローカルcheckoutやGo workspaceがなくても同じ結果になることを確認してから、
VERSIONと一致するv0.8.0 tagを付ける。公開前の検証結果は開発checkoutでの結果と区別する。

開発中は[stability-checks.md](stability-checks.md)の指定commitとGo workspaceを使用する。
devcontainerは開いたcheckoutとその兄弟ディレクトリを同じ絶対パスでbind mountし、
既存のworktreeを参照する。コンテナ内のGOWORKは`.devcontainer/go.work`に設定され、
依存は上記pseudo-versionを使うため、elist_headの新commitが公開されるまでは取得できない。
未公開の依存を検証する場合は、[stability-checks.md](stability-checks.md)のとおり
対象の依存worktreeを含むGo workspaceを指定する。
Goは1.27.1で、別のmasterを自動cloneしない。
既存のGo workspaceを使う場合は、そのファイルもマウント範囲内へ置く。
