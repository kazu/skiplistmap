# 型付きAPIを利用者へ配布する

次のバージョンは`VERSION`の0.8.0。0.7.3からMap/EntryおよびRMapの公開APIが
型パラメータを取る形に変わるため、pre-1.0のminor versionを上げる。
このファイルとVERSIONは公開済みという意味ではない。tag、push、公開リリースは人が行う。

go.modのrequireは、公開されたelist_headの`ec42b76be1bc4de322da2b6fd0b3d68d5014ed44`と
lonchaの`1a71ffebf3c97146f935f257d249261470a54b46`のpseudo-versionに固定している。
隣のcheckoutへのreplaceは開発用で、利用者側では依存モジュール内のreplaceは適用されない。
配布検証ではGo workspaceを無効にし、ローカルreplaceなしでこのrequireを取得して実行する。

別ディレクトリの新しいcloneで通常・race・checkptr・vet・
停止テストとfuzzを実行し、READMEのMapとRMapの例、および外部Entryの実行例を確認する。
依存のローカルcheckoutやGo workspaceがなくても同じ結果になることを確認してから、
VERSIONと一致するv0.8.0 tagを付ける。公開前の検証結果は開発checkoutでの結果と区別する。

開発中は[stability-checks.md](stability-checks.md)の指定commitとGo workspaceを使用する。
devcontainerは開いたcheckoutとその兄弟ディレクトリを同じ絶対パスでbind mountし、
既存のworktreeを参照する。コンテナ内のGOWORKは`.devcontainer/go.work`に設定され、
依存は公開済みの上記pseudo-versionを使う。隣のcheckoutの状態には依存しない。
Goは1.27.1で、別のmasterを自動cloneしない。
既存のGo workspaceを使う場合は、そのファイルもマウント範囲内へ置く。
