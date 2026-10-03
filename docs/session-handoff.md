# Resume stabilization through git_task

## 次のセッションの入口

013はOwner撤去後のList[T]にも走査遅延があり、修正前レビューcomment141は未承認。その後、終端保持とnil変換を除き、offsetだけのList[T]へ修正した。[最新の実装・測定・申し送り](task-013-offset-results.md)を先に読む。List[T]は1view8Bでノード増加なし。Next走査はREADME方式と近い測定値になったが、動的offsetのDirectNextには差が残る。Map通常テストは今回は成功したが過去の断続的な失敗を解消したとは扱わない。状態の正はgit_task。

管理用ディレクトリは `/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap`、統合先branch は `modernize/go-generics`。013を兄弟worktree `skiplistmap-task-013`、branch `task/013-elist-generics` で実装・検証した。[実装結果・性能・残存制約](task-013-results.md)とgit_task 013の最新レビュー/状態を確認する。統合・doneはHUMAN-ONLY。013の統合・完了後に003、Map側の型移行010–012は安定化後に進める。

コマンドは nu-run、Markdown はその中の md_fetch を使う。以下はリポジトリ内で実行する Nushell のコマンドで、登録と読み戻しに使用済み。

```nu
git_task ls
git_task cat 001
git_task cat 013
git_task cat 003
```

## タスク一覧

| ID | 親 | 内容 | 依存 |
| --- | --- | --- | --- |
| 001 | — | 依存型移行後の intrusive map の安定化 | 子013、003–009 |
| 002 | — | 型パラメータへの移行 | 001、子010–012 |
| 013 | 001 | elist_headの型付きAPI・内部経路・本体接続 | なし |
| 003 | 001 | テスト・ベンチの基盤 | 013 |
| 004 | 001 | 基本操作の修正 | 003 |
| 005 | 001 | 要素の所有権・寿命 | 003。004とは独立 |
| 006 | 001 | リンク/bucket の公開・探索 | 003、005 |
| 007 | 001 | pool の拡張・分割・再利用 | 003、005、006 |
| 008 | 001 | Map の並行操作・キー同一性 | 004、006、007 |
| 009 | 001 | rmap・安定化全体の検証 | 003–008 |
| 010 | 002 | 型付き intrusive API の確定 | 001 |
| 011 | 002 | 本体・pool の型移行 | 001、010 |
| 012 | 002 | rmap・利用例・型別性能 | 011 |

計画見直し後の別依頼で013の実装を再開した。最新状態はgit_taskを正とし、003は013の統合・完了待ち。010–012は予約タスクで、013の型付きAPIと安定化で決まった所有権・実装に合わせて着手前に詳細化する。

git_task の保存先は `/home/xtakei/.local/share/git_task/skiplistmap/`。Git リポジトリ外のローカル保存なので、docs の commit だけでは別マシンへタスク本文は移らない。このマシンの新セッションでは同じストアを使用する。

## 引き継ぐ条件

- 任意のユーザー定義 struct に linked list を埋め込んで、その実体を直接つなぐコンセプトを維持する。ライブラリ固定の Item 型だけでは不足。
- 型移行は Map[K,V]/New[K,V] と内部の型付き保持が目的。API 変更は明示的に了承済み。互換ラッパーだけの対応にしない。
- elist_headのgenerics化を先行し、その後にMapの安定化の区切りを作り、Map側の型移行へ進む。型消去したinterface{}／anyは全体を通じて可能な限り除去し、残存理由を記録する。型制約のanyは対象外。
- generic method を使うが、以前の asItem[K,V] 案は最終仕様ではない。任意 struct に対応する型設計は未確定。
- elist_headを013の `deps/elist_head` submoduleとlocal replaceで修正した。依存commitはローカルのみで、別マシンへの移送には依存側のcommitも必要。
- テストとベンチは各変更に含める。依頼・結果・レビュー・判断を git_task add_comment に残す。

## 調査済みの証拠と限界

比較元は `b7e996b5d3c05607ec437371746cdbbaf539664a`。通常の go は1.22.2だったため、`GOTOOLCHAIN=go1.27.1` で公式ツールチェーンを取得・実行した。go.mod はまだ1.17のまま。実装開始時に最新版を再確認する。

Go 1.27.1 で全パッケージの通常テストは成功。race は elist_head.diffPtrTo の checkptr で停止。checkptr だけ無効にした診断では bucket 初期化と探索の race を検出。vet は unsafe.Pointer とベンチのロック値コピーを報告した。空 rmap.Get の panic、atomic.Value 自体の返却、削除後 Len 不整合を独立した小プログラムで再現済み。小プログラムは片付け済みで、恒久回帰テストは未追加。

013ではOwner案を撤去し、元のrawリンクとList[T]補助APIを分けて検証・測定した。同一allocationの利用例はGC・race・checkptrを通るが、Map通常テストは元版でも断続的なSIGSEGVがあり、race/checkptr/vetも未解決。003でMap全体の比較を行い、元のb7e996b、Owner案、最終List[T]案を混同しない。rawリンクのallocation境界とpool/reader寿命は005–008に残る。レビューと性能差の判断はgit_taskの最新コメントを参照する。

詳細は [移行案](modernization-plan.md)、[依存調査](dependency-investigation.md)、[依頼原文](modernization-request.md)を参照する。
