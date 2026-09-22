# Resume stabilization through git_task

## 次のセッションの入口

管理用ディレクトリは `/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap`、統合先branch は `modernize/go-generics`。本体は未変更。2026-09-22に003の着手を停止し、elist_headのgenerics化を先行する計画へ変更した。次の実装候補は **013**。今回の依頼は計画更新までなので、実装は別途仕切り直す。再開時は親001・013と[共通worktree・PR手順](task-workflow.md)を読み、013専用branch/worktreeで実装・検証・ローカルPR・レビューを行う。013の統合・完了後に003、Map側の型移行010–012は安定化後に進める。

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

見直し後は013がready、003は依存待ちのbacklog。readyは開始可能な依存状態を表し、今回実装を再開した意味ではない。以後の状態と本文はgit_taskを正とする。010–012は予約タスクで、013の型付きAPIと安定化で決まった所有権・実装に合わせて着手前に詳細化する。

git_task の保存先は `/home/xtakei/.local/share/git_task/skiplistmap/`。Git リポジトリ外のローカル保存なので、docs の commit だけでは別マシンへタスク本文は移らない。このマシンの新セッションでは同じストアを使用する。

## 引き継ぐ条件

- 任意のユーザー定義 struct に linked list を埋め込んで、その実体を直接つなぐコンセプトを維持する。ライブラリ固定の Item 型だけでは不足。
- 型移行は Map[K,V]/New[K,V] と内部の型付き保持が目的。API 変更は明示的に了承済み。互換ラッパーだけの対応にしない。
- elist_headのgenerics化を先行し、その後にMapの安定化の区切りを作り、Map側の型移行へ進む。型消去したinterface{}／anyは全体を通じて可能な限り除去し、残存理由を記録する。型制約のanyは対象外。
- generic method を使うが、以前の asItem[K,V] 案は最終仕様ではない。任意 struct に対応する型設計は未確定。
- 修正が必要な kazu のライブラリは、この repo 内の submodule で対応してよい。現時点では submodule は追加していない。
- テストとベンチは各変更に含める。依頼・結果・レビュー・判断を git_task add_comment に残す。

## 調査済みの証拠と限界

比較元は `b7e996b5d3c05607ec437371746cdbbaf539664a`。通常の go は1.22.2だったため、`GOTOOLCHAIN=go1.27.1` で公式ツールチェーンを取得・実行した。go.mod はまだ1.17のまま。実装開始時に最新版を再確認する。

Go 1.27.1 で全パッケージの通常テストは成功。race は elist_head.diffPtrTo の checkptr で停止。checkptr だけ無効にした診断では bucket 初期化と探索の race を検出。vet は unsafe.Pointer とベンチのロック値コピーを報告した。空 rmap.Get の panic、atomic.Value 自体の返却、削除後 Len 不整合を独立した小プログラムで再現済み。小プログラムは片付け済みで、恒久回帰テストは未追加。

性能基準はまだ測定していない。013で依存単体の変更前後を測り、003で壊れた比較用receiverと測定内容を直してMap全体の旧新比較を行う。元のb7e996bと013完了時の比較点を区別し、同じ測定器で測れない経路は制限を記録する。GC寿命、移動先公開順、部分失敗は静的調査の懸念であり、依存API成立に必要な条件は013、Map全体の具体的再現と修正方式は005–007で確定する。以前のplan-reviewは当時の協議案へのレビューで、実装や後の型設計を承認するものではない。

詳細は [移行案](modernization-plan.md)、[依存調査](dependency-investigation.md)、[依頼原文](modernization-request.md)を参照する。
