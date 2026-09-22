# Worktree and PR workflow

## 適用範囲

git_task 001–013 の共通作業手順。実作業タスク003–013は、タスク専用の branch と worktree で作業し、git_task make_pr でローカル PR を作成してレビューに出す。001・002は親タスクなので、管理だけのために空のworktreeやPRを作らず、子のPRと検証結果を集約する。

## 着手と分離

1. 親タスク・対象タスク・この文書を読み、依存タスクの統合と完了を確認する。最初はelist_head型移行の013、その統合・完了後に003。Map側の型移行010–012は安定化001の完了を待つ。2026-09-22の見直し依頼は計画更新までで、実装再開は別途行う。
2. 統合先のデフォルトは `modernize/go-generics`。既存の管理用checkoutはこのbranchに保ち、実装を直接積まない。開始時の統合先commitを記録する。
3. タスクごとに `task/<ID>-<短い内容>` のbranchと専用worktreeを、確認した統合先commitから作る。worktreeはメインcheckout外の兄弟ディレクトリに置く。作成前に既存worktreeと未commit変更を確認し、名前が同じものを上書きしない。同じタスクの再開なら既存の作業場所を確認して使う。
4. git_taskのコメントにタスクID、branch、worktreeの絶対パス、統合先branchと開始commitを記録し、対象をreadyからin_processにする。既存タスクの依存を満たさないbranchから便宜的に開始しない。

git_taskはgit-common-dirから保存先を求めるため、同じリポジトリのworktreeからも既存のtaskストアを参照する。Markdownの参照は作業中worktreeのdocsを使う。

## 実装・依存・検証

再現テスト、修正、関連ベンチを同じタスク内で進める。差分の必要性レビューとその修正を含め、依頼・結果・判断はgit_task add_commentに残す。タスク全体を一度に完成させられない大きさなら、動作と接続点に沿って分割し、依存を更新する。

型移行ではinterface{}／型消去したanyを可能な限り除き、型付きの外側だけで済ませない。残存箇所には必要性・除去先または残す理由を記録する。型制約のanyは区別する。013は003に依存せず依存単体のテスト・ベンチと変更前の比較点を用意し、003でMap全体の比較基盤へ引き継ぐ。

依存ライブラリをsubmoduleとして修正する場合は、元checkoutと作業worktreeのgit-dir/core.worktreeを確認する。初期化によって元checkoutの参照先を変えない構成で準備し、依存側の修正commitと親側のgitlinkを対応させる。モジュールキャッシュを直接編集しない。

指定パスだけをaddしてcommitし、対象タスクの検証と性能比較を実行する。検証は対象worktreeで行い、結果には対象commitを記録する。既知の失敗と新規失敗を区別し、検査を無効化して合格としない。タスク003でこのrepoのGo用ゲートを整え、git_task ciの要求するmake ciとの接続を確認する。

## PR とレビュー

実装・検証がそろったらgit_task make_prを使い、対象ID、統合先を示す `--target`、タスクbranchを示す `--branch`、対象worktreeの絶対パスを示す `--worktree` を必ず指定する。

このrepoのremoteは現状originのみで、make_prのデフォルトであるghは存在しない。ローカルPRでは、当該branchを持つメインcheckoutの絶対パスを `--url` に明示する。これにより未pushのタスクbranchもローカルのgit request-pullで扱える。PRを作るためだけにremote追加やpushを行わない。

make_prはtaskストアに `010_branch.nuon` と `020_pull_request.txt` を保存する。記録されたbase・branch・worktreeと差分を読み戻し、タスク外の変更が混ざっていないことを確認する。これはGitHub上のPR作成ではなく、git_taskによるローカルPRである。

タスクをin_reviewへ進め、依頼・対象差分・検証結果・終了条件をコメントに登録してレビューする。指摘を修正したら同じworktreeで再検証・再レビューし、HEADが変わった場合はmake_prと検証記録も更新する。必要な指摘が解消したらout_reviewとしてkazuに渡す。

## 統合と後片付け

git_task merge と out_reviewからdoneへの遷移はツール上HUMAN-ONLYなので、kazuが統合・完了操作を行う。後続タスクはその統合結果を確認してから開始する。親001は013および003–009、親002は010–012の統合・検証が終わった時点で完了を判断する。

worktreeとbranchはレビュー・統合前に削除しない。統合後も未commit変更、依存側の未保存commit、必要な測定結果を確認してから片付ける。新セッションへの引き継ぎでは、taskコメントとメモリに作業場所、branch、対象commit、残作業を残す。
