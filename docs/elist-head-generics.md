# Add typed list helpers without changing embedded links

## 共通作業手順

[task-workflow.md](task-workflow.md) に従い、専用branch/worktreeで実装・検証し、git_task make_prからout_reviewまで進める。これは実装前の計画であり、現時点で型付きAPIの安全性・性能を検証済みとはしない。

## 親タスクと依存

親: 001。依存: なし。003より先に着手し、統合・完了後に003を再開する。002の型移行方針にも従うが、002や005/010の完了を先行条件にしない。

## 達成する動作

ユーザーの設計確認により、元の非genericなListHeadと相対リンクを維持し、要素型ごとの変換・操作を薄いList[T]にまとめる。任意structへの埋め込みと同一実体を維持し、Owner・要素slice管理・配列検索はelist_headへ持ち込まない。README方式とList[T]の速度・メモリ・profileを比較し、offset保持の影響を切り分ける。

## 接続点と変更範囲

elist_head v0.2.8にはinterface{}／anyはなく、主要リンク操作は具体型のListHeadを使っていると確認した。raw APIを維持し、旧List interface名をList[T]へ変更する。NewList、Link/Element、Next/Prev、DirectNext/DirectPrev、InsertBeforeを追加する。List[T]はoffsetだけを保持し、終端は呼び出し側がpointer比較する。型制約のanyは型消去した値とは区別する。具体的な設計確認は013comment131とその後の修正指示、最新結果はdocs/task-013-offset-results.md。

依存は本repo内のsubmoduleとローカルreplaceで修正する。既存MapのK/V型移行は011に残し、MapHead実体復元など利益のある接続だけを変更する。元から具体型のrawリンクを不要にgeneric化しない。旧Mapの型消去境界は理由・場所・除去先010/011を記録する。lonchaの全面変更は範囲へ加えない。

## 所有権・安全性と後続の境界

型付きAPIを成立させるのに必要なelist_headの所有者、GC到達性、同一実体への復元、参照の有効期間をここで確定する。既知の架空ポインタ生成やallocation境界の相対リンクはgenerics化だけで直ったと扱わない。新しい利用例を安全に実行するために必要な修正はこのtaskに含め、修正と構造変更のcommitを分ける。Map全体のpool拡張・削除・並行更新の寿命は005–008に残す。先行APIと両立しないことが判明した場合は、後続へ不整合を押し付けず接続点に沿って分割・依存更新する。

## 検証と完了条件

- 003を待たず依存単体のテスト・ベンチを用意し、変更前commitと同じ測定条件を保存する。追加・削除・走査／早期停止・実体復元を測り、ns/op、B/op、allocs/opを記録する。
- 異なる配置・埋め込み位置と独自フィールドを持つ複数structで、挿入・取得・削除後の同一性、呼び出し側が要素を保持した強制GC、文書化した寿命を確認する。利用例を実行し、race/checkptrを無効化せず同一allocationの新経路を検証する。
- skiplistmapの接続をビルド・通常テストで確認し、従来からのrace/checkptr/vet失敗と新規失敗を区別する。Mapの既知不具合を依存単体の成功で解決済みとしない。
- 型消去・型アサーションの残存一覧、必要な理由、後続の除去先を記録する。余計なコピー・allocation・interface dispatchの影響を測る。任意structや相対配置・連続poolの用途を黙って削らない。
- 依存commit、親gitlink、replace、型付きAPI、測定とレビュー結果をgit_taskへ記録する。003へ変更前／変更後の比較点と接続上の制約を引き継ぐ。

## 今回の作業範囲

013をworktreeで実装しout_reviewまで進める指示の後、Owner案を却下されin_processへ戻した。最新の承認は元のListHead＋List[T]補助APIとREADME方式との性能・メモリ比較。実装結果・測定・旧Mapとの境界は[結果文書](task-013-list-results.md)、最新の状態とレビューはgit_task 013を参照する。計測で残った性能差を許容済みと扱わない。
