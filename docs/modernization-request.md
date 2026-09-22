# Modernize Go and typed map operations

## User request

ええと　このリポジトリを go  の最新版にすること, またtype parameter 版への変更をやってほしい。あといろいろ不安定な部分もあるのでそれの修正をお願いしたい。パフォーマンスの効率性は可能な限りおとさないようにまたこれで使ってる俺が作成したライブラリ側も修正が必要であればしていい。その場合は、どうするかは、いったんはこのなかにsubmodule つくって対応するのでいいです。
あと　https://mmap.dev/posts/concurrent_skip_list_map/　に　これを作成したときの文章がある。あと最適化関連もこの　blog にはいろいろ書いてあるので
参照してください。またnu を使う場合は nu-run, markdown を読む場合は nu-run 経由で　md_fetch  つかってください。あと golang の最新版では type parameter は struct のmethod でも対応されたときいてるので、その機能もつかってください。作業はbranch つくってやってください。

## Follow-up instructions

- いきなり実装するよりもいろいろ協議をしてプランたてたほうがいのであればその方針でもいいです。
- `New[string, Value]()` のような API 変更を許容するかという確認に対して：「むしろ指示はそれを目的としてます。」
- いろいろテストベンチも不十分なのでそのあたりの追加も考えてください。
- 後元々のコンセプトが struct にlinked list を埋め込むことで hashmap を実現するってアイデアなんですがそれはキープされてますかね。
- まぁありがとう。もうupdate しようにも大変すぎて投げ出してしまった段階なのでまずはもうすこし安定的な動作にもっていきたいですね。実際に実装をすすめるにあたって git_task でタスク化してすすめるのがいいとおもってるんですが、どういう風に分割したらいいか推奨してください。
- ありがとうでは git_task でタスクに分解してください。それで　新しいセッションで作業をすすめていきます。 docs のほうがadd commit してかまいません。それでどうでしょう？
- あとここではすなおに worktree つくって pr つくる構成ですすめてるつもりだったんですが指定してますか？
- では記載してくだい。たぶん全部のtask にいるかどっかにいれて参照させる？

## Dependency investigation request

この節は旧調査の依頼原文。実施順序と型移行範囲は下記の2026-09-22の追加指示で更新する。

Read-only investigation of elist_head v0.2.8 and the linked list dependency in loncha v0.4.11. Determine the minimum changes needed for Go 1.27.1 checkptr and race safety while retaining relative links and contiguous pool storage. Main-agent baseline: ordinary `go test ./... -timeout 30s` passes; `-race` aborts in elist_head.ListHead.diffPtrTo during InitAsEmpty. Do not run builds, tests, lint, or background commands. Use nu-run for commands and md_fetch through nu-run for Markdown. Always pass a path to rg. Read applicable instructions first.

Write findings yourself to docs/dependency-investigation.md as one numbered list. Include concrete source locations, existing APIs examined, safety implications including GC reachability and relocation, smallest alternatives, and what remains unverified. This is investigation only; do not change implementation or dependencies. The parent is independently examining the map and benchmarks.

## Dependency-first revision (2026-09-22)

- 「全体にかかわる大事な指示をわすれてました。elist_head が interface{} 前提なのでこれを generics 化することが先かなとおもってます。」
- 「そうですね。見直しをしてそれで反映してください。そのあと仕切り直してでいいす 003 自体をすいません。」
- 「まぁ方針として可能な限り interface{} を排除することでやってほしい。あとでもいいけど」

反映: 新規013でelist_headのgenerics化を先行し、その統合後に003を再開する。以後もMap/item/pool/rmap/コールバックと必要な依存の通常経路から、可能な限りinterface{}と型消去としてのanyを除く。型制約のanyは対象外。残す箇所は必要性と後続の除去先、または残存理由を記録する。今回の依頼は計画・タスク更新までで、実装は再開しない。
