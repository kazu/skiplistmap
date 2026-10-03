# 013: 終端判定を除いた型付き操作の再測定

## 修正とレビューの区別

旧版の固定レビュー対象は親f24fcc6・依存8785631。別エージェントのレビューはcomment141に登録され、型付き走査の性能と依存側の検査一次ログ不足により完了承認は出なかった。以前のcomment129はベンチ比較基盤だけの承認であり、List[T]実装の承認ではない。

この修正はそのレビューとは別の版。ユーザーの指示で旧版レビュー中に着手した。旧版は固定コミットで評価され、著者の修正案やユーザーの指摘をレビュー入力にしないよう依頼した。修正版の再レビューは別に記録する。

## 実装

型付き実装の修正時の測定コミットは159504182307055412d109ea84b8b70c47dd993b。下の表は、ベンチを名前付きb.Runへ分けたa4e7e7ff4f1ac31d8b58469e2a2833562756751cで取り直した最新の値。

その後c4a33faでREADMEだけを修正した。元の英語の説明と手書き操作例を残し、List[T]の説明を英語で追記している。実装・ベンチ本体は1595041から変わっていない。手書き例と型付き例のcheckptr付き実行記録はdocs/benchmarks/task-013-readme-examples.nuon。

a4e7e7fではReadme/Typedをboolで切り替えるベンチ構造を廃止し、4種類の名前と測定コードを直接対応させた。README冒頭に2つの利用例への案内を加え、`Using List[T]` 節に操作の対応を明記した。ライブラリ本体は1595041から変更していない。新しいベンチを含む通常unitは成功し、交互測定・profile・24ケースの確認を取り直した。以前の測定結果も削除せず保存している。

- List[T]はoffsetだけを持つ値型。終端2本と、Elementによるnil・終端チェックを削除した。amd64のviewサイズは24Bから8Bに減少し、ListHeadは16B、SampleEntryは40Bのまま。
- NewList[T](offset)、Link、Element、Next/Prev、DirectNext/DirectPrev、InsertBeforeを提供する。Next/Prevは変換とraw移動を短い本体に記述し、余分なgenericメソッド呼出によるインライン制限を避ける。
- 終端もT内のListHeadとし、呼び出し側が元のREADME例と同じpointer比較で停止する。First/Lastと終端nil化は削除。Link/Elementへnilを渡す契約も削除した。空listではheadからNextするとtailが返る。
- List[T]はpointerを持たず、要素の生存・同期・コピー修復は呼び出し側の責務。Owner、slice registry、要素検索は追加しない。raw ListHeadのリンク処理は今回の修正で変更していない。
- 親のmapheadFromLListHeadでは既存のnil返却を明示的な分岐で維持し、その後NewList[MapHead](mapheadOffset).Elementを使う。Mapの走査全体をList.Nextへ置換してはいない。

終端判定除去だけの試作ではNextがインライン予算85/80となり遅延が残った。短いNext/Prev本体に変更して改善した。DirectNextの相対距離処理を共有する試作も測ったが、動的offset時の差は解消せずraw側の変更を増やすため撤回した。最終版のraw処理は元のレビュー対象と同じ。

再レビューで変換処理の共有可否を確認するため、固定1595041の別worktreeでNext/Prevだけを `return l.Element(l.Link(v).Next())` / Prevへ変更した。value receiverでもNextはcost85で予算80を越え、同一binaryの1024 WalkはReadme中央値2167ns、共有形Typed中央値2520nsとなった。正確な差分・コマンド・出力・終了コードはtask-013-offset-sharing-check.nuon。短いNext/Prev本体はこの呼出増を避けるため残す。元の作業worktreeはこの試作で変更していない。

## 同一binaryの比較

### ベンチの実装ファイルと実行方法

作業worktreeは `/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013`。

- この表のReadme/Typed/RuntimeOffset/Local比較は [deps/elist_head/list_bench_test.go](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/deps/elist_head/list_bench_test.go) の `BenchmarkList`。同じファイルの `BenchmarkListView` がviewのサイズと構築allocationを測る。
- 親MapHeadの復元比較は [maphead_bench_test.go](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/maphead_bench_test.go) の `BenchmarkMapHeadRecovery`。
- 4方式の交互測定・profile保存・集計は [tools/elist-perf/typed.nu](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/tools/elist-perf/typed.nu)。
- 原版とのraw操作比較は [migration_bench_test.go](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/deps/elist_head/migration_bench_test.go)、コピー修復は [repair_bench_test.go](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/deps/elist_head/repair_bench_test.go)、コピー単独は [copy_bench_test.go](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/deps/elist_head/copy_bench_test.go)。これらは下の4方式の表とは別のベンチ。

4方式の時間・allocationだけを確認するコマンド（Nushell）:

```nu
cd /mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/deps/elist_head
with-env {GOTOOLCHAIN: go1.27.1} {
    go test -run '^$' -bench '^BenchmarkList$|^BenchmarkListView$' -benchmem -benchtime 300ms -count 3 -cpu 1
}
```

保存済みの最新の数値は [task-013-named-summary.nuon](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/docs/benchmarks/task-013-named-summary.nuon)、実行出力は [task-013-named-typed.txt](/mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013/docs/benchmarks/task-013-named-typed.txt)。binaryとprofile本体は `/tmp/elist-013-named-final/` に保持している。交互測定とprofileの再現コマンドは下の「検証・再現」に記載する。

### 比較しているコード

4列とも同じ `SampleEntry` 配列をたどり、各要素の `Age` を合計する。違いは、型付きポインタへの変換をどこに書くかと、offsetを定数として使えるかにある。現在のソースでは4種類をそれぞれ名前付きの `b.Run` に分けた。`b.Run(mode)` やboolによる切替はない。

結果名 `BenchmarkList/1024/Typed/Walk` は、次の入れ子の `Typed` → `Walk` に対応する。以下のコードは実ソースのWalk部分を示し、隣のDirectWalkなどのブロックだけを省略している。`b.Run("Typed/Walk")` という別の構造へ書き換えて説明しない。

```text
BenchmarkList
└─ b.Run(fmt.Sprint(n))         // "16" または "1024"
   ├─ b.Run("Local")           // list_bench_test.go:44
   │  ├─ b.Run("DirectWalk")
   │  └─ b.Run("Walk")          // Nextのループは64行目
   ├─ b.Run("RuntimeOffset")   // 74行目
   │  ├─ b.Run("DirectWalk")
   │  └─ b.Run("Walk")          // Nextのループは94行目
   ├─ b.Run("Readme")          // 104行目
   │  ├─ b.Run("DirectWalk")
   │  ├─ b.Run("Walk")          // Nextのループは122行目
   │  ├─ b.Run("EarlyStop")
   │  └─ b.Run("Recover")
   └─ b.Run("Typed")           // 147行目
      ├─ b.Run("DirectWalk")
      ├─ b.Run("Walk")          // Nextのループは165行目
      ├─ b.Run("EarlyStop")
      └─ b.Run("Recover")
```

行番号は依存a4e7e7fのもの。全体は上のリンク先 `list_bench_test.go` にあり、配列の準備は4方式共通、最後に `runtime.KeepAlive(entries)` を呼ぶ。

共通の準備は次の形。先頭と末尾は終端で、データはその間のn個。

```go
n := 1024 // 16要素でも同じ比較をする
entries := make([]elist.SampleEntry, n+2)
head, tail := &entries[0].ListHead, &entries[n+1].ListHead
elist.InitAsEmpty(head, tail)
for i := 1; i <= n; i++ {
    entries[i].Age = i
    if _, err := tail.InsertBefore(&entries[i].ListHead); err != nil {
        panic(err)
    }
}
```

**Readme：要素型ごとに手書きしたNextを呼ぶ。** offsetは型ごとの定数。ソースの `b.Run("Readme")` の中の `b.Run("Walk")` がこれ。

```go
b.Run("Readme", func(b *testing.B) {
    b.Run("Walk", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            sum := 0
            for p := entries[0].Next(); p != &entries[n+1]; p = p.Next() {
                sum += p.Age
            }
            benchSum = uint64(sum)
            if sum != n*(n+1)/2 {
                b.Fatal(sum)
            }
        }
    })
})
```

**Typed：外で作ったList[T]を計測関数へ渡す。** 各要素型にNextを書く代わりに `view.Next(p)` を呼ぶ。viewは `b.Run` の外で構築され、計測関数が取り込む。この条件では保持されたoffsetが実行時の値になる。

```go
view := elist.NewList[elist.SampleEntry](
    unsafe.Offsetof(elist.SampleEntry{}.ListHead))
b.Run("Typed", func(b *testing.B) {
    b.Run("Walk", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            sum := 0
            for p := view.Next(&entries[0]); p != &entries[n+1]; p = view.Next(p) {
                sum += p.Age
            }
            benchSum = uint64(sum)
            if sum != n*(n+1)/2 {
                b.Fatal(sum)
            }
        }
    })
})
```

**RuntimeOffset：List[T]を使わず、offsetを引数で渡す。** Typedとの差が型付きヘルパー固有なのか、実行時offsetを使うことによるものなのかを比較するための対照。

```go
// 定数ではなく、package変数から実行時のoffsetを読む。
var listBenchOffset = unsafe.Offsetof(elist.SampleEntry{}.ListHead)

func readmeNextWithOffset(v *elist.SampleEntry, offset uintptr) *elist.SampleEntry {
    h := (*elist.ListHead)(unsafe.Add(unsafe.Pointer(v), offset)).Next()
    return (*elist.SampleEntry)(elist.ElementOf(unsafe.Pointer(h), offset))
}

b.Run("RuntimeOffset", func(b *testing.B) {
    b.Run("Walk", func(b *testing.B) {
        offset := listBenchOffset
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            sum := 0
            for p := readmeNextWithOffset(&entries[0], offset); p != &entries[n+1]; p = readmeNextWithOffset(p, offset) {
                sum += p.Age
            }
            benchSum = uint64(sum)
            if sum != n*(n+1)/2 {
                b.Fatal(sum)
            }
        }
    })
})
```

**Local：計測関数の中でList[T]を作る。** APIはTypedと同じ。構築場所を変えることで、コンパイラがoffsetを定数として走査へ伝えられる条件を測る。要素配列を作り直すわけではない。

```go
b.Run("Local", func(b *testing.B) {
    b.Run("Walk", func(b *testing.B) {
        local := elist.NewList[elist.SampleEntry](unsafe.Offsetof(elist.SampleEntry{}.ListHead))
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            sum := 0
            for p := local.Next(&entries[0]); p != &entries[n+1]; p = local.Next(p) {
                sum += p.Age
            }
            benchSum = uint64(sum)
            if sum != n*(n+1)/2 {
                b.Fatal(sum)
            }
        }
    })
})
```

`DirectWalk` も同じ終端比較とAge合計を行うが、移動に `DirectNext` を使う。ReadmeとRuntimeOffsetの1ステップは以下。Typed/Localではそれぞれ `view.DirectNext(p)` / `local.DirectNext(p)` を呼ぶ。

```go
// Readme: フィールド位置は型ごとの定数。
p = elist.SampleEntryFromListHead(p.ListHead.DirectNext())

// RuntimeOffset: 実行時offsetを使って手書きで復元する。
func readmeDirectNextWithOffset(v *elist.SampleEntry, offset uintptr) *elist.SampleEntry {
    h := (*elist.ListHead)(unsafe.Add(unsafe.Pointer(v), offset)).DirectNext()
    return (*elist.SampleEntry)(elist.ElementOf(unsafe.Pointer(h), offset))
}
p = readmeDirectNextWithOffset(p, offset)
```

ほかの操作名は次の意味。これらはReadme/Typedの2種類で測る。

| 操作 | Readme側のコード | Typed側のコード | 1回の操作 |
|---|---|---|---|
| EarlyStop | `entries[0].Next()` | `view.Next(&entries[0])` | 先頭要素を取得して止まる |
| Recover | `elist.SampleEntryFromListHead(&entries[i%n+1].ListHead)` | `view.Element(&entries[i%n+1].ListHead)` | 埋め込まれたListHeadから外側の要素を復元する |

`BenchmarkListView` は走査ではなく `NewList[SampleEntry](unsafe.Offsetof(SampleEntry{}.ListHead))` の構築とサイズを測る。`B/view` は保持サイズ、`B/op` と `allocs/op` は操作ごとのヒープ確保であり、別の指標。

### 測定結果

Go 1.27.1、linux/amd64、Ryzen 9 8945HS、cpu=1、各300msを交互順で3回、中央値。profileは別実行で各2s。16/1024要素の全24ケースが各3回あることをscriptが検査する。下記の時間は1回の全走査（Age合計と結果検査を含む）であり、Next 1回の時間ではない。

| 要素数・操作 | Readme | Typed | RuntimeOffset | Local |
|---|---:|---:|---:|---:|
| 16 Next走査 | 31.14 ns | 31.31 ns | 31.67 ns | 31.46 ns |
| 1024 Next走査 | 2129 ns | 2147 ns | 2153 ns | 2130 ns |
| 16 DirectNext走査 | 11.18 ns | 19.07 ns | 18.88 ns | 11.60 ns |
| 1024 DirectNext走査 | 1245 ns | 1639 ns | 1637 ns | 1167 ns |

Readmeは型ごとの手書きメソッド、Typedは外側で作ったviewをbenchmark closureへ渡す従来からのケース。RuntimeOffsetは実行時offsetを渡す手書き変換。Localは計測関数内でunsafe.Offsetofからviewを構築し、offsetの定数伝播が可能なケース。Localだけへの差替えはせず、遅いTyped条件も残した。

Nextの旧測定2170→2549ns（約17%増）に対し、今回の同一binaryでは2129→2147ns（約0.8%差）。ただし「どの呼び出し方でも速度低下なし」とは結論しない。動的offsetのDirectNextには1024要素で約32%、16要素で約71%の差が残る。RuntimeOffsetでも同程度なのでgeneric wrapperだけが原因とは言えない。ローカル構築と動的offset保持の違いを隠さず、利用側が判断できる資料とする。Localの小さな増減を一般的な高速化と主張しない。

上記操作とview構築は0 B/op・0 allocs/op。これは操作ごとのヒープ確保であり、viewの保持サイズ8Bや要素配列の確保が消えるという意味ではない。実際のescapeは呼び出し方に依存する。

## 検証・再現

依存1595041の通常unit、対象race、対象checkptr=2、vetは全てexit0。コマンド・revision・toolchain・stdout/stderr・終了コードを保存した。race/checkptr対象はTestList、TestSampleEntry、TestLegacy、ExampleList。全legacy経路の安全性を保証する範囲ではない。

親Mapには旧版レビュー時点でunit/race/checkptr/vetの失敗記録がある。原版でも通常テストのSIGSEGVを再現したが、停止位置が異なるため同じ根本原因や新規失敗なしとは断定しない。今回の結果も過去の失敗を置き換えて隠さず、別記録で残す。

今回の親通常unitはexit0。MapHead復元の通常benchmarkはOriginal中央値1.358ns、List中央値1.359nsで両方0alloc。checkptr=2での復元benchmarkも完走したが、計装ありの時間は通常性能比較に混ぜない。READMEのmainを抽出して実行し、manual/typedともalpha、betaの順で出力した。親全体のrace/checkptr/vetを解決したという意味ではない。

元版からのproduction Go有効行数（test・空行・行コメントを除く）は親3696→3700（+4）、依存821→841（+20）。今回の修正前と比べると依存は849→841で8行減った。

```nu
cd /mnt/bcachefs/xtakei/host/all/git/github.com/kazu/skiplistmap-task-013
nu tools/elist-perf/typed.nu deps/elist_head /tmp/elist-013-named-final --benchtime 300ms --profiletime 2s
```

最新の実行binary・profile・生ログは/tmp/elist-013-named-final。リポジトリにはdocs/benchmarks/task-013-named-*として保存する。前回の/tmp/elist-013-offset-finalとdocs/benchmarks/task-013-offset-*は当時の記録として保持する。最初の/tmp/elist-013-offset-typedは実行時間上限で中断したため完成結果として使用しない。

## 申し送り

埋め込みリストの高速な経路へ、便利さのための毎回の終端検査や非インライン呼出を追加しない。型付き補助の目的は型ごとの手続きを減らすことで、所有・要素保管・寿命管理をelist_headへ移すことではない。0allocと追加保持メモリゼロを混同しない。速度は終端契約とoffsetの伝播条件を揃えて評価し、速い条件だけを採用して遅い条件を隠さない。

現在の独立レビュー状態と完了判断はgit_taskを正とする。人間の確認・統合・doneを代行しない。依存コミットはローカルにのみあり、統合判断までworktreeとsubmodule object storeを保持する。
