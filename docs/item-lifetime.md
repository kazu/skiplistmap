# Map から得た item の寿命

Map から得たポインタがいつまで有効か、誰が item を生かすか(git_task 005)。規則の文は map.go の godoc(`Set`、`StoreItem`、`LoadItem`、`LoadItemByHash`、`SearchKey`、`RangeItem`、`First`、`Last`)にある。ここにはそのきっかけのコード、mode ごとの事実、理由を書く。

## stale read と lost update

デフォルトの `New()` でも `UseEmbeddedPool(true)` でも、下のとおりになる(lifetime_test.go の `Test_LoadItemAfterPoolGrowth` が確かめる)。

```go
m := skiplistmap.New()
m.Set("a", 1)
it, _ := m.LoadItem("a")
for i := 0; i < 100000; i++ {
	m.Set(strconv.Itoa(i), i) // 新しいキーの Set で item が新しい配列へ移る(pool の拡張。埋め込み pool では末尾以外への挿入でも)
}
m.Set("a", 2)
it.Value()     // stale read: 1 を返す
it.SetValue(3) // lost update: m.Get("a") は 2 のまま
```

## mode ごとの事実

| mode | item を生かすもの | item が移る操作 | 削除した slot の再利用 | 先に得たポインタが後で指すもの |
|---|---|---|---|---|
| 埋め込み pool の無い mode すべて(デフォルトの `New()`、skiplistmap4 など) | `Map.pooler` の pool 配列 | 満杯の pool への新しいキーの `Set`。`samepleItemPool._expand` が全 item を新しい配列へ写し、`elist_head.RepaireSliceAfterCopy` で外側の隣をつなぎ直す | しない | 古いコピー |
| skiplistmap5(`UseEmbeddedPool(true)`) | bucket の pool 配列 | 新しいキーの `Set`。pool が満杯なら `expand`、末尾以外への挿入なら `insertToPool`。どちらも全 item を新しい配列へ写す。`insertToPool` は空きがあっても写す | する | 古いコピーか、別のキーの item |
| `StoreItem` だけを使う Map | 呼び出し側 | 無い | しない | 同じ item |

- **公開時点**:
  - 埋め込み pool の無い mode: K と値を入れ、`_set` が reverse と conflict を書いてから list につなぐ。つないだ時点から reader に見える。
  - skiplistmap5: 検索は list を辿らず、配列を二分探索する(`bsearchBybucket`)。`Set` が slot に reverse と conflict を CAS で書いた時点から見つかり、K と値はその後に入る。`foundFree` と `appendLast` で再利用した slot では、その間に前のキーの値が見える(ソースの読みで、未再現)。
  - skiplistmap5 で reader が実在するキーを見逃す窓(ソースの読みで、未再現。006・007):
    - (a) reverse が 0 のまま見える slot がある。`insertToPool` が新しい配列を公開した直後の挿入位置、`foundFree` が 0 にした slot、`appendLast` が len を増やした直後の末尾。
    - (b) `itemSlice.CopyFrom` は data・cap・len を別々に公開する。`bsearchBybucket` は探索の範囲を len の 1 回の読みで決め、data は比較のたびに読み直すので、古い len と新しい data の組を読みうる。
- **旧 reader の寿命**: 先に得たポインタが指す古い配列は、そのポインタが GC から生かすので、解放済みのメモリにはならない。読めるが、Map の今の状態ではない。
- **移動中の更新**: 古いコピーへの書き込みは Map に届かない。今の Map のメソッドにも同じ窓があり、007 で直す。
  - 埋め込み pool の無い mode(skiplistmap4 など): 既存キーの `Set` と `Delete` が `_expand` と排他されていない。
  - skiplistmap5: 既存キーの `Set` と `purgeInEmbedded` は、`muPool` を取った後で検索し直さない。`Delete` はロックを取らない。
- **再利用の条件**: skiplistmap5 だけ。reader を待たずに再利用する。
  - `foundFree`: 新しいキーが reverse の順で入る位置の slot が削除済み(`Delete` か `Purge`)なら、その slot をその場で使う。
  - `appendLast`: `Purge` で末尾から外れた slot を使う。

## 「読んだ時点だけ有効」にした理由

- 「delete まで有効」にするには、item を動かさない配置(固定 chunk)が要る。これは作者の blog([Part3](https://mmap.dev/posts/tuning-skiplistmap3/))で局所性を理由に退けた案なので、採らない。
- 「次の書き込みまで有効」は、書き込みが直列のときだけ成り立つ。並行する writer がいれば「読んだ時点だけ有効」と同じになるので、`LoadItem` の godoc の注記にとどめる。
- 文書だけでは強制できないので、010 で、pool の中の実体へのポインタを呼び出しの外に出さない `Update(key, func(item))` を足して、ライブラリ側で強制する。

## 利用者の struct

`StoreItem` で入れる。例は example_test.go の `ExampleMap_StoreItem`。今の API には次の制約があり、型付きの API は 010 で作る。

- `SampleItem` を埋め込む。`Delete` の印(`mapIsDeleted`)が unexported なので、`MapHead` だけでは `Delete` を書けない。
- `HmapEntryFromListHead` を定義し、`ItemFn` で渡す。
- 同じ Map で `Set` が新しいキーを足すと、`ItemFn` が `SampleItem` 用に書き換わる。
- 埋め込み pool の Map では使えない(`StoreItem` の godoc)。
