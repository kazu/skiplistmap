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
| 埋め込み pool の無い mode すべて(デフォルトの `New()`、skiplistmap4 など) | `Map.pooler` の pool 配列 | 満杯の pool への新しいキーの `Set`。`samepleItemPool._expand` が `elist_head.FreezeSlice` でリンクを固定してつながった item のデータを写し、`SliceMove.Relink` でつなぎ直す | しない | 古いコピー |
| skiplistmap5(`UseEmbeddedPool(true)`) | bucket の pool 配列 | 新しいキーの `Set`。pool が満杯なら `expand`、末尾以外への挿入なら `insertToPool`。どちらも全 item を新しい配列へ写す。`insertToPool` は空きがあっても写す | する | 古いコピーか、別のキーの item |
| `StoreItem` だけを使う Map | 呼び出し側 | 無い | しない | 同じ item |

- **公開時点**:
  - 埋め込み pool の無い mode: K と値を入れ、`_set` が reverse と conflict を書いてから list につなぐ。つないだ時点から reader に見える。
  - skiplistmap5: `bsearchBybucket` が配列を二分探索し、`readMatchingEntry` がリンク・削除状態・ハッシュ対と、キーを指定した操作では実キーを確認する。`appendLast` と `insertToPool` は公開前に reverse を設定する。`foundFree` で再利用する slot は、新しい K と値の準備が済むまで削除印を残す。`Purge` 後の slot はリンクも初期化されている。
  - skiplistmap5 の配列置換は `publishItems` が世代番号を進めて data・cap・len を公開する。`bsearchBybucket` は公開途中、または検索中に世代が変わった場合に検索し直す。新しい配列と古い長さを組み合わせた取りこぼしは `Test_EmbeddedSearchDuringSlicePublication` で検証する。
- **旧 reader の寿命**: 先に得たポインタが指す古い配列は、そのポインタが GC から生かすので、解放済みのメモリにはならない。読めるが、Map の今の状態ではない。
  - `Get` と `GetByHash` は slot のキー・値の公開世代を確認してから結果を返す。`Range` も同じ世代のキーと値を callback に渡す。`Range` 全体のスナップショットは保証しない。`Len` は更新中には途中の件数を返し得るが、更新完了後には正確な件数を返す。
  - `KeyToHash` の対応キーと比較可能な独自キーは実キーを比較し、同じハッシュ対でも別キーの item が共存できる。string と []byte は内容を比較し、対応する整数型は従来の数値の正規化を維持する。独自 `KeyHash` だけで扱うその他の比較不能なキーは、従来のハッシュ対による同一性を使う。実キーを受け取らない `GetByHash` と `LoadItemByHash` は、その対に一致する item の一つを返す。
  - `NewEntryMap` の item は、削除してもキーを変更しない。値は atomic に公開し、nil や異なる型の値への更新も受け付ける。
- **移動中の更新**: 利用者が古いコピーへ直接書いても、Map の現在の item には届かない。変更は Map のメソッドを通す。
  - 埋め込み pool の無い mode(skiplistmap4 など): `_update` は値を移動先にも書き、`deleteItem` は移動元・移動先へ削除状態を反映する。`FreezeSlice` が固定するのはリンクで、値や `MapHead.state` への書き込みを禁止するものではない。
  - skiplistmap5: `Set` は `muPool` を取ってから検索する。`Purge` と `Delete` は `lockFoundItem` でロック取得後のキー・枠・所有 bucket を確認し、確認できなければ検索し直す。削除処理中もロックを保持するため、その間に配列を移動したり枠を再利用したりしない。
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
