# Mapから得たEntryの寿命

Mapから得たEntryのポインタは、取得後もMapの現在値を指し続けるとは限らない。
`UseEmbeddedPool(true)`では`LoadItem`・`LoadItemByHash`・`RangeItem`はpanicする。
値の取得には`Get`・`GetByHash`、列挙には`Range`・`All`・`Keys`・`Values`を使う。
非embeddedではこの3つのEntry取得APIを使える。
公開の`First()`・`Last()`・`SearchKey(hash)`は両modeで`(V, bool)`を返す。
`First`・`Last`の順序は`Range`と同じ。`SearchKey`は主ハッシュだけが一致する値の一つを返す。
並行する削除やslot再利用で候補を検証できなくなった場合は、ゼロ値とfalseを返す。
また、探索途中のノードが削除されて経路を失った場合も取得は失敗し得るため、
並行更新中のfalseはmap内に対象が存在しないことの保証ではない。
内部のentry取得は非公開の`first`・`last`・`searchKey`で行う。
規則は`Set`、`StoreItem`、`LoadItem`、`LoadItemByHash`、`SearchKey`、`RangeItem`、
`First`、`Last`のgodocにある。ここでは型付きMapの事例、modeごとの保持・移動・再利用を説明する。

## stale readと古いEntryへの書込み

stale readは、pool拡張前に得たEntryから古い値を読むと起きる。
次の例はデフォルトの非embedded poolで、`m.Set("a", 2)`後も
古いEntryから1を読む。`Test_LoadItemAfterPoolGrowth`も同じ操作を確認する。

```go
package main

import (
    "fmt"
    "strconv"

    "github.com/kazu/skiplistmap"
)

func main() {
        m := skiplistmap.New[skiplistmap.StringKey, int]()
        m.Set("a", 1)
        entry, _ := m.LoadItem("a")
        for i := 0; i < 100000; i++ {
            m.Set(skiplistmap.StringKey(strconv.Itoa(i)), i)
        }
        m.Set("a", 2)
        fmt.Println(entry.Value())
        entry.SetValue(3)
        value, ok := m.Get("a")
        fmt.Println(value, ok)
}
```

出力は古い値1と現在値2になる。`SetValue`はリンク済みEntryへの書込みを
拒否する。古いコピーが未リンクになっていて変更できても、Mapの現在値を更新する
入口としては使えない。変更には`Map.Set`を使う。

## modeごとの保持・移動・再利用

| mode | Entryを生かすもの | 移動・更新 | 削除slotの再利用 | 先に得たポインタ |
|---|---|---|---|---|
| 非embedded pool（デフォルト、skiplistmap4など） | Mapのpool配列。更新コピーはrootから保持する | pool拡張で配列内のEntryを移動する。既存キーの更新は新しいEntryを公開する | しない | 古い配列内のコピー、または古い更新版を指す |
| embedded pool（skiplistmap5） | bucketのpool配列 | 配列拡張・途中挿入で移動する。既存キーの更新は別slotへコピーして公開する | する | Mapの公開検索・列挙APIからはEntryを取得せず値を取得する |
| 両modeへStoreItemした外部Entry | 呼び出し側。更新コピーはrootから保持する | 登録した外部実体自体は動かない。更新後のMapは公開コピーを参照し得る | 外部実体はpool slotとして再利用しない | 同じ外部実体を指すが、現在値の固定snapshotとは扱わない |

非embeddedのEntryと両modeの外部Entryはコピー履歴を自動回収しないため、更新回数に応じて保持量が増える。
型付きの値コピーはslice・map・pointerの参照先を深くコピーしない。その参照先を
利用者が変更するときの同期は利用者が行う。

## 公開と再利用の同期

- 新しいEntryのK/Vを初期化してから公開する。検索では公開世代、削除状態、ハッシュ対と、キーを受け取る操作では`K.Equal`を確認する。比較不能な独自キーにも同じ`KeyHash`/`Equal`の条件を使う。`GetByHash`はハッシュ対が一致する値の一つを返す。非embeddedの`LoadItemByHash`は対応するEntryを返す。
- embeddedの配列置換では`publishItems`が公開世代を進める。検索中に配列やslotの世代が変われば検索し直す。`Get`と`GetByHash`は確認した値、`Range`は同じ世代のキーと値を返す。走査全体のsnapshotは保証しない。`Len`は更新中に途中の件数を返し得るが、更新完了後は正確な件数を返す。
- embeddedのslot再利用では、atomic reader pinを持つ読み手が終わるのを待ち、書き手がslotを確保してからK/Vを書き換える。`Value()`と`Key()`を別々に呼んだ結果の組は固定snapshotではない。組として読むには`Get`または`Range`を使う。
- embeddedの書込みは既存のbucket/poolのロックとowner・slot・世代の再確認を使う。`Delete`/`Purge`も取得後の対象を再確認する。非embeddedの更新は新Entryのコピー公開を使う。全EntryへRWMutexを追加する方式ではない。
- 再利用する空きslotはreverseの順序を保って選ぶ。同じreverseの連続部分と直前の削除slotも候補にする。末尾では`Purge`で外れたslotを使える。新しいK/Vの準備と公開前の削除状態を維持し、旧読み手との競合を防ぐ。
- 古い配列へのポインタが生きていれば、その配列はGCからも生きている。ただし、そのEntryがMapの現在値であることは意味しない。

## embeddedのLoadItemを禁止する理由

- 005で検討した「deleteまで有効」のための固定chunkは、作者の[Part3](https://mmap.dev/posts/tuning-skiplistmap3/)で局所性を理由に退けた配置なので採らない。
- 「次の書込みまで有効」は書込みが直列のときの説明であり、並行する書き手がいれば取得直後にも状態が変わり得る。
- 検索と`Entry.Value()`が別操作だと、その間の更新で古いEntryの値が消去・再利用され得る。embeddedでは`LoadItem`・`LoadItemByHash`を禁止し、取得と値の確認を行う`Get`・`GetByHash`を使う。
- `Update(key K, edit func(*V)) bool`は現在値のコピーをcallbackで編集して公開する。値のポインタはcallback内だけで使い、保持・返却・別goroutineへの受け渡しや同じMapへの再入をしない。これは利用規約であり、Goの型システムが持出しを防ぐとはしない。値に含まれるslice/map/pointerの参照先の所有権や同期は変わらない。

## 利用者のstructと外部Entry

通常のUserは`Entry[K, User]`のVへ直接格納でき、`Entry[K, *User]`なら外部を参照する。
User自身にリンクや取得メソッドを要求しない。呼出し側のstructに`Entry[K,V]`を埋め込む場合は、
ゼロEntryへ`InitEntry`する。単独のEntryなら`NewEntry`でも作れる。初期化済みEntryは値コピーしない。

外部Entryは`StoreItem`で登録し、呼出し側が実体を保持する。`ItemFn`や
`HmapEntryFromListHead`は不要。実行例は`ExampleMap_StoreItem`。
embedded poolの要素とも混在できる。配列が移動しても外部Entryは同じアドレスに残る。
混在の実行例は`ExampleMap_StoreItem_embedded`。
`Delete`だけでリンクから外れたとは扱わず、削除したという理由だけで保持をやめない。

削除または更新で退役した外部Entryは、Purgeやリンク初期化後も同じ実体を再登録できない。
`Entry.Copy`はキーと値を浅くコピーした新しいEntryを返し、リンク・削除履歴・更新履歴を引き継がない。
外側のstructはコピーしない。外側のstructへ埋め込む場合は、その新しいゼロEntryを`InitEntry`する。
`StoreItemOrCopy`は削除履歴による拒否の場合だけコピーして格納を試す。成功時に返す元Entryまたは
コピーは呼出し側が保持し、元mapに必要な古いrootの保持も続ける。失敗時はnilとfalseを返す。
同じキーが既にある場合はそのEntryを更新するため、返すEntryが`LoadItem`の結果と一致するとは限らない。
実行例は`ExampleEntry_Copy`と`ExampleMap_StoreItemOrCopy`。
