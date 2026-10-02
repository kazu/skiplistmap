# 型付きキーとEntryで値の配置を選ぶ

`Entry[K,V]` はキー・値・`MapHead`を持つ。利用者は通常のstructを
`Entry[K,User]`の値として直接格納でき、`Entry[K,*User]`なら外部の実体を参照する。
User自身にリンクやキー取得メソッドを要求しない。値のサイズ制限や、サイズによる
自動的なポインタへの切替は設けない。

task017で公開Map・Entry・options・pool・bucketを型付きにした。
`internal/typedapi`はtask010の配置・復元の試作を残したもので、本体Mapとは別である。
以下は公開APIの仕様であり、検証結果はtask017の記録に分ける。

## 型そのものにキーの条件を置く

```go
type Key[K any] interface {
    KeyHash() (uint64, uint64)
    Equal(K) bool
}

type Entry[K Key[K], V any] struct {
    key K
    value V
    MapHead
}
```

Mapも`K Key[K]`を要求する。Keyの宣言の`K any`は、Equalの引数に同じ型を
書くための型パラメータである。MapやEntryのキーを無制約にするものではなく、
interface値を格納するものでもない。Vも具体的な型で格納する。

Equalがtrueになるキーは同じハッシュ対を返す。異なるキーのハッシュ対が
一致した場合は候補をたどり、Equalで識別する。Equalは同じ型を受け取るため、
`Equal(any)`や型アサーションを利用者に要求しない。

標準の型集合とメソッドinterfaceをunionで結ぶ案はGo 1.27.1でコンパイルできない。
標準・独自の入口を分けてMap/EntryのKをanyにする案も採らない。
標準キーにメソッドを与え、独自キーと同じ制約・入口で扱う。

## 標準キーと独自キー

| 標準キー | 保持する型 | ハッシュ対 | Equal |
|---|---|---|---|
| StringKey | string | MemHashString、xxhash.Sum64String | `==` |
| BytesKey | []byte | MemHash、xxhash.Sum64 | bytes.Equal |
| Uint64Key | uint64 | uint64への変換、0 | `==` |
| ByteKey | byte | uint64への変換、0 | `==` |
| IntKey | int | uint64への変換、0 | `==` |
| Int32Key | int32 | uint64への変換、0 | `==` |
| Uint32Key | uint32 | uint64への変換、0 | `==` |
| Int64Key | int64 | uint64への変換、0 | `==` |

対象とハッシュ対は現在のKeyToHashに合わせる。符号付き整数の負値も従来どおり
uint64へ変換する。標準キーは元の型を基底型とする定義型で、別の箱や追加フィールドを
持たない。型付きの変数には明示的な変換が必要になるが、型が期待される引数位置では
表現可能なリテラルを渡せる。

BytesKeyは元のbyte配列を参照する。Mapへ登録した後にキーの内容を変更しない。
nilと空のbyte列は従来どおり等しく、同じハッシュ対を返す。

独自キーはKeyHashとEqualを実装する。比較不能なフィールドを持つstructでも使える。
事前計算したハッシュ対をキー内に保持して返すこともできる。任意structの生メモリを
一律にbyte列としてハッシュする実装は置かない。ポインタのアドレスやパディングと、
利用者が定義するEqualの一致条件は別だからである。

## 値の配置と寿命

| V | Entry内の配置 | コピーしたとき |
|---|---|---|
| User | Userそのもの | Userの値をコピーする |
| *User | Userへのポインタ | ポインタをコピーし、同じUserを参照する |

入れ子structのフィールドは直接配置されるが、境界のパディングがあるため、
平たく並べたstructとサイズまで必ず一致するとはしない。Userにsliceやpointerが
含まれる場合、値のコピーはそれらの参照先を深くコピーするものではない。

StoreItemで渡すEntryは利用者が生かす。相対リンクはGCの到達可能性を作らない。
削除または更新で退役した同一実体の再登録はfalseになる。`Copy`はリンク・削除履歴を持たない
新しいEntryへキー・値だけを浅くコピーする。`StoreItemOrCopy`は退役による拒否だけをコピーで
再試行し、成功時に格納に使用した元Entryまたはコピーを返す。呼出し側は返された実体も保持する。
同じキーが既にある場合は既存Entryを更新し、渡した実体やコピーはリンクしない。
Deleteだけではリンクから外れないため、削除したという理由だけで保持をやめない。
Mapがpool内の要素を移動・再利用する場合、以前取得したポインタは現在の値を表すとは
限らない。変更はMapのメソッドを通す。保持のためだけの新しいregistryや所有者表は
追加しない。

非embedded modeのEntryと、両modeの外部Entryは、更新ごとに新しいEntryを公開し、rootから更新コピーを保持する。
古いEntryの参照は古い値を指す。保持したrootの更新履歴は自動回収しないため、
更新回数に応じて保持メモリが増える。pool配列を移動した場合もrootの参照を引き継ぐ。

embedded modeのpool要素は更新を別slotにコピーして公開し、旧slotを削除して再利用する。
再利用時には任意のK/Vの読み取りと書き換えが競合するため、再利用対象のEntryだけに
atomicカウンタによる読み取り保護を置く。読み手の終了を待ってpayloadを書き換える。
RWMutexは使わないが、embedded modeの読み取りにはatomic操作が増える。
slotの公開世代・削除状態も再確認する。LoadItemで得たslotを現在値の固定snapshotとして
保持することはできない。キーと値の組はGetまたはRangeで取得する。

外部Entryは `NewEntry[K,V](key,value)` で作るか、所有者のstructや配列内のゼロEntryを
`InitEntry(key,value)` で初期化する。InitEntryは初回だけtrueを返す。
初期化済みEntryを値コピーしない。キーや値の参照先を利用者が書き換える場合の同期は
利用者が行う。Mapはslice、map、pointerの参照先を深くコピーしない。

`internal/typedapi` のEntryサイズ・allocationは本体のコピー保持コストを含まない。

## 型付き本体の公開API

以下は現在の型付き本体の公開APIである。
ハッシュ値のビット反転順と各modeの目的を維持する。

| 操作 | 型付きの定義 |
|---|---|
| 生成 | `New[K Key[K], V any](opts ...OptHMap[K,V]) *Map[K,V]` |
| 値を設定 | `Set(key K, value V) bool` |
| 現在値を更新 | `Update(key K, edit func(*V)) bool` |
| 値を取得 | `Get(key K) (V, bool)` |
| 先頭・末尾の値を取得 | `First() (V, bool)`、`Last() (V, bool)` |
| 主ハッシュによる値の取得 | `SearchKey(hash uint64) (V, bool)` |
| 外部のEntryを登録 | `StoreItem(item *Entry[K,V]) bool` |
| Entryのキーと値を浅くコピー | `(*Entry[K,V]).Copy() *Entry[K,V]` |
| 退役済みならコピーして登録 | `StoreItemOrCopy(item *Entry[K,V]) (*Entry[K,V], bool)` |
| Entryを取得 | `LoadItem(key K) (*Entry[K,V], bool)` |
| 削除 | `Delete(key K) bool`、`Purge(key K) bool` |
| 事前計算ハッシュによる取得 | `GetByHash(hash, conflict uint64) (V, bool)`、`LoadItemByHash(hash, conflict uint64) (*Entry[K,V], bool)` |
| Entry走査 | `RangeItem(func(*Entry[K,V]) bool)` |
| キー・値の走査 | `Range(func(K,V) bool)`、`All() iter.Seq2[K,V]` |
| キーだけ・値だけの走査 | `Keys() iter.Seq[K]`、`Values() iter.Seq[V]` |

GetByHashとLoadItemByHashには検索するキーが無いため、ハッシュ対が一致する要素の
一つを返す従来の意味を保つ。キーを受け取る操作はEqualまで確認する。
embedded poolではLoadItem・LoadItemByHash・RangeItemはpanicする。
値の取得・列挙にはGet・GetByHash・Rangeを使う。First・Last・SearchKeyは両modeで値を返す。
並行する削除や再利用で取得候補が無効になった場合は、ゼロ値とfalseを返す。
探索経路を削除で失った場合も失敗し得る。並行更新中のfalseは対象の不在を保証しない。

OptHMapは現在の関数形式のまま型付きにし、その定義は
`func(*Map[K,V]) OptHMap[K,V]`となる。オプション生成関数のK/Vも明示する。
型付きのMapを無制約のinterfaceへ渡して設定する別経路は導入しない。
Go 1.27.1でもinterfaceのメソッドには型パラメータを付けられないため、
非genericなオプションinterfaceのgeneric methodで適用する案は使えない。
ItemFnによる実体型の復元はEntry[K,V]と型付きListで置き換える。

`Update`は現在値の取得とcallbackによる更新を同じ保護の内側で行う。
task017で接続したSet/_updateに加え、task019で提供する。

All/ValuesはRange、Keysは非公開のentry走査を使う。走査全体のsnapshotを
新たに保証しない。yieldがfalseなら生産側を止める。
KeysはVを取り出さず、不要な値コピーを避ける。
maps.CollectはGo標準mapが受け取れるcomparableなKで使え、slices.Sortedは順序付けできる
Kで使える。BytesKeyや独自Keyすべてが、それらの標準関数で使えるとはしない。

## Updateの契約

`Update(key K, edit func(*V)) bool` は現在値から新しい値を作り、公開する。

Updateは対象を検索して保護できた場合にeditを1回呼び、更新完了時にtrueを返す。
キーが存在しない場合や、並行更新・slot再利用で検索候補を検証できない場合は、editを
呼ばずにfalseを返す。同じキーへの並行Updateでも失敗し得るため、falseはキーの不在を
保証しない。失敗したUpdateは値を変更しない。editが受け取るのは値だけで、キーやリンクを変更する入口にしない。
渡されたポインタはcallback内だけで使用し、保持・返却・別goroutineへの受け渡しをしない。
これは利用規約であり、Goの型システムがポインタの持ち出しを禁止できるとはしない。
同じMapの操作をcallbackから再入しない。Vにslice/map/pointerが含まれる場合、その参照先の
所有権や同期を新たに保証しない。nil callbackはプログラムの誤りとしてpanicする。
現在値の取得から更新までの同期とslotの保護を一体で行う。探索の再試行は
callbackの前に行い、callbackを再実行しない。panic時は保護を解放し、値の巻き戻しは保証しない。
非embeddedでは対象Entryのbusy保護、embeddedでは既存bucket mutexを使う。
公開予定のコピーをcallbackで編集するため、古い外部Entryの値は変更しない。
embeddedのpool slotでは既存のreader保護も使い、編集中の値の読み取りを防ぐ。

## 型付き実体の復元とgeneric method

本体のnewEntryListは既存のelist_head.List[Entry[K,V]]を使う。EntryのMapHead位置と
MapHead内のListHead位置を合わせたoffsetで、リンクから元のEntryへ戻る。
K/Vのサイズを固定せず、値・ポインタ・大きいstructを同じ規則で扱う。

本体のgeneric method `MapHead.recoverEntry[K,V]` は型付きListによって実体を復元する。
本体の探索では、front/tail sentinelとdummyを確認してから実Entryへ戻す。
bare ListHeadやbucketのMapHeadを、Entryの内部とみなして逆変換しない。

試作のRecoverField[F any]はgeneric methodであり、直接格納したV内のフィールドから
外側のEntryへ戻ることを検証する。引数は実Entry内のフィールドのアドレスと、V内の
正しいfield offsetでなければならない。Value()で返したコピーのフィールドや、
Vが指す別allocation内のフィールドからEntryへは戻らない。この低水準機能の
公開名・入口はまだ確定していない。

## 本体の接続点

| 現在の関数・型 | 型付き化で行うこと |
|---|---|
| Map、OptHMap、New、NewHMap | K/Vを型引数で運び、オプションも同じMap型を受ける |
| entryPayload、entryHMap、copyEntry | key/valueをK/Vにし、コピー公開とrootからの保持を維持する |
| KeyToHash、equalKeys、equalItemKey | キー自身のKeyHash/Equalへ接続し、通常経路のtype switchとreflect比較を除く |
| mapheadFromLListHead | MapHeadへの復元は維持し、実Entryへ戻す場所でEntryView相当の型付き復元を使う |
| matchEntry、matchNeighbors、readMatchingEntry | 同じハッシュ対の候補もEqualで判別し、既存の公開世代・削除・移動確認を保つ |
| Set、_update、storeKeyValue、loadKeyValue | Vの公開とpool移動を型付きにする。Vへ単純に非atomic代入する置換では済ませない |
| samepleItemPool、Pool、bucket、bucketFromPool | slotの型・stride・sliceの復元をK/Vに合わせ、配列の公開世代と連続配置を保つ |
| StoreItem、setItem | 呼び出し側のEntryをコピーせずに登録し、pool由来のitemの再登録を引き続き拒否する |
| bsearchBybucket、getItemWithBucket | poolの二分探索と隣接するリンク区間を使い、外部Entryとの混在も検索する |
| lockFoundItem、deleteItem、purgeItem | owner bucket・slot・世代の再確認を保ち、外部Entryをpool slotと取り違えない |
| RangeItem、Range、First、Last | 内部のEntry走査を共有し、sentinel/dummyを返さない。First/Lastは値を返し、embeddedのRangeItemは利用不可。All/Keys/Valuesを接続する |

## 後続タスクの範囲

StoreItemはembedded poolの要素と外部Entryを混在させられる。外部Entryの実体を
pool配列へコピーせず、配列の移動ではpool要素だけを置換する。混在専用の性能測定は
task012で行う。Update callback APIはtask019で接続した。

rmapは本体に接続する部分だけを型付きにした。read/dirty/callbackの型消去と
測定器全体の移行はtask012に残す。

残した型消去は次のとおり。

| 箇所 | 理由・後続 |
|---|---|
| `KeyToHash(interface{})` | rmapの既存入口と010の比較fixtureが使用する。本体MapはK.KeyHashを直接呼ぶ。rmapの移行は012 |
| rmapのSet/Get、storedValue内のatomic.Value | rmapの公開APIとread側の型移行は012。dirty/frozenだけMap[StringKey,*readSlot]へ接続 |
| rmapのonNewStores | 既存callbackのMapItem[StringKey,any]を維持。012で利用者側と一緒に変更 |
| Log、DumpExpandInfoの可変引数 | fmtへ渡す診断用の異種引数。MapのK/V保存・比較には使用しない |

HMapEntry、MapItem、SampleItem、entryHMap、copyEntryは型付きEntryの別名であり、
interfaceへの変換は行わない。ItemFnは削除した。型パラメータの制約としてのanyと、
利用者が明示的にVとして選んだanyは、内部でK/Vを型消去する経路とは区別する。

First/Lastはsentinelやdummyを除いた先頭・末尾の値を返す。
空の場合や並行削除・再利用で候補が無効になった場合は、ゼロ値とfalseを返す。

## 試作で確認する範囲

標準8種類のハッシュ対、空・nilのbyte列の同一性、同じハッシュ対を持つ独自キーの
Equal、値とポインタの格納、異なるサイズ・alignmentのEntry、GCを挟んだリンク復元、
フィールドからの復元、iteratorの順序・早期終了・標準ライブラリとの接続を検証する。
メソッドを持たないstring、KeyHashが無い型、Equalがanyを受ける型はコンパイルで拒否する。

キーのハッシュ計算と実体復元のbenchは、interface変換とallocationの影響を切り分ける
試作である。本体Mapの性能比較、poolの並行性、E3やUpdateの完了を示すものではない。
