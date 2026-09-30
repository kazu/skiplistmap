# 型付きキーとEntryで値の配置を選ぶ

`Entry[K,V]` はキー・値・`MapHead`を持つ。利用者は通常のstructを
`Entry[K,User]`の値として直接格納でき、`Entry[K,*User]`なら外部の実体を参照する。
User自身にリンクやキー取得メソッドを要求しない。値のサイズ制限や、サイズによる
自動的なポインタへの切替は設けない。

この文書はtask010の設計と検証範囲を示す。`internal/typedapi`は制約・配置・
実体復元・iteratorを実行する試作であり、別のMap実装ではない。
本体の公開APIはまだ型付きへ移行していない。

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
Deleteだけではリンクから外れないため、削除したという理由だけで保持をやめない。
Mapがpool内の要素を移動・再利用する場合、以前取得したポインタは現在の値を表すとは
限らない。変更はMapのメソッドを通す。保持のためだけの新しいregistryや所有者表は
追加しない。

既存NewEntryMapのコピー公開経路は、呼び出し側が保持するrootから新しいコピーを
保持し、古い参照が古い値を指す形である。型付きEntryへの移行でもこの寿命を保つ。
試作NewEntryは配置の検証用であり、このコピー公開・保持処理は実装していない。
したがって試作のEntryサイズやallocationは、完成Map全体の保持コストではない。

## 011へ渡す公開API定義

以下を型付き本体へ接続する。現在呼べる公開APIではなく、011の実装仕様である。
ハッシュ値のビット反転順と各modeの目的を維持する。

| 操作 | 型付きの定義 |
|---|---|
| 生成 | `New[K Key[K], V any](opts ...OptHMap[K,V]) *Map[K,V]` |
| 値を設定 | `Set(key K, value V) bool` |
| 値を取得 | `Get(key K) (V, bool)` |
| callback内で値を更新 | `Update(key K, edit func(*V)) bool` |
| 外部のEntryを登録 | `StoreItem(item *Entry[K,V]) bool` |
| Entryを取得 | `LoadItem(key K) (*Entry[K,V], bool)` |
| 削除 | `Delete(key K) bool`、`Purge(key K) bool` |
| 事前計算ハッシュによる取得 | `GetByHash(hash, conflict uint64) (V, bool)`、`LoadItemByHash(hash, conflict uint64) (*Entry[K,V], bool)` |
| Entry走査 | `RangeItem(func(*Entry[K,V]) bool)` |
| キー・値の走査 | `Range(func(K,V) bool)`、`All() iter.Seq2[K,V]` |
| キーだけ・値だけの走査 | `Keys() iter.Seq[K]`、`Values() iter.Seq[V]` |

GetByHashとLoadItemByHashには検索するキーが無いため、ハッシュ対が一致する要素の
一つを返す従来の意味を保つ。キーを受け取る操作はEqualまで確認する。

OptHMapは現在の関数形式のまま型付きにし、その定義は
`func(*Map[K,V]) OptHMap[K,V]`となる。オプション生成関数のK/Vも明示する。
型付きのMapを無制約のinterfaceへ渡して設定する別経路は導入しない。
Go 1.27.1でもinterfaceのメソッドには型パラメータを付けられないため、
非genericなオプションinterfaceのgeneric methodで適用する案は使えない。
ItemFnによる実体型の復元はEntry[K,V]と型付きListで置き換える。

Updateはキーが存在するときだけeditを1回呼び、更新完了時にtrueを返す。存在しなければ
呼ばずにfalseを返す。editが受け取るのは値だけで、キーやリンクを変更する入口にしない。
渡されたポインタはcallback内だけで使用し、保持・返却・別goroutineへの受け渡しをしない。
これは利用規約であり、Goの型システムがポインタの持ち出しを禁止できるとはしない。
同じMapの操作をcallbackから再入しない。Vにslice/map/pointerが含まれる場合、その参照先の
所有権や同期を新たに保証しない。nil callbackはプログラムの誤りとしてpanicする。
011では現在値の取得から更新までの同期とslotの保護を一体で実装する。探索の再試行は
callbackの前に行い、callbackを再実行しない。panic時は保護を解放し、値の巻き戻しは保証しない。
既存のmodeごとの同期方式を前提に検証し、全mode共通の大域ロックを追加する仕様にはしない。

All/Keys/Valuesは型付きRangeItemに被せる。走査順と削除状態の扱いはRangeItemに
従い、走査全体のsnapshotを新たに保証しない。yieldがfalseなら生産側を止める。
KeysはVを取り出さず、不要な値コピーを避ける。
maps.CollectはGo標準mapが受け取れるcomparableなKで使え、slices.Sortedは順序付けできる
Kで使える。BytesKeyや独自Keyすべてが、それらの標準関数で使えるとはしない。

## 型付き実体の復元とgeneric method

EntryViewは既存のelist_head.List[Entry[K,V]]を使う。EntryのMapHead位置と
MapHead内のListHead位置を合わせたoffsetで、リンクから元のEntryへ戻る。
K/Vのサイズを固定せず、値・ポインタ・大きいstructを同じ規則で扱う。

本体の探索では、front/tail sentinelとdummyを確認してから実Entryへ戻す。
bare ListHeadやbucketのMapHeadを、Entryの内部とみなして逆変換しない。

試作のRecoverField[F any]はgeneric methodであり、直接格納したV内のフィールドから
外側のEntryへ戻ることを検証する。引数は実Entry内のフィールドのアドレスと、V内の
正しいfield offsetでなければならない。Value()で返したコピーのフィールドや、
Vが指す別allocation内のフィールドからEntryへは戻らない。この低水準機能の
公開名・入口はまだ確定していない。

## 本体への具体的な接続点

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
| bsearchBybucket、getItemWithBucket | 外部Entryとpool内Entryの混在を検索で扱う。配列の二分探索だけで外部Entryを見落とさない |
| lockFoundItem、deleteItem、purgeItem | owner bucket・slot・世代の再確認を保ち、外部Entryをpool slotと取り違えない |
| RangeItem、Range、First、Last | 実Entryの型を統一し、sentinel/dummyを返さない。All/Keys/Valuesを接続する |

## 011で実装・検証する本体動作

2026-09-30のkazuの承認により、010ではAPIを確定し、E3修正とUpdateの本体実装は
011の型付き化と一緒に行う。以下は011の受入条件であり、この試作の検証済み動作ではない。

1. **E3**: UseEmbeddedPoolを有効にしたMapへStoreItemした外部要素が検索で見つからない
   不具合は残っている。setItemはembedded modeでハッシュ設定を省き、bsearchBybucketは
   pool配列を検索する。型付きEntryを宣言しただけでは直らない。外部要素を配列へコピーする
   ことで実体の同一性を失わせたり、検索を全面的に走査へ置き換えたりしない。
   現行本体でNewEntryMapをStoreItemする再現テストは、Map.add2内で20秒の
   タイムアウトになった。登録完了と、pool要素との混在時の検索・更新・削除を検証する。
2. **Update**: callbackで現在の値を更新するAPIは未実装。poolの移動・slotの再利用と
   callbackの実行範囲を合わせる必要がある。embedded modeの既存muPoolと再確認処理、
   非embedded modeの更新・コピー公開処理を元に、上記callbackの寿命と実行回数を
   満たす。コピーを受け取ってから再検索せずに書く形ではlost updateを防げない。
3. **poolの型付き値公開**: 現在のSampleItemはatomic.Valueを使う。Vを直接置くには、
   大きい値やpointerを含む値についても並行読み書き・コピー・GCを検証する必要がある。
   試作の配置テストは、この並行性を証明するものではない。

この3点と、型付き公開Mapの登録・取得・削除の実行例は011で完成させる。
010の試作は型制約・配置・復元・iteratorの検証であり、本体Mapの並行安全性や
E3修正を証明しない。

## 試作で確認する範囲

標準8種類のハッシュ対、空・nilのbyte列の同一性、同じハッシュ対を持つ独自キーの
Equal、値とポインタの格納、異なるサイズ・alignmentのEntry、GCを挟んだリンク復元、
フィールドからの復元、iteratorの順序・早期終了・標準ライブラリとの接続を検証する。
メソッドを持たないstring、KeyHashが無い型、Equalがanyを受ける型はコンパイルで拒否する。

キーのハッシュ計算と実体復元のbenchは、interface変換とallocationの影響を切り分ける
試作である。本体Mapの性能比較、poolの並行性、E3やUpdateの完了を示すものではない。
