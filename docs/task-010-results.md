# 010: 型付きAPIの設計と検証

本体の基点は47911fe。API仕様は[typed-api.md](typed-api.md)、実行可能な試作は
`internal/typedapi`に置いた。本体Mapの実装変更は0行であり、型付き化の完了ではない。
2026-09-30のkazuの承認により、E3修正とUpdateの本体実装は011で行う。
016は親002に残る別作業。

## 確認したこと

- KeyHashとEqualを持つ共通キー制約。既存8型に対応する標準キー型と独自キーを扱う。
  生のstring、KeyHashの無い型、Equalの引数型が異なる型はコンパイルで拒否される。
- EntryのVに通常のUserを直接格納し、*Userなら外部実体を参照する。異なるalignmentと
  大きい値でもelist_headの型付き復元を使える。内部フィールドからの復元はgeneric methodで試した。
- All/Keys/ValuesはRangeItemsの薄い変換で、yieldのfalseを伝播する。
  maps.Collect/slices.Collect/slices.Sortedとの接続も実行した。
- 試作のキー計算・リンク復元・フィールド復元は測定で0 B/op、0 allocs/op。
  StringKeyと{Name string; Age int}を持つ試作Entryは80 bytes。
  Mapの保持メモリ、並行安全性、速度改善を示す測定ではない。
  escape analysisではNewEntryの返す実体がheapへ逃げる経路もあるため、Entry生成や
  iteratorの収集まで無割当という意味ではない。Get/ValueのV返却は値コピーになる。

## 検証記録

Go 1.27.1、GOMAXPROCS=4、仮想メモリ上限4 GiB。
elist_headはcab6a1b、lonchaは1a71ffe。専用elist_head worktreeをgo.workから選び、
元の兄弟checkoutのbranchは変更していない。

`make ci`は通常117・race119・checkptr117・step31件、vetを含めて成功した。
所要11分51.74秒、最大RSS1,777,856 KiB、終了コード0。
最終版の試作にもraceとcheckptrを同時に有効にしたテストとvetを実行して成功した。
負のコンパイル例は期待どおり失敗した。workflowのYAML解析とci.shの構文検査も成功。
GitHub Actions自体は実行していない。

一次ログはgit_task 010のraw配下に保存した。
`ci.stdout.log` / `ci.stderr.log`、`api-bench.stdout.log` / `api-bench.stderr.log`、
`embedded-entry.stdout.log` / `embedded-entry.stderr.log`を参照。
escape analysisは`escape.stdout.log` / `escape.stderr.log`に保存した。
E3の既存本体での再現は20秒のタイムアウトであり、成功件数に含めていない。

## 011への引き継ぎ

Map/options/item/pool/bucketにK/Vを伝播し、HMapEntryとItemFnによる型消去を除く。
標準型のハッシュとEqual、外部Entryの同一性、sentinel判別、既存の所有権を保つ。
E3とUpdateは[API仕様の受入条件](typed-api.md)を満たし、型付き本体の登録・取得・削除の
実行例と全回帰を通す。試作だけを公開APIとして配布したり、本体完了と扱ったりしない。
