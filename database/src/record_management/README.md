# Record Management

## 概要
トランザクションでは, ディスクに対してデータの書き込みができたが, そのデータが何のデータなのかがわからなかった.
Record Managerがこれを解決する.

## Designing a Record Manager
### Spanned Versus Unspanned Records
データがブロックに入り切らないときに, 分割して複数のブロックを跨がせるか(Spanned), 新しいブロックを作成してそこに全部乗せるのか(Unspanned).
- spanned
    - 実装が複雑になる
    - ディスクからデータを読み込むたびに, データの再構築が必要になる
- unspanned
    - 無駄なスペースができる. 
    - そもそもブロックサイズよりも大きなレコードだった場合どうするのか?
### Homogeneous Versus Nonhomogeneous Files
- Homogeneous \
    同じtableのレコードは同じファイルに書く \
    メリット: single-tableのクエリに高速
- Nohomogeneous \
    ファイルに複数のtableのレコードを書く
    メリット: 複数のtableを結合するクエリに高速
### Fixed-Length Versus Variable-Length Fields
文字列などの可変長のデータをどのように格納するかの問題.
考えられる実装方法は三種類
- 可変長で実装する(varchar)
- 固定長: あらかじめ文字列の最大値を決めておく(char)
- 固定長: 文字列自体は別の場所に格納して, レコードの中には文字列の場所を表す情報を置く(clob)

## 実装メモ
- scheme: テーブルのそれぞれの要素の情報
- layout: テーブルの情報（schemeを集めたもの）
- record_page: テーブルへ実際の値を入れるときに使う