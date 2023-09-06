## Serializable Schedules
`concurrency control`は正しいscheduleのみが実行されることを保証するもの.
正しいスケジュールとは`serializable`なもの.
例えば二つのトランザクションT1, T2が次のように実行される場合を考える.
```
W1(b1) ; W2(b1) ; W1(b2) ; W2(b2)
```
これは
1. T1がブロック1に書き込み
2. T2がブロック1に書き込み
3. T1がブロック2に書き込み
4. T2がブロック2に書き込み

これは結局, トランザクション1を実行した後にトランザクション2を実行した結果（1->3->2->4）と同じ.
よってこれは, `serializable`.

```
W1(b1) ; W2(b1) ; W2(b2) ; W1(b2)
```
これは, トランザクションを綺麗に分解できない. これは, `non serializable`.これは不正な実行になる.

## The Lock Table
- slock : 読み込みする時. rustのimmutable ref
- xlock : 書き込みする時. rustのmutable ref