## Log Records
毎回logには`log record`が書き込まれる. 主要な`log record`は
- start: トランザクションの開始時点で書き込まれる
- commit: トランザクションの終了時点で書き込まれる
- rollback: トランザクションの終了時点で書き込まれる
- update: トランザクションが値を変更したら書き込まれる
  - Integers
  - String

## Rollback
実際に変更を行わずに, トランザクションをロールバック(それまでの変更を取り消す)する.
トランザクションTを取り消すアルゴリズムは
1. 今のレコードを最新のものに設定する
2. 現在のレコードがトランザクションTの`start`になるまで繰り返す
   1. 現在のレコードが`update`の場合はold valueを書き込む
   2. 現在のレコードを一つ前のレコードに設定する
3. `rollback`をlogに書き込む

## Recovery
database engineを起動させる毎にRecoveryを行う. これは
- 全ての完了していないトランザクションをroll backする
- 全てのコミットされたトランザクションの変更をディスクに書き込む

の状態にするため. (システムがクラッシュした場合にも対処できるように)

commitレコートをディスクに書き込んでから, 実際のcommit(データベースへの書き込み)を行うことで, 途中でクラッシュしても復元可能になる.逆にすると, 復元が不可能になるパターンが存在してしまう.

undo(1.)とredo(2.)のアルゴリズムは次のようになっている.
1. for record (後ろからループする)
   - if record == commit then commitedのリストに追加
   - if record == rollback then rolled_backのリストに追加
   - if record == update && recordがcommitもrollbackもされていない then old valueで復元する

2. for record (先頭からループする)
    - if record == update && recordがcommitされている then new valueで復元する

undoとredoを使って復元したときの結果の特徴は
- idempotent
  - 何度リカバリを実行しても, 結果は変わらないということ
- 不必要なディスクへの書き込み
  - 実際はディスクへ元から書き込まれてたデータでも, redoで再度書き込んでしまう.

### Undo-Only Recovery
コミットがlogに記録されていたら, 確実にバッファがディスクにflushされている場合, redoを省くことができる.(commitはバッファに何か書き込むわけではないので, Write-Ahead Loggingは満たしている. 重要なのはcommitがlogに記録される前に以前にupdateされて変更されたバッファをディスクに書き込んでいるということ.) \
アルゴリズムは次のようになっている.
1. トランザクションの変更をディスクにflushする
2. logにcommitレコードを書き込む
3. log pageをディスクにflushする

- メリット: undo-redoより高速
- デメリット: commitが遅くなる. 

### Redo-Only Recovery
トランザクションがコミットされるまでディスクに書き込まず, Bufferに保持しておく. (まだ書き込まれていないBufferはreplaceの対象にはならない). そのため, 途中でクラッシュしてもまだ書き込まれてないデータは消えるだけなので, 復元可能.
- メリット: undo-redoより高速
- デメリット: 巨大なトランザクションの場合には, バッファが足りなくなるかも.risky choice.

## Write-Ahead Logging
バッファをディスクにflushする前に, それに対応するlogをディスクに書き込むこと.

## Quiesct Checkpointing
静的なcheckpointを作ることで, undo-redoを高速化する. Quiesct Checkpointは次の二つを保証している.
- これより前のレコードは全てcommitされてある. (roll-backする必要がない)
- このトランザクションで使用されたバッファはすべてディスクにflushしてある.

checkpointを作る際のアルゴリズムは次のようになっている.
1. 新しいトランザクションの実行を受け付けない
2. いま実行しているトランザクションが全て終わるまで待つ
3. バッファを全てディスクにflushする
4. logにcheckpointを書き込み, logをディスクにflushする
5. 新しいトランザクションの受付を再開する

## NonQuiesct Checkpointing
Quiesct Checkpointingだと, トランザクションの処理が終わるのを待たないといけないので, パフォーマンスが非常に悪い.
そこで使われるのがNonQuiesct Checkpointで, その時点で処理が終わっていないトランザクションを記録する.
この記録したトランザクションの全てのStartレコード(トランザクションの開始地点)まで遡れば, それより前はcommitされていることが確実なので, undoされない(roll backがないことが確実).

NonQuiesct Checkpointingを作る際のアルゴリズムは次のようになっている.
1. T1, ... Tkが現在実行中のトランザクション
2. それ以外のトランザクションの新規受付を終了
3. バッファを全てディスクにflushする
4. \<NQCKPT T1, ... Tk\>をlogに書き込む
5. 新しいトランザクションの受付を開始する

## 実装していて詰まったところ
### rollbackとrecoveryの違い
rollbackはそのトランザクションの変更を全て取り消して、最後にログファイルに`Rollback`レコードを打つ.
`Rollback`がログファイルに書かれているということは、その取り消しは完了したことを意味するのでrecoveryの時は何もしなくていい.(トランザクションは正常に終了しているから)
recoveryで行うのは, `Commit`も`Rollback`も打たれていないようなトランザクション(つまり、トランザクションが不完全に終了している)の変更を全て取り消す. 