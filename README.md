# La valeur

このプロジェクトは[BIGCHA2016](http://bigcha.net/)で作成したものです．

## 概要
このアプリケーションでは
ブランド名とアイテム名（ex. Tシャツ)を指定すると
それに関連したデータの平均価格などの解析結果を表示してくれるアプリケーションです．

これにより，
ユーザはブランドの価格帯や適正価格について知ることができます．


## やったこと
* オークションデータをHadoopを用いて解析．
* Webアプリケーションにてその解析結果を表示．


## 担当
* データ準備, Webアプリケーション実装：[@t1ks0n](https://github.com/t1ks0n)
* UIデザイン：[@peanutmanuec](https://github.com/peanutmanuec)
* 平均価格計算Reducer実装：[@Umekawa](https://github.com/Umekawa)
* 平均価格計算Mapper実装, データクレイジングMapReduce実装：[@hakaiSAN](https://github.com/hakaiSAN)



## 補足
MapReduceのプロジェクトは講義のパッケージを準拠しています  
[MapReduce](https://github.com/exKAZUu/MapReduceExcercise)
