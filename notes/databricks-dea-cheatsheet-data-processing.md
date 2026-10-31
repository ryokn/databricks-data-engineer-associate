# Databricks Data Engineer Associate: データ処理チートシート

> 直前確認用。細かい既定値や版による違いは、受験前に公式ドキュメントで確認すること。

## 1. 取り込み(Ingestion)

| 手段 | 用途 | 特徴 |
|---|---|---|
| `COPY INTO` | クラウドストレージのファイルをバッチで取り込む | 冪等。読み込み済みファイルは再処理しない |
| Auto Loader (`cloudFiles`) | ファイルの増分取り込み(バッチ・ストリーム) | 新規ファイルを自動検知。スキーマ推論と進化に対応 |
| JDBC (`spark.read.jdbc`) | 外部 DB からバッチで読む | 認証情報は Secret で管理する |
| Lakeflow Connect(マネージドコネクタ) | Salesforce、Workday Reports、PostgreSQL などから取り込む | ノーコード。認証・スケジュール・CDC は自動 |

**Auto Loader の主なオプション**
```python
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", schema_path)     # 推論したスキーマの保存先
  .option("cloudFiles.inferColumnTypes", "true")        # 型を推論する(既定は文字列)
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .load(input_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)        # 進捗の保存先
  .toTable("main.bronze.events"))
```

- `schemaLocation` は**スキーマ**、`checkpointLocation` は**進捗**。役割を混同しない。
- `inferColumnTypes` を `true` にすると、数値・真偽値・タイムスタンプを推論する。

**スキーマ進化モード**

| モード | 新しい列を見つけたとき |
|---|---|
| `addNewColumns`(既定の一つ) | ストリームが失敗 → スキーマ更新 → 再起動で反映。過去の行は再処理しない |
| `rescue` | `_rescued_data` に退避。スキーマは変わらず、継続 |
| `failOnNewColumns` | 失敗する。スキーマは変わらない |
| `none` | 無視して継続。スキーマは変わらない |

**再起動の対象**: Structured Streaming のクエリ。チェックポイントがあるため、失敗地点から再開する。

## 2. 更新・書き込み

**テーブル作成(CTAS)**
```sql
CREATE TABLE main.silver.orders AS
SELECT * FROM main.bronze.orders WHERE status = 'done';
```

**MERGE(一致すれば更新、なければ追加)**
```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**DataFrame の書き込み**
- `df.write.mode("overwrite").saveAsTable("catalog.schema.table")`
- `overwrite` はテーブルの中身を置き換える。`append` は追加する。

## 3. テーブルとビュー

| オブジェクト | 物理データ | 有効範囲 |
|---|---|---|
| Managed table | あり | 永続。`DROP` でデータも削除 |
| External table | あり(自分の場所) | 永続。`DROP` はメタデータのみ削除 |
| View | なし | 永続。定義のみ保存 |
| Temporary view | なし | 現在のセッションのみ |
| Global temporary view | なし | クラスタ内(`global_temp` スキーマ) |

- 「コピーせず、セッションだけで使う」→ **Temporary view**
- 外部テーブルを管理テーブルへ: `ALTER TABLE t SET MANAGED`(テーブル履歴を保持し、外部へ戻せる)

## 4. 制約と列・行の制御(Unity Catalog)

```sql
-- NOT NULL(列の属性なので ALTER COLUMN)
ALTER TABLE t ALTER COLUMN c SET NOT NULL;
ALTER TABLE t ALTER COLUMN c DROP NOT NULL;

-- CHECK(名前付きの制約なので ADD CONSTRAINT)
ALTER TABLE t ADD CONSTRAINT valid_amount CHECK (amount >= 0);
ALTER TABLE t DROP CONSTRAINT valid_amount;

-- 列マスク
ALTER TABLE t ALTER COLUMN c SET MASK card_mask;
ALTER TABLE t ALTER COLUMN c DROP MASK;

-- 行フィルター
ALTER TABLE t SET ROW FILTER row_filter ON (region);
ALTER TABLE t DROP ROW FILTER;
```

- 複数列をまとめて指定する `ALTER COLUMNS (a, b) SET NOT NULL` という構文はない。列ごとに1文ずつ書く。
- マスク関数の判定には `IS_ACCOUNT_GROUP_MEMBER()` を使う。
- 列マスクと行フィルターは Unity Catalog の機能。Hive メタストアでは使えない。

## 5. 結合・集合演算

**結合**

| 結合 | 残る行 |
|---|---|
| `INNER JOIN` | 両方に一致する行のみ |
| `LEFT JOIN` | 左の行をすべて残す(一致なしは右が NULL) |
| `RIGHT JOIN` | 右の行をすべて残す |
| `LEFT ANTI JOIN` | 左のうち、右に一致しない行のみ |

- `LEFT` / `RIGHT` は、FROM に書いたテーブルの位置で決まる。
- 結果に NULL があれば、そちら側は一致しなかった行。

**UNION**
- `UNION ALL`: 重複を残す。速い。
- `UNION`(Spark SQL では `UNION DISTINCT` と同じ): 重複を削除する。
- DataFrame: `union()` は重複を残す。`distinct()` で削除。`unionByName()` は列名で対応させる。
- `union()` は列を**位置**で対応させる。

## 6. 結合の性能(Spark)

| 方式 | 仕組み | 向いている場面 |
|---|---|---|
| Broadcast join | 小テーブルを全ノードに配る。シャッフルなし | 大 × 小 |
| Shuffle (sort-merge) join | 両方をキーで振り分けて移動 | 大 × 大 |

- ヒント: `/*+ BROADCAST(p) */`、関数: `broadcast(df)`
- 既定の自動ブロードキャスト閾値は `spark.sql.autoBroadcastJoinThreshold`(既定は約10MB)。

**AQE(Adaptive Query Execution)**
- 実行時の実データ量で計画を直す。最近のランタイムでは既定で有効。
- 主な機能: パーティションの統合、結合方式の切り替え、スキュー結合の分割。

**OOM の原因**
- `collect()`、`toPandas()` は全データをドライバーに集める → `spark.driver.memory` 不足で失敗。
- 大きいデータは分散したまま書き込む。必要なら `limit()` で絞る。

## 7. 最適化とメンテナンス

**Liquid Clustering**
```sql
CREATE TABLE events (...) CLUSTER BY (user_id);
ALTER TABLE events CLUSTER BY (event_time);   -- キー変更
ALTER TABLE events CLUSTER BY NONE;           -- 無効化
OPTIMIZE events;                              -- 実際に並べ替える
```
- `CLUSTER BY` は目標の配置を決める。`OPTIMIZE` が実際に並べる(増分処理)。
- 設定しただけでは既存データは並ばない。
- Liquid Clustering と `ZORDER BY`、パーティションは併用できない。
- クラスタリングキーは最大4列。

**その他**
- `VACUUM`: 保持期間を過ぎた古いファイルを物理削除する。`OPTIMIZE` とは別の操作。
- `DESCRIBE HISTORY`: 変更履歴を見る。`RESTORE`: 過去のバージョンに戻す。
- 予測最適化(Predictive Optimization): `OPTIMIZE` などを自動実行(マネージドテーブル)。

## 8. パイプライン(Lakeflow Spark Declarative Pipelines)

```sql
CREATE OR REFRESH STREAMING TABLE orders_clean (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amt EXPECT (amount >= 0) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM(main.bronze.orders);
```

| 句 | 違反したレコード | パイプライン |
|---|---|---|
| (省略) | 残す(違反はメトリクスに記録) | 継続 |
| `ON VIOLATION DROP ROW` | 削除(メトリクスに記録) | 継続 |
| `ON VIOLATION FAIL UPDATE` | — | 失敗 |

- `DELETE ROW` という句はない。

## 9. ジョブとデプロイ

**Jobs のタスク種類**: ノートブック、Python スクリプト、Python wheel、SQL、JAR、パイプライン、別ジョブの実行、If/else 条件、For each など。
- `REST API call` はタスク種類ではない(API で操作するもの)。
- **Python wheel**: パッケージをインストールし、エントリポイントの関数を実行する。
- `libraries` で wheel を指定するのは、インストールのみ。ほかのタスクから `import` して使う。

**コンピュート**

| 用途 | 選択 |
|---|---|
| 需要の増減に自動対応したい(運用負荷を減らしたい) | Serverless compute |
| SQL で BI・分析 | SQL warehouse |
| Unity Catalog のデータにアクセス | Classic compute の Standard / Dedicated アクセスモード |
| Unity Catalog 非対応 | `No isolation shared` は不可 |

**Declarative Automation Bundles(旧 Databricks Asset Bundles)**
```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod --fail-on-active-runs   # 実行中ならデプロイを失敗させる
```
- `--force` は上書きのフラグ。実行中の確認はしない。

## 10. 直前チェックリスト

- [ ] `schemaLocation`(スキーマ)と `checkpointLocation`(進捗)を区別できる
- [ ] `addNewColumns` は「失敗 → スキーマ更新 → 再起動」
- [ ] `COPY INTO` と Auto Loader の使い分け
- [ ] Managed / External の `DROP` の違い
- [ ] Temporary view と Global temporary view の範囲
- [ ] `LEFT` / `RIGHT` の残る側、`ANTI JOIN`
- [ ] `UNION` と `UNION ALL`、`union()` と `distinct()`
- [ ] Broadcast join の条件と、`collect()` による OOM
- [ ] `CLUSTER BY` と `OPTIMIZE` の役割分担
- [ ] Expectations の3つの句(省略 / DROP ROW / FAIL UPDATE)
- [ ] `ALTER COLUMN` と `ADD CONSTRAINT` の使い分け
- [ ] 列マスク(`SET MASK`)と行フィルター(`SET ROW FILTER`)
- [ ] `--fail-on-active-runs` の意味
