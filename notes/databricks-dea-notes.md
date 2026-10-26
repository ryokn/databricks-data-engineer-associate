---
title: Databricks Certified Data Engineer Associate 要点メモ
exam: Databricks Certified Data Engineer Associate(2026年5月4日版 / 公式ガイド 2026-08-06 更新)
created: 2026-10-26
tags: [certification, databricks, data-engineer-associate, study-note]
sources:
  - Databricks Certified Data Engineer Associate(公式試験ページ): https://www.databricks.com/learn/certification/data-engineer-associate
  - Exam Guide August 2026(公式PDF): https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf
  - Auto Loader スキーマ推論と進化: https://docs.databricks.com/en/ingestion/auto-loader/schema.html
  - テーブル更新トリガー: https://docs.databricks.com/en/jobs/trigger-table-update
  - パイプラインの Expectations: https://docs.databricks.com/delta-live-tables/expectations.html
  - ABAC(属性ベースアクセス制御): https://docs.databricks.com/en/data-governance/unity-catalog/abac.html
---

# Databricks Certified Data Engineer Associate 要点メモ

> 用語の改称に注意。旧 Delta Live Tables(DLT) → **Lakeflow Spark Declarative Pipelines**、旧 Workflows → **Lakeflow Jobs**、旧 Databricks Asset Bundles(DABs) → **Declarative Automation Bundles**(略称は DABs のまま使われる)。古い教材の名前と読み替えること。

## 試験概要
| 項目 | 内容 |
|---|---|
| 形式 | 択一選択(4択)。採点対象 45問 + 採点対象外(unscored)が最大10問混ざる |
| 時間 | 90分(unscored 分の追加時間は含まれている) |
| 受験料 | USD 200(税別) |
| 合格基準 | 公式ガイドに記載なし（要確認: 公式ガイドで未確認。本問題集は目安として70%を使用） |
| 提供方法 | オンライン監督 or テストセンター。言語は英語・日本語・ポルトガル語(BR)・韓国語 |
| 有効期間 | 2年(更新には Renewal Exam か最新版の本試験) |
| 推奨経験 | Databricks で約6か月の実務(公式推奨) |
| コード表記 | データ操作コードは可能な限り SQL、それ以外は Python |
| 出典確認日 | 2026-10-26 |

### 出題分野と配点
| 分野 | 比率 | 45問換算の目安 |
|---|---|---|
| Databricks Intelligence Platform | 6% | 約3問 |
| Data Ingestion and Loading | 21% | 約9問 |
| Data Transformation and Modeling | 22% | 約10問 |
| Working with Lakeflow Jobs | 16% | 約7問 |
| Implementing CI/CD | 10% | 約4〜5問 |
| Troubleshooting, Monitoring, and Optimization | 10% | 約4〜5問 |
| Governance and Security | 15% | 約7問 |

Ingestion + Transformation で全体の43%。ここを固めるのが最短ルート。

---

## 1. Databricks Intelligence Platform(配点 6%)

### 押さえる要点
- **Delta Lake**: Parquet + トランザクションログ(`_delta_log`)。ACID、タイムトラベル、スキーマ強制/進化、ストリーミングとバッチの統合を提供する。
- **タイムトラベル系コマンド**: `DESCRIBE HISTORY`(履歴確認)、`VERSION AS OF` / `TIMESTAMP AS OF`(過去参照)、`RESTORE TABLE ... TO VERSION AS OF`(巻き戻し)。
- **VACUUM**: 参照されない古いファイルを削除する。既定の保持期間は7日(168時間)。実行するとその期間より前へはタイムトラベルできなくなる。
- **Unity Catalog(UC)**: データとAI資産を一元管理するガバナンス層。3階層の名前空間 `catalog.schema.table`。アクセス制御・監査・リネージを提供する。メタストアは複数ワークスペースで共有できる。
- **コンピュートの選択**:
  - 対話的な開発・探索 → all-purpose compute
  - 定期実行の本番ETL → ジョブ用コンピュート(job compute。起動して終了するのでコスト効率が良い)
  - 多数の同時SQLユーザー・BI → SQL warehouse(サーバーレスは起動が速くオートスケール)
  - 運用負荷を下げたい → サーバーレスコンピュート

### 比較表
| 観点 | all-purpose compute | job compute | SQL warehouse(serverless) |
|---|---|---|---|
| 主用途 | 開発・対話分析 | スケジュールされたETL | アドホックSQL・BI・ダッシュボード |
| ライフサイクル | 手動で起動/停止(自動終了あり) | ジョブ実行ごとに作成・終了 | オートスケール、起動が速い |
| コスト感 | 高めになりやすい | 本番ジョブ向きで安い | 同時利用が多いときに有利 |

### ひっかけポイント
- 「監査・巻き戻し・単一の真実の源」→ Delta Lake(ACID・タイムトラベル) + UC(ガバナンス)の組み合わせで答える。
- VACUUM は「古い**ファイル**を物理削除」。テーブルのメタデータ履歴の表示(DESCRIBE HISTORY)とは別物。

---

## 2. Data Ingestion and Loading(配点 21%)

### 押さえる要点
- **取り込みパターン**: バッチ / ストリーミング / 増分(incremental)。
- **COPY INTO**: クラウドストレージからDeltaテーブルへファイルを**増分**ロードする SQL コマンド。**冪等**(同じファイルを二重にロードしない)で、再実行しても安全。比較的ファイル数が少ない・SQL中心の増分ロード向き。
- **Auto Loader**: `format("cloudFiles")` で新着ファイルを自動検知して増分処理する。大量ファイル・継続的な到着に向く。
  - 必須級のオプション: `cloudFiles.format`、`cloudFiles.schemaLocation`(スキーマ推論・進化に必要)、書き込み側の `checkpointLocation`。
  - ファイル検知: ディレクトリリスト方式 / ファイル通知方式(ファイルイベント)。
  - `trigger(availableNow=True)` でバッチ的に「溜まった分を処理して終了」できる。
  - スキーマ推論したとき、パースできなかったデータは `_rescued_data` 列に退避される。
- **スキーマ進化モード**(`cloudFiles.schemaEvolutionMode`):
  | モード | 挙動 |
  |---|---|
  | `addNewColumns`(**スキーマ未指定時の既定**) | 新しい列を検知するとストリームを失敗させ、スキーマを更新。再起動後は新列つきで処理 |
  | `rescue` | スキーマは変えず、新しい列のデータは `_rescued_data` に入る。ストリームは失敗しない |
  | `failOnNewColumns` | 新しい列が来たら失敗。スキーマも更新されない |
  | `none`(**スキーマを自分で指定した場合の既定**) | スキーマ進化なし。新しい列は無視される(rescued data を設定していれば退避) |
- **Lakeflow Connect**: 取り込みコネクタの総称。標準コネクタ(クラウドストレージ等)と**マネージドコネクタ**(SaaS・データベース向けのフルマネージド取り込み。例: Salesforce、SQL Server)がある。
- **JDBC/ODBC・REST**: ノートブックから外部DB/APIに接続し、クラウドストレージやUCテーブルへ書き込む。Lakeflow Jobs で定期実行する構成が典型。
- **半構造化データ**: JSON などの入れ子データも UC 管理の Delta テーブルへ取り込める。

### 比較表
| 観点 | COPY INTO | Auto Loader | Lakeflow Connect(マネージド) |
|---|---|---|---|
| 入力元 | クラウドストレージのファイル | クラウドストレージのファイル | SaaS・DB など |
| 向く規模 | 比較的少ないファイル数 | 大量ファイル・継続到着 | 外部システムとの連携 |
| インターフェース | SQL | Spark Structured Streaming(Python/SQL) | UI/設定ベース |
| 冪等性 | 組み込み | チェックポイントで保証 | サービス側で管理 |

### ひっかけポイント
- 「新着ファイルを増分で取り込む」「ファイルが大量」→ Auto Loader。「SQLで手軽に・ファイルが少ない」→ COPY INTO。
- 「SaaSやDBを設定ベースで継続取り込み」→ Lakeflow Connect のマネージドコネクタ。自前の JDBC ノートブックにしない。
- `addNewColumns` が既定なのは**スキーマ未指定のとき**。スキーマを渡すと既定は `none`。
- `schemaLocation` を設定しないとスキーマ推論・進化が使えない。`checkpointLocation` とは別物。

---

## 3. Data Transformation and Modeling(配点 22%)

### 押さえる要点
- **メダリオンアーキテクチャ**: Bronze(生データ) → Silver(クレンジング・標準化・重複排除) → Gold(ビジネス向け集計・BI/分析用)。
- **Silver 化の典型作業**: null 処理(`dropna` / `fillna` / `coalesce`)、型の統一(`cast`)、重複排除(`dropDuplicates`)、列名の標準化。
- **結合**: inner / left / right / full / cross。`broadcast()` ヒントで小さい側を全ワーカーに配って shuffle を避ける。複数キーの結合は条件を `&` で連結。
- **集合演算**: DataFrame の `union()` は**列の位置**で結合し、重複を**除去しない**(SQL の `UNION ALL` 相当)。列名で合わせるなら `unionByName()`。SQL の `UNION` は重複除去、`UNION ALL` は重複保持。
- **列・行の操作**: `withColumn`(追加/上書き)、`drop`、`withColumnRenamed`、`filter`、`split`、`explode`(配列を1要素1行に展開)。
- **集計**: `count`、`approx_count_distinct`(近似。大規模データで高速)、`avg`/`mean`、`summary`/`describe`。
- **基本チューニング**:
  | 設定 | 内容 | 既定値 |
  |---|---|---|
  | `spark.sql.shuffle.partitions` | shuffle後のパーティション数 | 200 |
  | `spark.sql.autoBroadcastJoinThreshold` | 自動ブロードキャストするテーブルサイズの閾値 | 10MB |
  | `spark.default.parallelism` | RDD操作の既定並列度 | — |
  | `spark.executor.memory` / `spark.driver.memory` | メモリ割り当て | — |
  変更したら必ず**再計測**して効果を確認する。
- **Gold のオブジェクト**:
  | オブジェクト | 特徴 |
  |---|---|
  | テーブル | データを物理保存。手動/ジョブで更新 |
  | ビュー | クエリのたびに計算(データは保存しない) |
  | マテリアライズドビュー | 結果を事前計算して保存。可能なら増分更新。BI の高速化向け |
  | ストリーミングテーブル | ストリーミング/増分入力を追記処理。取り込み・Silver向き |
- **データ品質(Expectations)**: Lakeflow Spark Declarative Pipelines でテーブル/MVにルールを宣言する。

  | 種類 | 違反時の動作 |
  |---|---|
  | `expect` | レコードは残す。違反件数をメトリクスに記録 |
  | `expect_or_drop` | 違反レコードを捨てる |
  | `expect_or_fail` | 更新を直ちに失敗させる |

  SQL では `CONSTRAINT 名 EXPECT (条件) [ON VIOLATION DROP ROW | FAIL UPDATE]`。メトリクスはパイプラインのイベントログで確認できる。

### ひっかけポイント
- `union()` は重複を消さない。重複排除が必要なら別途 `distinct()` / `dropDuplicates()`。
- `approx_count_distinct` は**近似**。厳密な値が必要なら `countDistinct`。
- マテリアライズドビュー=事前計算、ビュー=都度計算。「BIで応答を速く」→ MV。
- ブロードキャスト結合は**小さい側**を配る。大きいテーブル同士には使えない/効かない。

---

## 4. Working with Lakeflow Jobs(配点 16%)

### 押さえる要点
- **タスクの種類**: ノートブック、SQLクエリ、ダッシュボード、パイプライン(Lakeflow Spark Declarative Pipelines)など。タスク間の依存関係は **DAG(有向非巡回グラフ)** で表す。
- **制御フロー**: 失敗時の**リトライ**、**条件分岐タスク**(If/else)、**ループ**(For each タスク)。依存タスクの実行条件(「すべて成功」「少なくとも1つ成功」「すべて完了」など)も設定できる。
- **トリガー種別**:
  | 種別 | 起動条件 | 向く場面 |
  |---|---|---|
  | スケジュール(cron) | 時刻ベース | 毎日・毎時など定刻の処理 |
  | ファイル到着(file arrival) | 外部ロケーション/ボリュームに新ファイルが届いたとき | 到着が不定期なファイル |
  | テーブル更新(table update) | UCテーブルが更新されたとき | 上流テーブルの更新後に下流を回す |
  | 継続(continuous) | 常時稼働 | 低遅延のストリーミング処理 |
- **トリガーの調整**: `Minimum time between triggers`(前回の実行から最低限空ける時間=クールダウン)と `Wait after last change`(最後の変更から待つ時間=デバウンス)。両方設定すると、まず最小間隔を待ち、そのあと最終変更からの待機を行う。
- **時間ベース vs データ駆動**: データ到着が読めない・空振りを避けたい → データ駆動(ファイル到着/テーブル更新)。到着時刻が決まっている → スケジュール。
- **実行時間のしきい値**: タスクに timeout を設定でき、超過すると `Timed Out` で終了する。

### ひっかけポイント
- 「到着が不定期」+「空のコンピュートを起動したくない」→ ファイル到着トリガー。5分ごとのポーリングは空振りが発生する。
- 「上流のテーブルが更新されたら実行」→ テーブル更新トリガー(UC のテーブルが対象)。
- 継続トリガーは常時稼働なのでコストが増える。低遅延が不要なら使わない。
- パイプライン(旧DLT)は Lakeflow Jobs の**パイプラインタスク**として組み込んで他タスクとつなぐ。

---

## 5. Implementing CI/CD(配点 10%)

### 押さえる要点
- **Databricks Git フォルダー**(旧 Repos): ワークスペース UI からブランチの作成・切り替え、コミット、プッシュ、プルリクエスト作成ができる。本番ブランチの運用は Git 側のルールで守る。
- **Declarative Automation Bundles(DABs)**: ジョブ、パイプライン、その他ワークスペース資産を **`databricks.yml`** でコードとして宣言し、Git でバージョン管理して、dev/test/prod に同じコードを昇格させる仕組み。
  - `targets` で環境ごとの差分(ワークスペースURL、モード、変数の上書き)を定義する。
  - `variables` で値を共通化し、ターゲットごとに上書きできる。環境変数 `BUNDLE_VAR_<変数名>` で値を渡せる(秘密情報をYAMLやコードに書かないために使う)。
- **Databricks CLI のバンドルコマンド**:
  | コマンド | 役割 |
  |---|---|
  | `databricks bundle validate` | 設定の検証 |
  | `databricks bundle deploy -t <target>` | 指定ターゲットへデプロイ |
  | `databricks bundle run <リソース名>` | デプロイ済みのジョブ/パイプラインを実行 |
  | `databricks bundle destroy` | デプロイしたリソースを削除 |

### ひっかけポイント
- DABs が管理するのは**オーケストレーション定義ごと**(ジョブ・パイプライン)。wheel や notebook の履歴だけでは環境昇格できない。
- 秘密情報は `databricks.yml` に直書きしない。変数 + `BUNDLE_VAR_` 環境変数、またはシークレット機能を使う。
- 環境ごとの違いはコードをコピーせず、**ターゲットと変数の上書き**で表現する。

---

## 6. Troubleshooting, Monitoring, and Optimization(配点 10%)

### 押さえる要点
- **ジョブ監視**: Lakeflow Jobs の実行履歴(run history)で、直近の実行時間を過去のベースラインと比べて傾向を見る。ジョブ詳細のタスクグラフ(DAG)で、上流で止まっているタスクを特定する。ステータス(成功/失敗/Timed Out/スキップ等)と失敗率を追う。
- **Spark UI での性能診断**:
  | 症状 | 見るところ | 原因 |
  |---|---|---|
  | 1タスクだけ極端に遅い | ステージのタスク分布(Max vs Median の shuffle read) | **データスキュー**(特定キーの偏り) |
  | shuffle read/write が大きい | ステージ間のデータ移動量 | 広い変換(join/groupBy)による shuffle |
  | Spill(メモリ/ディスク)が大きい | タスクの spill 指標 | パーティションがメモリに収まらない |
  - スキュー対策の例: ソルティング(キーにランダム接尾辞を付けて分散し、2段階で集計)、ブロードキャスト結合、AQE のスキュー結合最適化(結合向け。groupBy 集計のスキュー自体は解消しない)。
- **Liquid Clustering**: `CLUSTER BY (列)` でテーブルのレイアウトを管理する新方式。従来のパーティショニングと `ZORDER` の**代替**で、クラスタリングキーを後から変更でき、`OPTIMIZE` で増分的にクラスタリングされる。同じテーブルで ZORDER とは併用しない。`CLUSTER BY AUTO` はキーを自動選択(予測最適化が必要)。
- **予測最適化(Predictive Optimization)**: UC 管理テーブルの保守(OPTIMIZE、VACUUM など)を Databricks が自動で実行する。
- **クラスタ起動失敗・ライブラリ競合・OOM** の切り分け: クラスタのイベントログ/ドライバーログを見る。OOM はメモリの大きいノード、パーティション数の見直し、スキュー解消で対処する。

### ひっかけポイント
- 「Max shuffle read が Median の10倍以上」=スキュー。**ノードを増やしても遅い1タスクは速くならない**。
- `spark.sql.shuffle.partitions` を減らすとパーティションが大きくなり、スキュー・spill が悪化しやすい。
- Liquid Clustering は ZORDER/パーティションの**併用ではなく置き換え**。

---

## 7. Governance and Security(配点 15%)

### 押さえる要点
- **管理テーブルと外部テーブル(UC)**:
  | 観点 | 管理(managed)テーブル | 外部(external)テーブル |
  |---|---|---|
  | データの置き場 | UC が管理する領域 | 自分で指定した LOCATION |
  | DROP したとき | メタデータとデータの両方が削除対象 | メタデータのみ削除。データファイルは残る |
  | ライフサイクル管理 | UC が最適化・保守を担う | 利用者が管理 |
  - 外部 → 管理への変換は `ALTER TABLE ... SET MANAGED`。変換直後に戻す場合は `ALTER TABLE ... UNSET MANAGED`(変換後14日以内に限る。公式ガイドのサンプル解説による）。
- **アクセス制御**: 権限は `GRANT` / `REVOKE`(ガイドは `DENY` も列挙)。付与先(プリンシパル)は**ユーザー、グループ、サービスプリンシパル**。階層は metastore → catalog → schema → table/view など。下位オブジェクトを使うには上位に `USE CATALOG` / `USE SCHEMA` が必要。
- **行フィルターと列マスク**: テーブルに関数を紐づけ、ユーザー/グループに応じて見える行・値を制御する。判定には `is_account_group_member('グループ名')` などを使う。
- **ABAC(属性ベースアクセス制御)**: **ガバナンスタグ**を付けた対象に、行フィルターポリシー/列マスクポリシーを**一括適用**する。ポリシーは catalog / schema / table の階層に付与し、クエリ時に動的に評価される。UDF にフィルタ・マスクのロジックを書く。テーブルごとの行フィルター/列マスクより**集中管理**に向く。

### 比較表
| 観点 | テーブル単位の行フィルター/列マスク | ABAC ポリシー |
|---|---|---|
| 設定の単位 | テーブル/列ごと | タグで一致した複数オブジェクトに一括 |
| 管理の集中度 | 分散しやすい | 中央集権的 |
| 基準 | 関数 + グループ判定 | ガバナンスタグ + ポリシー条件 |

### ひっかけポイント
- 外部テーブルを DROP してもデータファイルは消えない。管理テーブルは消える(UNDROPの猶予あり）。
- 列の見え方をユーザー別に変える方法は、**ユーザーごとにテーブルを複製しない**。列マスクを使う。
- 権限は上位(catalog/schema)から継承される。最小権限で `USE CATALOG` + `USE SCHEMA` + `SELECT` が必要。

---

## 用語集
| 用語 | 一言説明 |
|---|---|
| Delta Lake | ACID とタイムトラベルを提供するオープンなテーブル形式 |
| Unity Catalog | データ/AI資産の統合ガバナンス。`catalog.schema.table` |
| Medallion | Bronze→Silver→Gold の段階的データ精製 |
| Auto Loader | `cloudFiles` による新着ファイルの増分取り込み |
| COPY INTO | ファイルを冪等に増分ロードする SQL コマンド |
| `_rescued_data` | スキーマに合わないデータを退避する列 |
| Streaming table | 増分/ストリーミング入力を扱うパイプラインのテーブル |
| Materialized view | 事前計算して保存するビュー。増分更新できる |
| Expectation | パイプラインのデータ品質ルール(expect / drop / fail) |
| Lakeflow Jobs | ジョブのオーケストレーション(旧 Workflows) |
| File arrival trigger | 新ファイル到着で起動するトリガー |
| Table update trigger | UC テーブル更新で起動するトリガー |
| DABs | `databricks.yml` でリソースをコード化して昇格させる仕組み |
| Git folder | ワークスペース内の Git 連携フォルダー(旧 Repos) |
| AQE | 実行時にプランを最適化する Adaptive Query Execution |
| Data skew | 特定キー/パーティションへのデータ偏り |
| Liquid Clustering | `CLUSTER BY` による柔軟なレイアウト。ZORDER/パーティションの代替 |
| Predictive Optimization | 保守処理の自動実行 |
| Row filter / Column mask | 行の絞り込み / 値のマスキング |
| ABAC | タグとポリシーによる属性ベースのアクセス制御 |

## 直前チェックリスト
- [ ] 試験は 45問(採点対象) / 90分 / 択一。配点は Transformation 22%、Ingestion 21%、Jobs 16%、Governance 15%
- [ ] 旧名と新名: DLT=Lakeflow Spark Declarative Pipelines、Workflows=Lakeflow Jobs、DABs=Declarative Automation Bundles
- [ ] COPY INTO は冪等、Auto Loader は大量ファイル向け、SaaS/DB は Lakeflow Connect のマネージドコネクタ
- [ ] Auto Loader のスキーマ進化 4モードと既定値(未指定=`addNewColumns`、スキーマ指定=`none`)
- [ ] `schemaLocation`(スキーマ)と `checkpointLocation`(進捗)の違い
- [ ] `union()` は重複を除去しない。`unionByName()` は列名で結合
- [ ] `shuffle.partitions` 既定200、`autoBroadcastJoinThreshold` 既定10MB
- [ ] expect / expect_or_drop / expect_or_fail の違い
- [ ] トリガー4種(スケジュール・ファイル到着・テーブル更新・継続)の使い分け
- [ ] `databricks bundle validate → deploy → run` と `BUNDLE_VAR_`
- [ ] スキューは「Max ≫ Median」。ノード増設では解決しない。ソルティングで分散
- [ ] Liquid Clustering は `CLUSTER BY`。ZORDER/パーティションの代替
- [ ] 外部テーブルは DROP してもデータが残る。`SET MANAGED` / `UNSET MANAGED`
- [ ] 列マスクは `is_account_group_member()` などで分岐。ABAC はガバナンスタグ + ポリシー
- [ ] VACUUM 既定7日

## 出典
- [Databricks Certified Data Engineer Associate(公式試験ページ)](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Exam Guide August 2026(公式PDF)](https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf)
- [Auto Loader: スキーマ推論と進化](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)
- [テーブル更新トリガー](https://docs.databricks.com/en/jobs/trigger-table-update)
- [パイプラインの Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [ABAC(Unity Catalog)](https://docs.databricks.com/en/data-governance/unity-catalog/abac.html)

## 出典で裏取りできていない項目
- 合格基準(スコア)は公式ガイドに記載がない（要確認）
- VACUUM の既定7日、`shuffle.partitions` 200、`autoBroadcastJoinThreshold` 10MB は一般的な既定値で、今回の検索では一次資料の再確認をしていない（要確認）
- 管理テーブルの DROP 後の UNDROP 猶予は今回未確認（要確認）
- DENY の挙動の詳細は公式ガイドが列挙しているのみで、仕様を未確認（要確認）
