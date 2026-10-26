# Databricks Certified Data Engineer Associate 学習キット

Databricks Certified Data Engineer Associate(2026年5月4日版、公式ガイド 2026-08-06 更新)の学習用に作った、要点メモと模擬問題集です。

## 構成

| パス | 内容 |
|---|---|
| `notes/databricks-dea-notes.md` | 要点メモ。試験概要、7分野の要点・比較表・ひっかけポイント、用語集、直前チェックリスト |
| `quiz/databricks-dea-quiz.html` | 問題集(40問)。ダブルクリックでブラウザで開く。外部通信なし |
| `quiz/databricks-dea-questions.json` | 問題集の元データ。問題を直すときはこれを編集する |
| `scripts/build_quiz.py` | `questions.json` を検証して HTML を生成するスクリプト |
| `assets/quiz-template.html` | HTML のテンプレート(`build_quiz.py` が読み込む) |

## 問題集の使い方

- 練習モード: 1問ごとに採点と解説が出ます。
- 本番モード: 80分のタイマー付きで、最後にまとめて採点します。分野別の成績も出ます。
- 問題と選択肢のシャッフル、間違えた問題だけの再挑戦、進捗のブラウザ保存に対応しています。

## 問題を直して再ビルドする

```bash
python3 scripts/build_quiz.py quiz/databricks-dea-questions.json quiz/databricks-dea-quiz.html
```

id の重複、`answer` の範囲外、解説の欠落があるとエラーで止まります。

## 出題の配分

| 分野 | 配点 | 問題数 |
|---|---|---|
| Databricks Intelligence Platform | 6% | 2 |
| Data Ingestion and Loading | 21% | 9 |
| Data Transformation and Modeling | 22% | 9 |
| Working with Lakeflow Jobs | 16% | 6 |
| Implementing CI/CD | 10% | 4 |
| Troubleshooting, Monitoring, and Optimization | 10% | 4 |
| Governance and Security | 15% | 6 |

## 注意

- 合格スコアは公式ガイドに記載がないため、問題集の合格目安は仮に70%です。
- 要点メモの `（要確認）` は、公式ドキュメントで裏取りできていない項目です。
- 問題はすべてオリジナルです。実際の試験問題の再現ではありません。
- 受験前に、必ず[公式試験ページ](https://www.databricks.com/learn/certification/data-engineer-associate)で最新の試験ガイドを確認してください。
