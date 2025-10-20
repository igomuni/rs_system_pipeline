# データ品質確認ツール

処理済みデータの品質を確認・レポート生成するためのツール群

## ディレクトリ構成

```
data_quality/
├── README.md                              # このファイル
│
├── 【品質レポート生成】
├── generate_quality_report.py            # 年度別品質レポート生成（メインツール）
├── create_column_matrix.py               # RS2024と過去データの列名対応マトリクス
├── analyze_historical_data.py            # 過去データ構造分析
├── analyze_mapping_opportunities.py      # 列名マッピング改善機会分析
├── analyze_rs_conversion_gap.py          # RSシステム形式変換ギャップ分析
├── validate_budget_continuity.py         # 予算執行データ継続性検証
│
├── 【データ分析ツール】
├── analyze_pension_project.py            # 年金事業の詳細分析
├── analyze_olympic_projects.py           # オリンピック関連事業分析
│
├── 【サマリーツール】
├── check_totals.py                       # 年度別予算・執行額の簡易チェック
├── summary_report.py                     # 予算・執行額サマリーレポート
├── expenditure_summary.py                # 支出先データサマリーレポート
│
├── reports/                              # 生成されたレポート保存先
│   ├── DATA_QUALITY_REPORT.md            # 全年度統合品質レポート
│   ├── quality_report_2014.md            # 年度別詳細レポート
│   ├── quality_report_2015.md
│   ├── ...
│   ├── column_matrix_report.md           # 列名対応マトリクス
│   ├── historical_data_analysis_report.md # 過去データ構造分析
│   ├── mapping_opportunities.md          # マッピング改善機会
│   ├── rs_conversion_gap_2023.md         # RSシステム変換ギャップ
│   ├── budget_continuity_validation.md   # 予算継続性検証
│   └── pension_project_analysis.md       # 年金事業分析
│
├── investigations/                       # 調査用スクリプト（長音修正調査等）
│   ├── analyze_all_years_longvowel.py   # 全年度長音分析
│   ├── check_missing_longvowel_words.py # 長音単語の欠落チェック
│   ├── export_all_longvowel_words_csv.py # 長音単語CSV出力
│   ├── generate_preserve_words_list.py  # 長音保持単語リスト生成
│   ├── identify_words_to_restore_longvowel.py # 長音復元対象単語特定
│   ├── investigate_diet_member_budget.py # 国会議員予算調査
│   ├── investigate_hyphen_longvowel_mixture.py # ハイフン・長音混在調査
│   ├── investigate_raw_hyphen_misuse.py # rawデータのハイフン誤用調査
│   ├── test_neologdn_behavior.py        # neologdn動作検証
│   ├── test_our_normalization.py        # 正規化処理検証
│   └── verify_hyphen_to_longvowel_fix.py # ハイフン→長音修正検証
│
└── archives/                             # 調査で生成された中間ファイル
    ├── all_longvowel_words_2014-2023.csv # 全長音単語リスト
    ├── raw_hyphen_misuse_2014-2023.csv  # ハイフン誤用データ
    └── PRESERVE_LONG_VOWEL_WORDS*.txt   # 長音保持単語候補（廃止）
```

## 使用方法

### 1. 年度別品質レポート生成（推奨）

全年度の詳細な品質レポートを一括生成します。

```bash
python data_quality/generate_quality_report.py
```

**出力**:
- `data_quality/reports/DATA_QUALITY_REPORT.md`: **全年度統合レポート**
  - 年度別基本統計一覧
  - 予算・執行データ一覧
  - 支出先データ一覧
  - 検出された品質問題一覧（事業名・府省庁付き）
  - 推奨事項
- `data_quality/reports/quality_report_YYYY.md`: 各年度の詳細レポート
  - 基本情報（事業数、ファイルサイズ）
  - 予算・執行データの統計
  - 支出先データの統計
  - 検出された品質問題

**品質チェック項目**:
- ✅ 予算合計の異常値検出（100兆円以上）
- ✅ 単一事業の予算異常値検出（1兆円以上）
- ✅ 支出額合計の異常値検出（50兆円以上）
- ✅ 平均支出額の異常値検出（100億円以上）

### 2. 列名対応マトリクス生成

RS2024と過去データ（2014-2023）の列名対応状況をマトリクス形式で分析します。

```bash
python data_quality/create_column_matrix.py
```

**出力**: `data_quality/reports/column_matrix_report.md`
- テーブル別の列名存在マトリクス
- 年度別対応率
- 列名正規化とマッピングルール適用済み

### 3. 過去データ構造分析

全年度のファイル構造と列名パターンを分析します。

```bash
python data_quality/analyze_historical_data.py
```

**出力**: `data_quality/reports/historical_data_analysis_report.md`
- 年度別ファイル構成
- テーブル別列名の変遷
- 列数推移グラフ（Markdown表形式）

### 4. マッピング改善機会分析

列名変換マップで改善可能な箇所を自動検出します。

```bash
python data_quality/analyze_mapping_opportunities.py
```

**出力**: `data_quality/reports/mapping_opportunities.md`
- 対応率が低いテーブルの列名差分
- RS2024と過去データの類似列名候補（自動検出）
- 推奨度付きマッピング候補リスト

### 5. RSシステム変換ギャップ分析

2023年データをRS2024形式に変換する際のギャップを分析します。

```bash
python data_quality/analyze_rs_conversion_gap.py
```

**出力**: `data_quality/reports/rs_conversion_gap_2023.md`
- 新規作成が必要なファイル一覧
- 既存ファイルのカラム差分
- フェーズ別実装方針

### 6. 予算執行データ継続性検証

2019-2024年の予算データについて、年度間の継続性を検証します。

```bash
python data_quality/validate_budget_continuity.py
```

**出力**: `data_quality/reports/budget_continuity_validation.md`
- 年度間の事業継続率
- 全年度継続事業リスト
- 予算変遷追跡の妥当性
- データ異常値検出

### 7. 特定事業の詳細分析

#### 年金事業分析

基礎年金給付事業の10年間の予算・執行・支出データを詳細分析します。

```bash
python data_quality/analyze_pension_project.py
```

**出力**: `data_quality/reports/pension_project_analysis.md`

#### オリンピック関連事業分析

オリンピック・パラリンピック関連事業を抽出し、予算推移を分析します（標準出力）。

```bash
python data_quality/analyze_olympic_projects.py
```

### 8. 予算・執行額サマリー

10年分の予算・執行額を一覧表示します（標準出力）。

```bash
python data_quality/summary_report.py
```

**出力例**:
```
年度       事業数                  当初予算(10億円)            執行額(10億円)          執行率(%)
----------------------------------------------------------------------------------------------------
2014     4,739               1,784,582.6          2,313,790.5          129.7% ⚠️ 異常値
2015     5,365                  34,526.1             35,399.3          102.5%
...
```

### 9. 支出先データサマリー

10年分の支出先データを一覧表示します（標準出力）。

```bash
python data_quality/expenditure_summary.py
```

**出力例**:
```
年度              支出先件数            支出額合計(10億円)             平均支出額(百万円)
------------------------------------------------------------------------------------------------------------------------
2014           67,436          18,198,445.62           2,698,624.71 ⚠️ 異常値
2015           71,544              15,637.41               2,185.70
...
```

### 10. 簡易チェック

年度別の予算総額と執行額を簡易表示します（デバッグ用、標準出力）。

```bash
python data_quality/check_totals.py
```

## レポート解説

### 生成される品質レポートの見方

#### 基本情報
- **総事業数**: その年度の事業数
- **ファイルサイズ**: 基本情報ファイルのサイズ（処理効率の目安）

#### 予算・執行データ
- **当初予算合計**: 全事業の当初予算の合計（10億円単位）
- **執行率**: 執行額 ÷ 当初予算 × 100
  - 正常値: 100-110%程度
  - 高値の要因: 補正予算、繰越予算
- **予算最大値/最小値**: 異常値検出の参考

#### 支出先データ
- **支出先件数**: 支出先の総件数
- **支出額合計**: 全支出先への支出額合計（10億円単位）
- **平均支出額**: 1件あたりの平均支出額（百万円単位）
  - 正常値: 1,000-2,000百万円程度

#### 品質問題の重大度

- **高**: データの信頼性に重大な影響（単位間違いなど）
- **中**: 注意が必要だが、一部データのみに影響
- **低**: 軽微な問題

## 既知の品質問題

### 2014年度
- **予算データ**: 一部事業で100万倍の単位間違い
- **支出データ**: 約18,000兆円（正常値の約1,800倍）
- **原因**: 百万円単位のところに円単位で入力
- **影響事業**: 約10事業（文部科学省の原子力関連事業）

### 2016年度
- **支出データ**: 約115兆円（正常値の約11倍）
- **原因**: 調査中
- **影響事業**: 特定が必要

### 2020年度
- **予算データ**: ~~データ欠損~~（✅ 修正済）
- **修正内容**: 正規表現パターンを修正し、5,445件のレコードを抽出

## 開発者向け情報

### 新しいチェック項目の追加

`generate_quality_report.py`の`generate_year_report()`関数に追加:

```python
# 例：執行率が200%を超える場合
if report['budget']['執行率(%)'] > 200:
    report['quality_issues'].append({
        'カテゴリ': '予算',
        '重大度': '中',
        '問題': f'執行率が異常に高い ({report["budget"]["執行率(%)"]}%)'
    })
```

### レポート形式のカスタマイズ

`save_year_report_md()`関数を編集してMarkdown出力をカスタマイズできます。

### 新しい分析ツールの追加

1. `data_quality/` 直下に Python スクリプトを作成
2. レポート出力先は `data_quality/reports/` 配下に設定
3. このREADME.mdにツールの説明を追加

```python
# レポート出力例
from pathlib import Path

project_root = Path(__file__).parent.parent
report_dir = project_root / "data_quality" / "reports"
output_file = report_dir / "my_analysis_report.md"

with open(output_file, "w", encoding="utf-8") as f:
    f.write("# My Analysis Report\n\n")
    # レポート内容を書き込み
```

## 注意事項

- レポート生成には`output/processed/`に処理済みデータが必要です
- 生成されたレポートは`data_quality/reports/`に保存されます
- 既存のレポートは上書きされます
- 金額単位:
  - 10億円 = 0.01兆円
  - 1兆円 = 1,000 × 10億円
  - 百万円 = 0.001 × 10億円

## 関連ドキュメント

- [reports/DATA_QUALITY_REPORT.md](reports/DATA_QUALITY_REPORT.md): 全年度統合品質レポート（自動生成）
- [../CHANGELOG.md](../CHANGELOG.md): 修正履歴
- [../CLAUDE.md](../CLAUDE.md): プロジェクト全体のガイド

## レポート例

### 統合レポート（reports/DATA_QUALITY_REPORT.md）

全年度のデータを一覧化した統合レポートです：

- **年度別基本統計**: 事業数、レコード数、ファイルサイズを一覧表示
- **予算・執行データサマリー**: 予算・執行額を10年分比較
- **支出先データサマリー**: 支出先件数と支出額を10年分比較
- **品質問題一覧**: 検出された全問題を重大度順に表示（事業名・府省庁を含む）
- **推奨事項**: 緊急対応が必要な年度と長期的改善項目

### 年度別レポート（reports/quality_report_YYYY.md）

各年度の詳細な統計情報と品質問題を記載：

- 基本情報: 総事業数、ファイルサイズ
- 予算・執行データ: 合計、執行率、最大値/最小値
- 支出先データ: 件数、合計、平均、最大値/最小値
- 品質問題: その年度で検出された具体的な問題
