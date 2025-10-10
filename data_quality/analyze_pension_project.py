"""
基礎年金給付に必要な経費の予算・執行・支出分析

このスクリプトは「基礎年金給付に必要な経費」という事業について、
全年度の予算額、執行額、支出先を分析してレポートを生成します。
"""
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PROCESSED_DIR

# レポート出力先
REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)

def analyze_pension_project():
    """基礎年金給付に必要な経費の分析"""

    # レポートファイルを開く
    report_file = REPORT_DIR / "pension_project_analysis.md"

    def output(text="", file_only=False):
        """コンソールとファイルの両方に出力"""
        if not file_only:
            print(text)
        with open(report_file, 'a', encoding='utf-8') as f:
            f.write(text + '\n')

    # レポートファイルを初期化
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('')

    output("# 基礎年金給付に必要な経費 - 予算・執行・支出分析レポート")
    output()
    output(f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output()
    output("---")
    output()

    results = []

    # 全年度を処理
    for year in range(2014, 2024):
        year_dir = PROCESSED_DIR / f"year_{year}"

        if not year_dir.exists():
            continue

        # 1-2: 事業概要ファイルから事業を検索
        overview_file = year_dir / f"1-2_{year}_基本情報_事業概要.csv"
        if not overview_file.exists():
            continue

        df_overview = pd.read_csv(overview_file, encoding='utf-8-sig')

        # 事業名で検索（部分一致）
        pension_projects = df_overview[
            df_overview['事業名'].str.contains('基礎年金給付に必要な経費', na=False)
        ]

        if len(pension_projects) == 0:
            continue

        output()
        output(f"## {year}年度")
        output()

        for idx, project in pension_projects.iterrows():
            project_id = project['予算事業ID']
            project_name = project['事業名']
            ministry = project.get('府省庁', '')
            bureau = project.get('局・庁', '')

            output(f"**事業名**: {project_name}  ")
            output(f"**予算事業ID**: {project_id}  ")
            output(f"**府省庁**: {ministry}  ")
            output(f"**局・庁**: {bureau}  ")
            output()

            # 2-1: 予算・執行データを取得
            budget_file = year_dir / f"2-1_{year}_予算・執行_サマリ.csv"
            if budget_file.exists():
                df_budget = pd.read_csv(budget_file, encoding='utf-8-sig')
                project_budgets = df_budget[df_budget['予算事業ID'] == project_id]

                if len(project_budgets) > 0:
                    output("### 予算・執行データ")
                    output()
                    output("| 年度 | 当初予算 | 執行額 | 執行率 | 計(歳出予算現額) |")
                    output("|-----:|-------------:|-------------:|-------:|-----------------:|")

                    total_budget = 0
                    total_execution = 0

                    for _, budget in project_budgets.iterrows():
                        budget_year = budget.get('予算年度', '')
                        initial_budget = budget.get('当初予算(合計)', 0)
                        execution = budget.get('執行額(合計)', 0)
                        execution_rate = budget.get('執行率', 0)
                        total_amount = budget.get('計(歳出予算現額合計)', 0)

                        # 数値に変換
                        try:
                            initial_budget = float(initial_budget) if pd.notna(initial_budget) else 0
                            execution = float(execution) if pd.notna(execution) else 0
                            execution_rate = float(execution_rate) if pd.notna(execution_rate) else 0
                            total_amount = float(total_amount) if pd.notna(total_amount) else 0
                        except:
                            pass

                        total_budget += initial_budget
                        total_execution += execution

                        output(f"| {budget_year} | {initial_budget:,.0f} | {execution:,.0f} | {execution_rate:.1f}% | {total_amount:,.0f} |")

                    avg_execution_rate = (total_execution / total_budget * 100) if total_budget > 0 else 0
                    output(f"| **合計** | **{total_budget:,.0f}** | **{total_execution:,.0f}** | **{avg_execution_rate:.1f}%** | |")
                    output()
                    output("※単位: 百万円")
                    output()

            # 5-1: 支出先データを取得
            expenditure_file = year_dir / f"5-1_{year}_支出先_支出情報.csv"
            if expenditure_file.exists():
                df_expenditure = pd.read_csv(expenditure_file, encoding='utf-8-sig')
                project_expenditures = df_expenditure[df_expenditure['予算事業ID'] == project_id]

                if len(project_expenditures) > 0:
                    output("### 支出先上位10者")
                    output()
                    output("| 番号 | 支出先名 | 支出額(百万円) | 契約方式 |")
                    output("|-----:|:---------|---------------:|:---------|")

                    total_expenditure = 0

                    # 支出額でソート
                    project_expenditures = project_expenditures.sort_values(
                        '支出額（百万円）', ascending=False, na_position='last'
                    )

                    for _, exp in project_expenditures.head(10).iterrows():
                        exp_num = exp.get('支出先番号', '')
                        exp_name = exp.get('支出先名', '')
                        exp_amount = exp.get('支出額（百万円）', 0)
                        contract_method = exp.get('契約方式等', '')

                        try:
                            exp_amount = float(exp_amount) if pd.notna(exp_amount) else 0
                        except:
                            exp_amount = 0

                        total_expenditure += exp_amount

                        # 支出先名を表示
                        exp_name_display = str(exp_name) if pd.notna(exp_name) else ''
                        contract_display = str(contract_method) if pd.notna(contract_method) else ''

                        output(f"| {exp_num} | {exp_name_display} | {exp_amount:,.0f} | {contract_display} |")

                    output(f"| | **支出先合計(上位10者)** | **{total_expenditure:,.0f}** | |")
                    output()
                    output("※単位: 百万円")
                    output()

            # 結果を保存
            result = {
                '年度': year,
                '事業ID': project_id,
                '事業名': project_name,
                '府省庁': ministry,
                '局・庁': bureau,
                '予算データ件数': len(project_budgets) if budget_file.exists() else 0,
                '支出先件数': len(project_expenditures) if expenditure_file.exists() else 0,
            }
            results.append(result)

    # サマリー
    if results:
        output()
        output("---")
        output()
        output("## 全年度サマリー")
        output()

        df_summary = pd.DataFrame(results)
        output(f"- **対象年度数**: {len(df_summary)}")
        output(f"- **総予算データ件数**: {df_summary['予算データ件数'].sum()}")
        output(f"- **総支出先件数**: {df_summary['支出先件数'].sum()}")
        output()

        output("### 年度別データ件数")
        output()
        output("| 年度 | 府省庁 | 予算データ件数 | 支出先件数 |")
        output("|-----:|:-------|---------------:|-----------:|")
        for _, row in df_summary.iterrows():
            output(f"| {row['年度']} | {row['府省庁']} | {row['予算データ件数']} | {row['支出先件数']} |")
    else:
        output()
        output("該当する事業が見つかりませんでした。")

    output()
    output("---")
    output()
    output(f"**レポートファイル**: `{report_file}`")
    output()

    print(f"\n✅ レポート生成完了: {report_file}")

if __name__ == '__main__':
    analyze_pension_project()
