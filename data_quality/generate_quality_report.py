#!/usr/bin/env python3
"""
データ品質レポート生成ツール
年度別の品質レポートと全体サマリーを生成
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

OUTPUT_DIR = Path("output/processed")
REPORT_DIR = Path("data_quality/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def generate_overall_summary():
    """全体サマリーレポート生成"""
    print("=" * 120)
    print("データ品質レポート - 全体サマリー")
    print(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 120)
    print()

    summary_data = {
        'basic': [],
        'budget': [],
        'expenditure': []
    }

    # 予算・執行データ
    for year in range(2014, 2024):
        year_dir = OUTPUT_DIR / f"year_{year}"
        overview_file = year_dir / f"1-2_{year}_基本情報_事業概要.csv"
        budget_file = year_dir / f"2-1_{year}_予算・執行_サマリ.csv"
        exp_file = year_dir / f"5-1_{year}_支出先_支出情報.csv"

        # 基本情報
        business_count = 0
        if overview_file.exists():
            df_ov = pd.read_csv(overview_file, low_memory=False)
            business_count = len(df_ov)

        # 予算レコード数とファイルサイズ
        budget_records = 0
        exp_records = 0
        total_size_mb = 0

        if overview_file.exists():
            total_size_mb += overview_file.stat().st_size / (1024 * 1024)
        if budget_file.exists():
            df_budget = pd.read_csv(budget_file, low_memory=False)
            budget_records = len(df_budget)
            total_size_mb += budget_file.stat().st_size / (1024 * 1024)
        if exp_file.exists():
            df_exp = pd.read_csv(exp_file, low_memory=False)
            exp_records = len(df_exp)
            total_size_mb += exp_file.stat().st_size / (1024 * 1024)

        summary_data['basic'].append({
            '年度': year,
            '事業数': business_count,
            '予算レコード': budget_records,
            '支出先件数': exp_records,
            'ファイル合計(MB)': int(round(total_size_mb))
        })

        if budget_file.exists():
            df = pd.read_csv(budget_file, low_memory=False)
            current_year = df[df['予算年度'] == year]

            if len(current_year) > 0:
                initial_budget = pd.to_numeric(current_year['当初予算(合計)'], errors='coerce').fillna(0).sum()
                execution = pd.to_numeric(current_year['執行額(合計)'], errors='coerce').fillna(0).sum()
                execution_rate = (execution / initial_budget * 100) if initial_budget > 0 else 0

                summary_data['budget'].append({
                    '年度': year,
                    '事業数': len(current_year),
                    '当初予算(10億円)': initial_budget / 10000,
                    '執行額(10億円)': execution / 10000,
                    '執行率(%)': execution_rate
                })

        if exp_file.exists():
            df = pd.read_csv(exp_file, low_memory=False)
            total_exp = pd.to_numeric(df['支出額（百万円）'], errors='coerce').fillna(0).sum()
            avg_exp = pd.to_numeric(df['支出額（百万円）'], errors='coerce').fillna(0).mean()

            summary_data['expenditure'].append({
                '年度': year,
                '支出先件数': len(df),
                '支出額合計(10億円)': total_exp / 10000,
                '平均支出額(百万円)': avg_exp
            })

    return summary_data


def generate_year_report(year):
    """年度別詳細レポート生成"""
    year_dir = OUTPUT_DIR / f"year_{year}"

    if not year_dir.exists():
        return None

    report = {
        'year': year,
        'overview': {},
        'budget': {},
        'expenditure': {},
        'quality_issues': []
    }

    # 1. 基本情報
    overview_file = year_dir / f"1-2_{year}_基本情報_事業概要.csv"
    if overview_file.exists():
        df = pd.read_csv(overview_file, low_memory=False)
        report['overview'] = {
            '総事業数': len(df),
            'ファイルサイズ(MB)': overview_file.stat().st_size / 1024 / 1024
        }

    # 2. 予算・執行データ
    budget_file = year_dir / f"2-1_{year}_予算・執行_サマリ.csv"
    if budget_file.exists():
        df = pd.read_csv(budget_file, low_memory=False)
        current_year = df[df['予算年度'] == year]

        if len(current_year) > 0:
            initial_budget = pd.to_numeric(current_year['当初予算(合計)'], errors='coerce').fillna(0)
            execution = pd.to_numeric(current_year['執行額(合計)'], errors='coerce').fillna(0)

            report['budget'] = {
                'レコード数': len(current_year),
                '当初予算合計(10億円)': initial_budget.sum() / 10000,
                '執行額合計(10億円)': execution.sum() / 10000,
                '執行率(%)': (execution.sum() / initial_budget.sum() * 100) if initial_budget.sum() > 0 else 0,
                '予算最大値(百万円)': initial_budget.max(),
                '予算最小値(百万円)': initial_budget[initial_budget > 0].min() if (initial_budget > 0).any() else 0,
                'ファイルサイズ(MB)': budget_file.stat().st_size / 1024 / 1024
            }

            # 品質チェック
            if initial_budget.sum() / 10000 > 100000:  # 100兆円以上
                report['quality_issues'].append({
                    'カテゴリ': '予算',
                    '重大度': '高',
                    '問題': f'当初予算合計が異常に大きい ({initial_budget.sum() / 10000:,.1f} 10億円)'
                })

            if initial_budget.max() > 1000000:  # 1兆円以上の単一事業
                # 異常値を持つ事業を特定
                max_idx = initial_budget.idxmax()
                business_name = current_year.loc[max_idx, '事業名'] if '事業名' in current_year.columns else '不明'
                ministry = current_year.loc[max_idx, '府省庁'] if '府省庁' in current_year.columns else '不明'

                report['quality_issues'].append({
                    'カテゴリ': '予算',
                    '重大度': '高',
                    '問題': f'異常に大きい予算の事業が存在 ({initial_budget.max():,.1f} 百万円)',
                    '事業名': business_name,
                    '府省庁': ministry,
                    '金額': initial_budget.max()
                })

    # 3. 支出先データ
    exp_file = year_dir / f"5-1_{year}_支出先_支出情報.csv"
    if exp_file.exists():
        df = pd.read_csv(exp_file, low_memory=False)
        exp_amounts = pd.to_numeric(df['支出額（百万円）'], errors='coerce').fillna(0)

        report['expenditure'] = {
            '支出先件数': len(df),
            '支出額合計(10億円)': exp_amounts.sum() / 10000,
            '平均支出額(百万円)': exp_amounts.mean(),
            '支出額最大値(百万円)': exp_amounts.max(),
            '支出額最小値(百万円)': exp_amounts[exp_amounts > 0].min() if (exp_amounts > 0).any() else 0,
            'ファイルサイズ(MB)': exp_file.stat().st_size / 1024 / 1024
        }

        # 品質チェック
        if exp_amounts.sum() / 10000 > 50000:  # 50兆円以上
            report['quality_issues'].append({
                'カテゴリ': '支出',
                '重大度': '高',
                '問題': f'支出額合計が異常に大きい ({exp_amounts.sum() / 10000:,.1f} 10億円)'
            })

        if exp_amounts.mean() > 10000:  # 平均100億円以上
            report['quality_issues'].append({
                'カテゴリ': '支出',
                '重大度': '中',
                '問題': f'平均支出額が異常に大きい ({exp_amounts.mean():,.1f} 百万円)'
            })

    return report


def generate_consolidated_report(all_reports, summary_data):
    """全年度の統合レポートを生成"""
    output_file = Path("data_quality") / "DATA_QUALITY_REPORT.md"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# データ品質レポート（統合版）\n\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("2014-2023年度の行政事業レビューデータの品質を分析した統合レポートです。\n\n")

        # 1. 年度別基本統計一覧
        f.write("## 年度別基本統計\n\n")
        f.write("| 年度 | 事業数 | 予算レコード | 支出先件数 | ファイル合計(MB) |\n")
        f.write("|------|--------|--------------|------------|------------------|\n")

        for basic in summary_data['basic']:
            f.write(f"| {basic['年度']} | {basic['事業数']:,} | {basic['予算レコード']:,} | {basic['支出先件数']:,} | {basic['ファイル合計(MB)']} |\n")

        f.write("\n")

        # 2. 予算・執行データ一覧
        f.write("## 予算・執行データサマリー\n\n")
        f.write("| 年度 | 当初予算(10億円) | 執行額(10億円) | 執行率(%) | 予算最大値(百万円) |\n")
        f.write("|------|------------------|----------------|-----------|--------------------|\n")

        for report in all_reports:
            year = report['year']
            if report['budget']:
                budget = report['budget'].get('当初予算合計(10億円)', 0)
                execution = report['budget'].get('執行額合計(10億円)', 0)
                rate = report['budget'].get('執行率(%)', 0)
                max_budget = report['budget'].get('予算最大値(百万円)', 0)

                flag = " ⚠️" if budget > 100000 or max_budget > 1000000 else ""
                f.write(f"| {year} | {budget:,.1f} | {execution:,.1f} | {rate:.1f} | {max_budget:,.1f}{flag} |\n")

        f.write("\n注：金額は百万円を10億円に換算（1兆円 = 1,000 × 10億円）\n\n")

        # 3. 支出先データ一覧
        f.write("## 支出先データサマリー\n\n")
        f.write("| 年度 | 支出先件数 | 支出額合計(10億円) | 平均支出額(百万円) |\n")
        f.write("|------|------------|--------------------|--------------------|")
        f.write("\n")

        for report in all_reports:
            year = report['year']
            if report['expenditure']:
                count = report['expenditure'].get('支出先件数', 0)
                total = report['expenditure'].get('支出額合計(10億円)', 0)
                avg = report['expenditure'].get('平均支出額(百万円)', 0)

                flag = " ⚠️" if total > 50000 or avg > 10000 else ""
                f.write(f"| {year} | {count:,} | {total:,.1f} | {avg:,.2f}{flag} |\n")

        f.write("\n注：支出額合計は百万円を10億円に換算、平均支出額は百万円単位\n\n")

        # 4. 品質問題一覧
        f.write("## 検出された品質問題\n\n")

        # 全年度の品質問題を収集
        all_issues = []
        for report in all_reports:
            for issue in report['quality_issues']:
                issue_copy = issue.copy()
                issue_copy['年度'] = report['year']
                all_issues.append(issue_copy)

        if all_issues:
            # 重大度でソート
            severity_order = {'高': 0, '中': 1, '低': 2}
            all_issues.sort(key=lambda x: (severity_order.get(x['重大度'], 3), x['年度']))

            # テーブルヘッダー
            f.write("| 年度 | カテゴリ | 重大度 | 事業名 | 府省庁 | 金額(百万円) | 問題内容 |\n")
            f.write("|------|----------|--------|--------|--------|--------------|----------|\n")

            for issue in all_issues:
                year = issue['年度']
                category = issue['カテゴリ']
                severity = issue['重大度']
                business = issue.get('事業名', '-')
                ministry = issue.get('府省庁', '-')
                amount = f"{issue['金額']:,.0f}" if '金額' in issue else '-'

                # 問題内容を短縮表示
                problem = issue['問題']
                # 具体的な金額を含む部分を除去してコンパクトに
                if '(' in problem:
                    problem = problem.split('(')[0].strip()

                f.write(f"| {year} | {category} | {severity} | {business} | {ministry} | {amount} | {problem} |\n")

            f.write("\n")
        else:
            f.write("✅ 重大な品質問題は検出されませんでした。\n\n")

        # 5. 推奨事項
        f.write("## 推奨事項\n\n")
        f.write("### 緊急対応が必要な項目\n\n")

        high_severity_years = set()
        for report in all_reports:
            for issue in report['quality_issues']:
                if issue['重大度'] == '高':
                    high_severity_years.add(report['year'])

        if high_severity_years:
            for year in sorted(high_severity_years):
                f.write(f"- **{year}年度**: データの単位間違いの可能性が高いため、元データの確認と修正が必要\n")
        else:
            f.write("現時点で緊急対応が必要な項目はありません。\n")

        f.write("\n### 長期的な改善項目\n\n")
        f.write("- データ入力時の単位チェック機能の実装\n")
        f.write("- 異常値の自動検出とアラート機能の強化\n")
        f.write("- 年度間のデータ一貫性チェックの実施\n\n")

    return output_file


def save_year_report_md(report):
    """年度別レポートをMarkdown形式で保存"""
    year = report['year']
    output_file = REPORT_DIR / f"quality_report_{year}.md"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# データ品質レポート - {year}年度\n\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # 基本情報
        if report['overview']:
            f.write("## 基本情報\n\n")
            f.write(f"- 総事業数: {report['overview']['総事業数']:,}件\n")
            f.write(f"- ファイルサイズ: {report['overview']['ファイルサイズ(MB)']:.2f} MB\n\n")

        # 予算・執行データ
        if report['budget']:
            f.write("## 予算・執行データ\n\n")
            f.write(f"- レコード数: {report['budget']['レコード数']:,}件\n")
            f.write(f"- 当初予算合計: {report['budget']['当初予算合計(10億円)']:,.1f} 10億円 ({report['budget']['当初予算合計(10億円)']/1000:.2f}兆円)\n")
            f.write(f"- 執行額合計: {report['budget']['執行額合計(10億円)']:,.1f} 10億円 ({report['budget']['執行額合計(10億円)']/1000:.2f}兆円)\n")
            f.write(f"- 執行率: {report['budget']['執行率(%)']:.1f}%\n")
            f.write(f"- 予算最大値: {report['budget']['予算最大値(百万円)']:,.1f} 百万円\n")
            f.write(f"- 予算最小値: {report['budget']['予算最小値(百万円)']:,.6f} 百万円\n")
            f.write(f"- ファイルサイズ: {report['budget']['ファイルサイズ(MB)']:.2f} MB\n\n")

        # 支出先データ
        if report['expenditure']:
            f.write("## 支出先データ\n\n")
            f.write(f"- 支出先件数: {report['expenditure']['支出先件数']:,}件\n")
            f.write(f"- 支出額合計: {report['expenditure']['支出額合計(10億円)']:,.1f} 10億円 ({report['expenditure']['支出額合計(10億円)']/1000:.2f}兆円)\n")
            f.write(f"- 平均支出額: {report['expenditure']['平均支出額(百万円)']:,.2f} 百万円\n")
            f.write(f"- 支出額最大値: {report['expenditure']['支出額最大値(百万円)']:,.1f} 百万円\n")
            f.write(f"- 支出額最小値: {report['expenditure']['支出額最小値(百万円)']:,.6f} 百万円\n")
            f.write(f"- ファイルサイズ: {report['expenditure']['ファイルサイズ(MB)']:.2f} MB\n\n")

        # 品質問題
        if report['quality_issues']:
            f.write("## 検出された品質問題\n\n")
            for issue in report['quality_issues']:
                f.write(f"### {issue['カテゴリ']}（重大度: {issue['重大度']}）\n")
                f.write(f"{issue['問題']}\n\n")
        else:
            f.write("## 品質問題\n\n")
            f.write("✅ 重大な品質問題は検出されませんでした。\n\n")

    return output_file


def main():
    """メイン処理"""
    print("\n📊 データ品質レポート生成を開始します...\n")

    # サマリーデータ生成
    summary_data = generate_overall_summary()

    # 年度別レポート生成
    print("📝 年度別詳細レポート生成中...\n")
    generated_files = []
    all_reports = []

    for year in range(2014, 2024):
        report = generate_year_report(year)
        if report:
            all_reports.append(report)
            output_file = save_year_report_md(report)
            generated_files.append(output_file)

            issues = len(report['quality_issues'])
            status = "⚠️" if issues > 0 else "✅"
            print(f"{status} {year}年度: {issues}件の品質問題")

    print(f"\n✅ 年度別レポート生成完了: {len(generated_files)}件")
    print(f"📁 保存先: {REPORT_DIR}/")

    # 統合レポート生成
    print("\n📊 統合レポート生成中...")
    consolidated_file = generate_consolidated_report(all_reports, summary_data)
    print(f"✅ 統合レポート生成完了")
    print(f"📄 保存先: {consolidated_file}")

    return generated_files, consolidated_file


if __name__ == "__main__":
    main()
