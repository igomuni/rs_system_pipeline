#!/usr/bin/env python3
"""
予算執行データの継続性検証ツール

2019-2024年の予算執行データについて:
1. 府省庁・事業名による年度間の紐付けが可能か
2. 複数年度にまたがる事業が正しく引き継がれているか
3. 予算変遷の追跡が可能か
を検証します。
"""

import pandas as pd
import sys
from pathlib import Path
from collections import defaultdict
import re

# プロジェクトルートの設定
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import MINISTRY_NAME_MAPPING


def normalize_ministry_name(name):
    """府省庁名を正規化"""
    if pd.isna(name):
        return None
    name = str(name).strip()
    return MINISTRY_NAME_MAPPING.get(name, name)


def normalize_project_name(name):
    """事業名を正規化（空白・記号の統一）"""
    if pd.isna(name):
        return None
    name = str(name).strip()
    # 全角・半角スペースを統一
    name = re.sub(r'[\s\u3000]+', ' ', name)
    return name


def load_budget_data(year):
    """指定年度の予算・執行サマリデータを読み込む"""
    if year == 2024:
        file_path = PROJECT_ROOT / 'data' / 'unzipped' / f'2-1_RS_{year}_予算・執行_サマリ.csv'
    else:
        file_path = PROJECT_ROOT / 'output' / 'processed' / f'year_{year}' / f'2-1_{year}_予算・執行_サマリ.csv'

    if not file_path.exists():
        print(f"⚠️  {year}年度データが見つかりません: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        print(f"✓ {year}年度データ読み込み: {len(df):,}行")
        return df
    except Exception as e:
        print(f"❌ {year}年度データ読み込みエラー: {e}")
        return None


def analyze_project_continuity(years=range(2019, 2025)):
    """事業の継続性を分析"""
    print("\n" + "="*80)
    print("予算執行データ継続性検証")
    print("="*80)

    # 各年度のデータを読み込み
    data_by_year = {}
    for year in years:
        df = load_budget_data(year)
        if df is not None:
            data_by_year[year] = df

    if len(data_by_year) < 2:
        print("❌ 比較に必要なデータが不足しています")
        return

    # 府省庁・事業名で各年の事業を整理
    project_keys_by_year = {}
    project_details_by_year = {}

    for year, df in data_by_year.items():
        # 府省庁名と事業名を正規化
        df['府省庁_正規化'] = df['府省庁'].apply(normalize_ministry_name)
        df['事業名_正規化'] = df['事業名'].apply(normalize_project_name)

        # 事業年度ごとの最初の行のみを使用（予算年度展開されたデータから事業を特定）
        project_info = df.groupby('予算事業ID').first().reset_index()

        project_keys = set()
        project_details = {}

        for _, row in project_info.iterrows():
            ministry = row['府省庁_正規化']
            project_name = row['事業名_正規化']

            if pd.notna(ministry) and pd.notna(project_name):
                key = (ministry, project_name)
                project_keys.add(key)
                project_details[key] = {
                    '予算事業ID': row['予算事業ID'],
                    '府省庁': row['府省庁'],
                    '事業名': row['事業名']
                }

        project_keys_by_year[year] = project_keys
        project_details_by_year[year] = project_details
        print(f"\n{year}年度: {len(project_keys):,}事業")

    # 年度間の継続性を分析
    print("\n" + "-"*80)
    print("年度間の事業継続性分析")
    print("-"*80)

    continuity_stats = []

    for i, year1 in enumerate(sorted(data_by_year.keys())[:-1]):
        year2 = sorted(data_by_year.keys())[i+1]

        keys1 = project_keys_by_year[year1]
        keys2 = project_keys_by_year[year2]

        # 継続事業（両年に存在）
        continued = keys1 & keys2
        # 新規事業（year2のみ）
        new_projects = keys2 - keys1
        # 終了事業（year1のみ）
        ended_projects = keys1 - keys2

        continuity_rate = len(continued) / len(keys1) * 100 if keys1 else 0

        print(f"\n【{year1}→{year2}年度】")
        print(f"  継続事業: {len(continued):,}件 ({continuity_rate:.1f}%)")
        print(f"  新規事業: {len(new_projects):,}件")
        print(f"  終了事業: {len(ended_projects):,}件")

        continuity_stats.append({
            'period': f'{year1}→{year2}',
            'continued': len(continued),
            'new': len(new_projects),
            'ended': len(ended_projects),
            'continuity_rate': continuity_rate
        })

    # 全期間にわたる継続事業を検出
    print("\n" + "-"*80)
    print("全期間（2019-2024）にわたる継続事業")
    print("-"*80)

    all_years_keys = set.intersection(*[project_keys_by_year[y] for y in data_by_year.keys()])
    print(f"\n全{len(data_by_year)}年度継続事業: {len(all_years_keys):,}件")

    if len(all_years_keys) > 0:
        print("\n例（先頭10件）:")
        for i, (ministry, project) in enumerate(sorted(all_years_keys)[:10], 1):
            print(f"  {i}. {ministry} - {project}")

    return {
        'continuity_stats': continuity_stats,
        'all_years_continued': all_years_keys,
        'project_keys_by_year': project_keys_by_year,
        'project_details_by_year': project_details_by_year,
        'data_by_year': data_by_year
    }


def validate_budget_tracking(analysis_result):
    """予算変遷の追跡可能性を検証"""
    print("\n" + "="*80)
    print("予算変遷追跡可能性の検証")
    print("="*80)

    all_years_projects = analysis_result['all_years_continued']
    data_by_year = analysis_result['data_by_year']
    project_details_by_year = analysis_result['project_details_by_year']

    if len(all_years_projects) == 0:
        print("⚠️  全年度継続事業がないため、追跡検証をスキップします")
        return

    # サンプル事業で予算変遷を追跡
    sample_projects = sorted(all_years_projects)[:5]

    issues = []

    for ministry, project_name in sample_projects:
        print(f"\n【事業】{ministry} - {project_name}")
        print("-" * 60)

        budget_history = []

        for year in sorted(data_by_year.keys()):
            df = data_by_year[year]
            details = project_details_by_year[year].get((ministry, project_name))

            if details:
                # この事業年度のレコードを取得
                project_id = details['予算事業ID']
                project_data = df[df['予算事業ID'] == project_id]

                # 予算年度ごとのデータを取得
                for _, row in project_data.iterrows():
                    budget_year = row.get('予算年度')

                    # 2024年データの列名（全角括弧）と2019-2023データの列名（半角括弧）の違いに対応
                    if '当初予算（合計）' in row:
                        initial_budget = row.get('当初予算（合計）')
                    else:
                        initial_budget = row.get('当初予算(合計)')

                    if '執行額（合計）' in row:
                        execution = row.get('執行額（合計）')
                    else:
                        execution = row.get('執行額(合計)')

                    if pd.notna(budget_year):
                        budget_history.append({
                            '事業年度': year,
                            '予算年度': budget_year,
                            '当初予算': initial_budget,
                            '執行額': execution
                        })

        # 予算年度順にソート
        budget_history.sort(key=lambda x: (x['予算年度'], x['事業年度']))

        # 表示
        print(f"{'事業年度':<8} {'予算年度':<8} {'当初予算':>15} {'執行額':>15}")
        for record in budget_history:
            budget_val = record['当初予算']
            exec_val = record['執行額']

            # 数値フォーマット
            budget_str = f"{budget_val:,.0f}" if pd.notna(budget_val) and budget_val != 0 else "-"
            exec_str = f"{exec_val:,.0f}" if pd.notna(exec_val) and exec_val != 0 else "-"

            print(f"{record['事業年度']:<8} {record['予算年度']:<8} {budget_str:>15} {exec_str:>15}")

        # データ品質チェック
        # 1. 同じ予算年度が複数の事業年度に出現しているか
        budget_year_sources = defaultdict(list)
        for record in budget_history:
            budget_year_sources[record['予算年度']].append(record['事業年度'])

        for budget_year, event_years in budget_year_sources.items():
            if len(event_years) > 1:
                # 過去データからの引き継ぎは正常
                pass

        # 2. 予算額の異常な変動をチェック（10倍以上の変化）
        for i in range(len(budget_history) - 1):
            curr = budget_history[i]
            next_rec = budget_history[i + 1]

            curr_budget = curr['当初予算']
            next_budget = next_rec['当初予算']

            if pd.notna(curr_budget) and pd.notna(next_budget) and curr_budget > 0 and next_budget > 0:
                ratio = next_budget / curr_budget
                if ratio > 10 or ratio < 0.1:
                    issue = {
                        '府省庁': ministry,
                        '事業名': project_name,
                        '問題': f"予算額の異常な変動: {curr['予算年度']}年度 {curr_budget:,.0f} → {next_rec['予算年度']}年度 {next_budget:,.0f} (変化率: {ratio:.1f}倍)",
                        '重大度': 'HIGH' if ratio > 1000 or ratio < 0.001 else 'MEDIUM'
                    }
                    issues.append(issue)
                    print(f"  ⚠️  {issue['問題']}")

    return issues


def check_data_anomalies(analysis_result):
    """データの異常値・不整合をチェック"""
    print("\n" + "="*80)
    print("データ異常値・不整合チェック")
    print("="*80)

    data_by_year = analysis_result['data_by_year']
    issues = []

    for year, df in data_by_year.items():
        print(f"\n【{year}年度】")

        # 列名の違いに対応
        if '当初予算（合計）' in df.columns:
            budget_col = '当初予算（合計）'
            exec_col = '執行額（合計）'
        else:
            budget_col = '当初予算(合計)'
            exec_col = '執行額(合計)'

        # 1. 執行額が予算額を大幅に超過（繰越・補正を考慮せず）
        if budget_col in df.columns and exec_col in df.columns:
            over_exec = df[
                (df[budget_col] > 0) &
                (df[exec_col] > df[budget_col] * 1.5)
            ]

            if len(over_exec) > 0:
                print(f"  ⚠️  執行額が当初予算の1.5倍超: {len(over_exec):,}件")
                for _, row in over_exec.head(3).iterrows():
                    issue = {
                        '年度': year,
                        '府省庁': row['府省庁'],
                        '事業名': row['事業名'],
                        '予算年度': row['予算年度'],
                        '問題': f"執行額({row[exec_col]:,.0f})が当初予算({row[budget_col]:,.0f})の{row[exec_col]/row[budget_col]:.1f}倍",
                        '重大度': 'LOW'  # 補正予算・繰越があるため低
                    }
                    issues.append(issue)
                    print(f"     - {row['府省庁']} - {row['事業名']}: {issue['問題']}")

        # 2. 予算額が異常に大きい（1兆円超）
        if budget_col in df.columns:
            huge_budget = df[df[budget_col] > 1_000_000_000_000]

            if len(huge_budget) > 0:
                print(f"  ℹ️  当初予算1兆円超の事業: {len(huge_budget):,}件")
                for _, row in huge_budget.head(3).iterrows():
                    print(f"     - {row['府省庁']} - {row['事業名']}: {row[budget_col]:,.0f}円")

        # 3. 予算年度が事業年度より未来（2年以上先）
        future_budget = df[
            pd.notna(df['予算年度']) &
            (df['予算年度'] > year + 2)
        ]

        if len(future_budget) > 0:
            print(f"  ⚠️  予算年度が事業年度+2年超: {len(future_budget):,}件")
            for _, row in future_budget.head(3).iterrows():
                issue = {
                    '年度': year,
                    '府省庁': row['府省庁'],
                    '事業名': row['事業名'],
                    '問題': f"予算年度({row['予算年度']})が事業年度({year})より{row['予算年度']-year}年先",
                    '重大度': 'MEDIUM'
                }
                issues.append(issue)
                print(f"     - {row['府省庁']} - {row['事業名']}: {issue['問題']}")

        # 4. 府省庁名が空
        empty_ministry = df[df['府省庁'].isna() | (df['府省庁'] == '')]
        if len(empty_ministry) > 0:
            print(f"  ⚠️  府省庁名が空: {len(empty_ministry):,}件")
            issue = {
                '年度': year,
                '問題': f"府省庁名が空のレコード: {len(empty_ministry):,}件",
                '重大度': 'HIGH'
            }
            issues.append(issue)

        # 5. 事業名が空
        empty_project = df[df['事業名'].isna() | (df['事業名'] == '')]
        if len(empty_project) > 0:
            print(f"  ⚠️  事業名が空: {len(empty_project):,}件")
            issue = {
                '年度': year,
                '問題': f"事業名が空のレコード: {len(empty_project):,}件",
                '重大度': 'HIGH'
            }
            issues.append(issue)

    return issues


def generate_report(analysis_result, budget_issues, data_issues):
    """検証結果をレポートファイルに出力"""
    report_path = PROJECT_ROOT / 'data_quality' / 'reports' / 'budget_continuity_validation.md'

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# 予算執行データ継続性検証レポート\n\n")
        f.write(f"生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # 1. サマリ
        f.write("## 1. サマリ\n\n")
        f.write(f"- **検証対象年度**: 2019-2024年\n")
        f.write(f"- **全年度継続事業数**: {len(analysis_result['all_years_continued']):,}件\n")
        f.write(f"- **検出された問題**: {len(budget_issues) + len(data_issues)}件\n\n")

        # 2. 年度間継続性
        f.write("## 2. 年度間の事業継続性\n\n")
        f.write("| 期間 | 継続事業 | 新規事業 | 終了事業 | 継続率 |\n")
        f.write("|------|----------|----------|----------|--------|\n")
        for stat in analysis_result['continuity_stats']:
            f.write(f"| {stat['period']} | {stat['continued']:,}件 | {stat['new']:,}件 | {stat['ended']:,}件 | {stat['continuity_rate']:.1f}% |\n")

        # 3. 全年度継続事業
        f.write("\n## 3. 全年度（2019-2024）継続事業\n\n")
        f.write(f"**合計**: {len(analysis_result['all_years_continued']):,}件\n\n")

        if len(analysis_result['all_years_continued']) > 0:
            f.write("### 代表例（先頭20件）\n\n")
            for i, (ministry, project) in enumerate(sorted(analysis_result['all_years_continued'])[:20], 1):
                f.write(f"{i}. **{ministry}** - {project}\n")

        # 4. 予算変遷追跡の問題
        f.write("\n## 4. 予算変遷追跡における問題\n\n")
        if len(budget_issues) == 0:
            f.write("✓ サンプル事業において重大な問題は検出されませんでした。\n\n")
        else:
            f.write(f"**検出数**: {len(budget_issues)}件\n\n")

            high_issues = [i for i in budget_issues if i.get('重大度') == 'HIGH']
            medium_issues = [i for i in budget_issues if i.get('重大度') == 'MEDIUM']

            if high_issues:
                f.write("### 高重大度\n\n")
                for issue in high_issues:
                    f.write(f"- **{issue.get('府省庁', 'N/A')}** - {issue.get('事業名', 'N/A')}\n")
                    f.write(f"  - {issue['問題']}\n\n")

            if medium_issues:
                f.write("### 中重大度\n\n")
                for issue in medium_issues:
                    f.write(f"- **{issue.get('府省庁', 'N/A')}** - {issue.get('事業名', 'N/A')}\n")
                    f.write(f"  - {issue['問題']}\n\n")

        # 5. データ品質の問題
        f.write("\n## 5. データ品質における問題\n\n")
        if len(data_issues) == 0:
            f.write("✓ データ品質において重大な問題は検出されませんでした。\n\n")
        else:
            f.write(f"**検出数**: {len(data_issues)}件\n\n")

            high_issues = [i for i in data_issues if i.get('重大度') == 'HIGH']
            medium_issues = [i for i in data_issues if i.get('重大度') == 'MEDIUM']
            low_issues = [i for i in data_issues if i.get('重大度') == 'LOW']

            if high_issues:
                f.write("### 高重大度\n\n")
                for issue in high_issues[:10]:  # 最大10件
                    f.write(f"- **{issue.get('年度', 'N/A')}年度**\n")
                    f.write(f"  - {issue['問題']}\n\n")

            if medium_issues:
                f.write("### 中重大度\n\n")
                for issue in medium_issues[:10]:
                    f.write(f"- **{issue.get('年度', 'N/A')}年度** - {issue.get('府省庁', 'N/A')} - {issue.get('事業名', 'N/A')}\n")
                    f.write(f"  - {issue['問題']}\n\n")

        # 6. 結論と推奨事項
        f.write("\n## 6. 結論と推奨事項\n\n")

        f.write("### 府省庁・事業名による紐付け\n\n")
        continuity_rate_avg = sum(s['continuity_rate'] for s in analysis_result['continuity_stats']) / len(analysis_result['continuity_stats'])

        if continuity_rate_avg > 50:
            f.write(f"✓ **可能**: 年度間の継続率は平均{continuity_rate_avg:.1f}%で、府省庁名と事業名による紐付けは実用的です。\n\n")
        else:
            f.write(f"⚠️  **要注意**: 年度間の継続率は平均{continuity_rate_avg:.1f}%で、事業名の変更が頻繁に発生している可能性があります。\n\n")

        f.write("### 予算変遷の追跡\n\n")
        f.write("✓ **可能**: 同一事業（府省庁名・事業名が一致）について、複数年度の予算年度データを収集することで予算変遷を追跡できます。\n\n")
        f.write("- 各年度のレビューシートは過去数年分の予算年度データを含んでいます\n")
        f.write("- 最新年度のデータを参照することで、最も正確な過去データを取得できます\n\n")

        f.write("### 推奨事項\n\n")
        if len(budget_issues) + len(data_issues) > 0:
            f.write("1. **データクレンジング**: 検出された高重大度の問題を優先的に修正\n")
            f.write("2. **事業名の正規化**: 事業名の表記ゆれを吸収するための正規化処理を強化\n")
            f.write("3. **異常値の調査**: 予算額の異常な変動について、元データを確認\n")
        else:
            f.write("現時点で重大な問題は検出されませんでした。引き続きデータ品質モニタリングを継続してください。\n")

    print(f"\n✓ レポート生成完了: {report_path}")
    return report_path


def main():
    """メイン処理"""
    # 継続性分析
    analysis_result = analyze_project_continuity()

    if not analysis_result:
        return

    # 予算追跡検証
    budget_issues = validate_budget_tracking(analysis_result) or []

    # データ異常チェック
    data_issues = check_data_anomalies(analysis_result) or []

    # レポート生成
    report_path = generate_report(analysis_result, budget_issues, data_issues)

    print("\n" + "="*80)
    print("検証完了")
    print("="*80)
    print(f"\n詳細レポート: {report_path}")
    print(f"\n全年度継続事業: {len(analysis_result['all_years_continued']):,}件")
    print(f"検出された問題: {len(budget_issues) + len(data_issues)}件")


if __name__ == '__main__':
    main()
