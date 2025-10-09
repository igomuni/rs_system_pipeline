#!/usr/bin/env python3
"""
2014-2023年度データの横断分析

以下の分析を実行:
1. 予算で最も金額の大きい事業
2. すべての年度に存在する事業名
3. コロナ前後で影響を受けている事業
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List
import sys

project_root = Path(__file__).parent.parent
output_dir = project_root / "output" / "processed"


def load_all_overview_data() -> pd.DataFrame:
    """全年度の基本情報データを読み込み"""
    dfs = []
    for year in range(2014, 2024):
        file_path = output_dir / f"year_{year}" / f"1-2_{year}_基本情報_事業概要.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['年度'] = year
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("基本情報データが見つかりません")

    return pd.concat(dfs, ignore_index=True)


def load_all_budget_data() -> pd.DataFrame:
    """全年度の予算・執行データを読み込み"""
    dfs = []
    for year in range(2014, 2024):
        file_path = output_dir / f"year_{year}" / f"2-1_{year}_予算・執行_サマリ.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['データ年度'] = year  # レビューシートの年度
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("予算・執行データが見つかりません")

    return pd.concat(dfs, ignore_index=True)


def analyze_largest_budget_projects():
    """予算で最も金額の大きい事業を分析"""
    print("# 1. 予算で最も金額の大きい事業\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # 事業ごとの予算総額を計算（全年度合計）
    budget_by_project = budget.groupby(['データ年度', '予算事業ID']).agg({
        '当初予算(合計)': 'sum'
    }).reset_index()

    # 基本情報と結合
    merged = budget_by_project.merge(
        overview[['年度', '予算事業ID', '事業名', '府省庁']],
        left_on=['データ年度', '予算事業ID'],
        right_on=['年度', '予算事業ID'],
        how='left'
    )

    # TOP 20を抽出
    top20 = merged.nlargest(20, '当初予算(合計)')

    print("## 全年度における予算額TOP 20\n")
    print("| 順位 | 年度 | 予算額（百万円） | 事業名 | 府省庁 |")
    print("|------|------|------------------|--------|--------|")
    for rank, (idx, row) in enumerate(top20.iterrows(), 1):
        budget_str = f"{row['当初予算(合計)']:,.1f}"
        project_name = row['事業名'][:50] + ('...' if len(row['事業名']) > 50 else '')
        print(f"| {rank} | {row['データ年度']} | {budget_str} | {project_name} | {row['府省庁']} |")

    # 年度別TOP 3
    print("\n## 年度別TOP 3\n")
    for year in range(2014, 2024):
        year_data = merged[merged['データ年度'] == year].nlargest(3, '当初予算(合計)')
        if len(year_data) > 0:
            print(f"### {year}年度\n")
            print("| 順位 | 予算額（百万円） | 事業名 | 府省庁 |")
            print("|------|------------------|--------|--------|")
            for rank, (idx, row) in enumerate(year_data.iterrows(), 1):
                budget_str = f"{row['当初予算(合計)']:,.1f}"
                project_name = row['事業名'][:60] + ('...' if len(row['事業名']) > 60 else '')
                print(f"| {rank} | {budget_str} | {project_name} | {row['府省庁']} |")
            print()


def analyze_continuous_projects():
    """すべての年度に存在する事業名を分析"""
    print("\n---\n")
    print("# 2. すべての年度に存在する事業名\n")

    overview = load_all_overview_data()

    # 事業名ごとに存在する年度数をカウント
    project_years = overview.groupby('事業名')['年度'].agg(['count', 'min', 'max', list]).reset_index()
    project_years.columns = ['事業名', '年度数', '開始年度', '終了年度', '年度リスト']

    # 全10年度（2014-2023）に存在する事業
    continuous_projects = project_years[project_years['年度数'] == 10].sort_values('事業名')

    print(f"## サマリー\n")
    print(f"- **全10年度（2014-2023）に継続する事業数**: {len(continuous_projects)}件")

    # 9年度に存在する事業（1年欠けている）
    near_continuous = project_years[project_years['年度数'] == 9].sort_values('事業名')
    print(f"- **9年度に存在する事業数**: {len(near_continuous)}件（参考）\n")

    # 府省庁別の継続事業数
    if len(continuous_projects) > 0:
        continuous_with_ministry = overview[
            overview['事業名'].isin(continuous_projects['事業名'])
        ].groupby(['府省庁', '事業名'])['年度'].count().reset_index()

        ministry_stats = continuous_with_ministry.groupby('府省庁').size().sort_values(ascending=False)

        print("## 府省庁別の全年度継続事業数\n")
        print("| 順位 | 府省庁 | 継続事業数 |")
        print("|------|--------|-----------|")
        for rank, (ministry, count) in enumerate(ministry_stats.items(), 1):
            print(f"| {rank} | {ministry} | {count}件 |")

    if len(continuous_projects) > 0:
        print("\n## 継続事業リスト（先頭50件）\n")
        print("| No. | 事業名 |")
        print("|-----|--------|")
        for idx, (_, row) in enumerate(continuous_projects.head(50).iterrows(), 1):
            print(f"| {idx} | {row['事業名']} |")

        if len(continuous_projects) > 50:
            print(f"\n*... 他 {len(continuous_projects) - 50}件*")


def analyze_covid_impact():
    """コロナ前後で影響を受けている事業を分析"""
    print("\n---\n")
    print("# 3. コロナ前後で影響を受けている事業\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # 予算年度でフィルタ（2019年度 vs 2020年度の予算額を比較）
    # データ年度2020のシートに含まれる2019年度予算 vs 2020年度予算

    # 各事業の年度別予算額を集計
    budget_pivot = budget.groupby(['データ年度', '予算事業ID', '予算年度']).agg({
        '当初予算(合計)': 'sum'
    }).reset_index()

    # 2019-2020年の予算を比較（データ年度2020のシートを使用）
    budget_2020_sheet = budget_pivot[budget_pivot['データ年度'] == 2020]

    budget_2019 = budget_2020_sheet[budget_2020_sheet['予算年度'] == 2019][
        ['予算事業ID', '当初予算(合計)']
    ].rename(columns={'当初予算(合計)': '予算2019'})

    budget_2020 = budget_2020_sheet[budget_2020_sheet['予算年度'] == 2020][
        ['予算事業ID', '当初予算(合計)']
    ].rename(columns={'当初予算(合計)': '予算2020'})

    # 両年度のデータがある事業のみ比較
    comparison = budget_2019.merge(budget_2020, on='予算事業ID', how='inner')
    comparison['増減額'] = comparison['予算2020'] - comparison['予算2019']
    comparison['増減率'] = (comparison['増減額'] / comparison['予算2019'] * 100).round(1)

    # 事業名を追加
    overview_2020 = overview[overview['年度'] == 2020][['予算事業ID', '事業名', '府省庁']]
    comparison = comparison.merge(overview_2020, on='予算事業ID', how='left')

    # 統計サマリ
    print("## 全体統計\n")
    print("| 項目 | 件数・値 |")
    print("|------|---------|")
    print(f"| 比較対象事業数 | {len(comparison)}件 |")
    print(f"| 増加事業数 | {len(comparison[comparison['増減額'] > 0])}件 |")
    print(f"| 減少事業数 | {len(comparison[comparison['増減額'] < 0])}件 |")
    print(f"| 変化なし | {len(comparison[comparison['増減額'] == 0])}件 |")
    print(f"| 平均増減率 | {comparison['増減率'].replace([float('inf'), float('-inf')], 0).mean():.1f}% |")
    print(f"| 中央値増減率 | {comparison['増減率'].median():.1f}% |")

    # 大幅増加した事業（増加率 > 50% かつ 増加額 > 100百万円）
    print("\n## 大幅増加した事業（増加率50%以上 & 増加額100百万円以上）\n")
    increased = comparison[
        (comparison['増減率'] > 50) & (comparison['増減額'] > 100)
    ].sort_values('増減額', ascending=False).head(20)

    print("| 順位 | 事業名 | 府省庁 | 2019年度 | 2020年度 | 増減額 | 増減率 |")
    print("|------|--------|--------|----------|----------|--------|--------|")
    for rank, (idx, row) in enumerate(increased.iterrows(), 1):
        project_name = row['事業名'][:40] + ('...' if len(row['事業名']) > 40 else '')
        print(f"| {rank} | {project_name} | {row['府省庁']} | {row['予算2019']:,.0f} | {row['予算2020']:,.0f} | +{row['増減額']:,.0f} | +{row['増減率']:.1f}% |")

    # 大幅減少した事業（減少率 > 50% かつ 減少額 > 100百万円）
    print("\n## 大幅減少した事業（減少率50%以上 & 減少額100百万円以上）\n")
    decreased = comparison[
        (comparison['増減率'] < -50) & (comparison['増減額'] < -100)
    ].sort_values('増減額').head(20)

    print("| 順位 | 事業名 | 府省庁 | 2019年度 | 2020年度 | 増減額 | 増減率 |")
    print("|------|--------|--------|----------|----------|--------|--------|")
    for rank, (idx, row) in enumerate(decreased.iterrows(), 1):
        project_name = row['事業名'][:40] + ('...' if len(row['事業名']) > 40 else '')
        print(f"| {rank} | {project_name} | {row['府省庁']} | {row['予算2019']:,.0f} | {row['予算2020']:,.0f} | {row['増減額']:,.0f} | {row['増減率']:.1f}% |")


def main():
    """メイン処理"""
    try:
        # ヘッダー
        print("# 2014-2023年度 行政事業レビューデータ 横断分析レポート\n")
        print(f"**生成日時**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print("---\n")

        # 1. 最大予算事業
        analyze_largest_budget_projects()

        # 2. 継続事業
        analyze_continuous_projects()

        # 3. コロナ影響
        analyze_covid_impact()

        print("\n---\n")
        print("## 分析完了\n")
        print("このレポートは `data_quality/analyze_historical_data.py` により自動生成されました。")

    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
