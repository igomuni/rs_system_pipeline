#!/usr/bin/env python3
"""
オリンピック・パラリンピック関連事業の分析

2014-2024年度のオリンピック関連事業を抽出し、以下を分析:
1. 事業リストと予算推移
2. 年度別予算総額
3. 東京2020前後の予算変動
"""

import pandas as pd
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
output_dir = project_root / "output" / "processed"
rs_data_dir = project_root / "data" / "unzipped"


def load_all_overview_data() -> pd.DataFrame:
    """全年度の基本情報データを読み込み（2014-2024）"""
    dfs = []

    # 2014-2023年度（過去データ）
    for year in range(2014, 2024):
        file_path = output_dir / f"year_{year}" / f"1-2_{year}_基本情報_事業概要.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['年度'] = year
            df['データソース'] = '過去データ'
            df = df.drop_duplicates(subset=['予算事業ID'], keep='first')
            dfs.append(df)

    # 2024年度（RSシステムデータ）
    rs_file = rs_data_dir / "1-2_RS_2024_基本情報_事業概要等.csv"
    if rs_file.exists():
        df = pd.read_csv(rs_file, encoding='utf-8-sig')
        if '事業年度' in df.columns:
            df['年度'] = df['事業年度']
        else:
            df['年度'] = 2024
        df['データソース'] = 'RSシステム'
        df = df.drop_duplicates(subset=['予算事業ID'], keep='first')
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError("基本情報データが見つかりません")

    return pd.concat(dfs, ignore_index=True)


def load_all_budget_data() -> pd.DataFrame:
    """全年度の予算・執行データを読み込み（2014-2024）"""
    dfs = []

    # 2014-2023年度（過去データ）
    for year in range(2014, 2024):
        file_path = output_dir / f"year_{year}" / f"2-1_{year}_予算・執行_サマリ.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['データ年度'] = year
            df['データソース'] = '過去データ'
            dfs.append(df)

    # 2024年度（RSシステムデータ）
    rs_file = rs_data_dir / "2-1_RS_2024_予算・執行_サマリ.csv"
    if rs_file.exists():
        df = pd.read_csv(rs_file, encoding='utf-8-sig')
        if '事業年度' in df.columns:
            df['データ年度'] = df['事業年度']
        else:
            df['データ年度'] = 2024
        df['データソース'] = 'RSシステム'
        if '当初予算（合計）' in df.columns:
            df['当初予算(合計)'] = df['当初予算（合計）']
        # 単位変換: 円 → 百万円
        if df['当初予算(合計)'].max() > 1000000:
            df['当初予算(合計)'] = df['当初予算(合計)'] / 1000000
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError("予算・執行データが見つかりません")

    return pd.concat(dfs, ignore_index=True)


def find_olympic_projects():
    """オリンピック関連事業を抽出"""
    print("# オリンピック・パラリンピック関連事業 分析レポート\n")
    print(f"**生成日時**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    print(f"**対象年度**: 2014-2024年度\n")
    print("---\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # オリンピック関連のキーワード
    keywords = [
        'オリンピック', 'olympic', 'Olympic', 'OLYMPIC',
        'パラリンピック', 'paralympic', 'Paralympic', 'PARALYMPIC',
        '東京2020', '東京五輪', 'Tokyo2020',
        'スポーツ国際', '国際スポーツ', '国際競技',
        'ハイパフォーマンス', 'ナショナルトレーニングセンター',
        '競技力向上', 'メダル獲得'
    ]

    # 事業名でキーワード検索
    mask = overview['事業名'].str.contains('|'.join(keywords), case=False, na=False)
    olympic_projects = overview[mask].copy()

    # 事業の目的や概要でも検索（RS 2024データのみ）
    if '事業の目的' in overview.columns:
        mask_purpose = overview['事業の目的'].str.contains('|'.join(keywords), case=False, na=False)
        olympic_projects_purpose = overview[mask_purpose]
        olympic_projects = pd.concat([olympic_projects, olympic_projects_purpose]).drop_duplicates(subset=['年度', '予算事業ID'])

    print(f"## サマリー\n")
    print(f"- **検出された関連事業数（延べ）**: {len(olympic_projects)}件")
    print(f"- **ユニーク事業名数**: {olympic_projects['事業名'].nunique()}件")
    print(f"- **対象年度**: {olympic_projects['年度'].min():.0f}-{olympic_projects['年度'].max():.0f}年度\n")

    return olympic_projects, budget


def analyze_project_list(olympic_projects, budget):
    """事業リストと予算推移を分析"""
    print("## 関連事業リスト\n")

    # ユニーク事業名ごとに集計
    unique_projects = olympic_projects.groupby('事業名').agg({
        '年度': ['min', 'max', 'count'],
        '府省庁': 'first',
        '予算事業ID': 'first'
    }).reset_index()

    unique_projects.columns = ['事業名', '開始年度', '終了年度', '年度数', '府省庁', '予算事業ID_sample']

    # 各事業の2024年度予算を取得
    budget_2024 = budget[budget['データ年度'] == 2024]
    budget_2024 = budget_2024[budget_2024['予算年度'] == 2024]
    budget_2024_grouped = budget_2024.groupby('予算事業ID').agg({
        '当初予算(合計)': 'sum'
    }).reset_index()

    # マージ
    unique_projects = unique_projects.merge(
        budget_2024_grouped,
        left_on='予算事業ID_sample',
        right_on='予算事業ID',
        how='left'
    )

    # ソート
    unique_projects = unique_projects.sort_values('当初予算(合計)', ascending=False)

    print("| 順位 | 事業名 | 府省庁 | 実施期間 | 2024年度予算（百万円） |")
    print("|------|--------|--------|----------|------------------------|")
    for rank, (_, row) in enumerate(unique_projects.iterrows(), 1):
        project_name = row['事業名'][:50] + ('...' if len(row['事業名']) > 50 else '')
        period = f"{int(row['開始年度'])}-{int(row['終了年度'])}"
        budget_str = f"{row['当初予算(合計)']:,.1f}" if pd.notna(row['当初予算(合計)']) else 'N/A'
        print(f"| {rank} | {project_name} | {row['府省庁']} | {period} | {budget_str} |")

    print()
    return unique_projects


def analyze_yearly_budget(olympic_projects, budget):
    """年度別予算総額を分析"""
    print("\n## 年度別予算総額推移\n")

    # 予算事業IDリスト
    project_ids = olympic_projects[['年度', '予算事業ID']].drop_duplicates()

    # 各年度の当該年度予算を集計
    yearly_budgets = []
    for year in range(2014, 2025):
        year_budget = budget[budget['データ年度'] == year]
        year_budget = year_budget[year_budget['予算年度'] == year]

        # オリンピック関連事業のみ
        year_project_ids = project_ids[project_ids['年度'] == year]['予算事業ID'].unique()
        olympic_budget = year_budget[year_budget['予算事業ID'].isin(year_project_ids)]

        total = olympic_budget['当初予算(合計)'].sum()
        yearly_budgets.append({
            '年度': year,
            '総予算': total,
            '事業数': len(year_project_ids)
        })

    yearly_df = pd.DataFrame(yearly_budgets)

    print("| 年度 | 関連事業数 | 総予算（百万円） | 前年度比 |")
    print("|------|-----------|------------------|----------|")
    prev_budget = None
    for _, row in yearly_df.iterrows():
        year = int(row['年度'])
        count = int(row['事業数'])
        total = row['総予算']

        if prev_budget is not None and prev_budget > 0:
            change = ((total - prev_budget) / prev_budget * 100)
            change_str = f"{change:+.1f}%"
        else:
            change_str = "-"

        print(f"| {year} | {count} | {total:,.1f} | {change_str} |")
        prev_budget = total

    print()

    return yearly_df


def analyze_tokyo2020_impact(yearly_df):
    """東京2020前後の予算変動を分析"""
    print("\n## 東京2020オリンピック前後の変動\n")

    # 2019年（開催前年）、2020年（延期決定年）、2021年（開催年）、2022年（開催後）
    key_years = yearly_df[yearly_df['年度'].isin([2019, 2020, 2021, 2022])]

    print("| 年度 | 総予算（百万円） | イベント |")
    print("|------|------------------|----------|")
    for _, row in key_years.iterrows():
        year = int(row['年度'])
        total = row['総予算']

        if year == 2019:
            event = "東京2020開催予定の前年"
        elif year == 2020:
            event = "東京2020延期決定（COVID-19）"
        elif year == 2021:
            event = "東京2020開催（無観客）"
        elif year == 2022:
            event = "東京2020開催後"
        else:
            event = ""

        print(f"| {year} | {total:,.1f} | {event} |")

    print()


def analyze_major_projects(olympic_projects, budget):
    """主要事業の詳細分析"""
    print("\n## 主要事業の予算推移\n")

    # 2024年度で予算の大きい上位5事業
    budget_2024 = budget[(budget['データ年度'] == 2024) & (budget['予算年度'] == 2024)]
    project_ids_2024 = olympic_projects[olympic_projects['年度'] == 2024]['予算事業ID'].unique()
    olympic_budget_2024 = budget_2024[budget_2024['予算事業ID'].isin(project_ids_2024)]

    top5_2024 = olympic_budget_2024.groupby('予算事業ID').agg({
        '事業名': 'first',
        '当初予算(合計)': 'sum'
    }).nlargest(5, '当初予算(合計)')

    for project_id, row in top5_2024.iterrows():
        project_name = row['事業名']
        print(f"### {project_name}\n")

        # この事業の全年度予算推移
        project_budget = budget[budget['予算事業ID'] == project_id]
        project_budget = project_budget[project_budget['予算年度'] == project_budget['データ年度']]

        if len(project_budget) > 0:
            print("| 年度 | 予算額（百万円） |")
            print("|------|------------------|")
            for _, b_row in project_budget.sort_values('データ年度').iterrows():
                print(f"| {int(b_row['データ年度'])} | {b_row['当初予算(合計)']:,.1f} |")
            print()


def main():
    """メイン処理"""
    try:
        # 1. オリンピック関連事業を抽出
        olympic_projects, budget = find_olympic_projects()

        # 2. 事業リスト
        unique_projects = analyze_project_list(olympic_projects, budget)

        # 3. 年度別予算総額
        yearly_df = analyze_yearly_budget(olympic_projects, budget)

        # 4. 東京2020前後の影響
        analyze_tokyo2020_impact(yearly_df)

        # 5. 主要事業の詳細
        analyze_major_projects(olympic_projects, budget)

        print("\n---\n")
        print("## 分析完了\n")
        print("このレポートは `data_quality/analyze_olympic_projects.py` により自動生成されました。")

    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
