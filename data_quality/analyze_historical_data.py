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
            # 予算事業IDで重複排除（最初の1件のみ）
            df = df.drop_duplicates(subset=['予算事業ID'], keep='first')
            dfs.append(df)

    # 2024年度（RSシステムデータ）
    rs_file = rs_data_dir / "1-2_RS_2024_基本情報_事業概要等.csv"
    if rs_file.exists():
        df = pd.read_csv(rs_file, encoding='utf-8-sig')
        # 列名を統一
        if '事業年度' in df.columns:
            df['年度'] = df['事業年度']
        else:
            df['年度'] = 2024
        df['データソース'] = 'RSシステム'
        # 予算事業IDで重複排除（最初の1件のみ）- 主要経費別に分かれているレコードを統合
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
            df['データ年度'] = year  # レビューシートの年度
            df['データソース'] = '過去データ'
            dfs.append(df)

    # 2024年度（RSシステムデータ）
    rs_file = rs_data_dir / "2-1_RS_2024_予算・執行_サマリ.csv"
    if rs_file.exists():
        df = pd.read_csv(rs_file, encoding='utf-8-sig')
        # データ年度を設定
        if '事業年度' in df.columns:
            df['データ年度'] = df['事業年度']
        else:
            df['データ年度'] = 2024
        df['データソース'] = 'RSシステム'
        # 列名を統一
        if '当初予算（合計）' in df.columns:
            df['当初予算(合計)'] = df['当初予算（合計）']
        # RSシステムは金額単位が「円」なので百万円に変換
        if df['当初予算(合計)'].max() > 1000000:  # 100万円以上の値があれば円単位
            df['当初予算(合計)'] = df['当初予算(合計)'] / 1000000
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError("予算・執行データが見つかりません")

    return pd.concat(dfs, ignore_index=True)


def analyze_largest_budget_projects():
    """予算で最も金額の大きい事業を分析"""
    print("# 1. 予算で最も金額の大きい事業\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # 各事業の当該年度の予算額のみを取得（予算年度==データ年度）
    budget_current_year = budget[budget['予算年度'] == budget['データ年度']].copy()

    # 基本情報と結合（suffixesを明示的に指定）
    merged = budget_current_year.merge(
        overview[['年度', '予算事業ID', '事業名', '府省庁', 'データソース']],
        left_on=['データ年度', '予算事業ID'],
        right_on=['年度', '予算事業ID'],
        how='left',
        suffixes=('_budget', '_overview')
    )

    # 列名を整理（overviewの列を優先）
    if '事業名_overview' in merged.columns:
        merged['事業名'] = merged['事業名_overview']
    if '府省庁_overview' in merged.columns:
        merged['府省庁'] = merged['府省庁_overview']
    if 'データソース_overview' in merged.columns:
        merged['データソース'] = merged['データソース_overview']
    elif 'データソース_budget' in merged.columns:
        merged['データソース'] = merged['データソース_budget']

    # 2014年度（単位エラーあり）のTOP 20
    merged_2014 = merged[merged['データ年度'] == 2014]
    top20_2014 = merged_2014.nlargest(20, '当初予算(合計)')

    print("## 2014年度 予算額TOP 20\n")
    print("**注**: 2014年度は単位エラーあり（百万円単位で約1000倍の値が含まれる）\n")
    print("| 順位 | 予算額（百万円） | 事業名 | 府省庁 |")
    print("|------|------------------|--------|--------|")
    for rank, (idx, row) in enumerate(top20_2014.iterrows(), 1):
        budget_str = f"{row['当初予算(合計)']:,.1f}"
        project_name = row['事業名'][:50] + ('...' if len(row['事業名']) > 50 else '')
        print(f"| {rank} | {budget_str} | {project_name} | {row['府省庁']} |")

    # 2015-2024年度のTOP 20
    merged_2015_2024 = merged[merged['データ年度'] >= 2015]
    top20_2015_2024 = merged_2015_2024.nlargest(20, '当初予算(合計)')

    print("\n## 2015-2024年度 予算額TOP 20\n")
    print("| 順位 | 年度 | 予算額（百万円） | 事業名 | 府省庁 | データソース |")
    print("|------|------|------------------|--------|--------|-------------|")
    for rank, (idx, row) in enumerate(top20_2015_2024.iterrows(), 1):
        budget_str = f"{row['当初予算(合計)']:,.1f}"
        project_name = row['事業名'][:45] + ('...' if len(row['事業名']) > 45 else '')
        print(f"| {rank} | {row['データ年度']} | {budget_str} | {project_name} | {row['府省庁']} | {row['データソース']} |")

    # 年度別TOP 10
    print("\n## 年度別TOP 10\n")
    for year in range(2014, 2025):
        year_data = merged[merged['データ年度'] == year].nlargest(10, '当初予算(合計)')
        if len(year_data) > 0:
            print(f"### {year}年度\n")
            print("| 順位 | 予算額（百万円） | 事業名 | 府省庁 |")
            print("|------|------------------|--------|--------|")
            for rank, (idx, row) in enumerate(year_data.iterrows(), 1):
                budget_str = f"{row['当初予算(合計)']:,.1f}"
                project_name = row['事業名'][:60] + ('...' if len(row['事業名']) > 60 else '')
                print(f"| {rank} | {budget_str} | {project_name} | {row['府省庁']} |")
            print()

    # 定番大型事業の分析を追加
    analyze_regular_large_projects(merged, budget)


def analyze_regular_large_projects(merged, budget):
    """定番大型事業の分析"""
    print("\n## 定番大型事業の分析\n")

    # 事業名の正規化関数
    def normalize_project_name(name):
        """事業名の表記ゆれを統一"""
        # 括弧の統一（全角→半白）
        name = name.replace('（', '(').replace('）', ')')

        # スラッシュの削除（保険給付に必要な経費／(年金特別会計→保険給付に必要な経費(年金特別会計）
        name = name.replace('／(', '(').replace('／（', '(')

        # 括弧前のスペースを削除（`保険給付に必要な経費 (年金...`→`保険給付に必要な経費(年金...`）
        name = name.replace(' (', '(')

        # 個別の表記ゆれを統一
        replacements = {
            '介護給付費金財政調整交付金': '介護給付費財政調整交付金',
            '介護給付費等負担金': '介護給付費負担金',
            '障害者自立支援給付費': '障害者自立支援給付',
            '失業等給付費等': '失業等給付費',
            '国立大学法人運営費交付金': '国立大学法人の運営'  # 「運営費交付金」→「運営」に統一
        }

        for old, new in replacements.items():
            if old in name:
                name = name.replace(old, new)

        return name

    # budgetデータの事業名を正規化
    budget = budget.copy()
    budget['事業名'] = budget['事業名'].apply(normalize_project_name)

    # mergedデータの事業名を正規化
    merged = merged.copy()
    merged['事業名'] = merged['事業名'].apply(normalize_project_name)

    # 定番事業のリスト（事業名の一部でマッチング）
    regular_projects_patterns = [
        '基礎年金給付',
        '保険給付に必要な経費',
        '医療保険給付費国庫負担金',
        '保険料等交付金',
        '保護費負担金',
        '失業等給付費',
        '介護給付費',
        '子どものための教育・保育給付',
        '義務教育費国庫負担金に必要な経費',  # 「及び標準法実施等」を除外するため具体的に指定
        '障害者自立支援給付',
        '児童手当等交付金',
        '国立大学法人の運営',  # 「国立大学法人の運営に必要な経費」「国立大学法人運営費交付金」両方をカバー
        '防災・安全交付金',
        '道路事業(直轄・改築等)',
        '社会資本整備総合交付金',
        '国民年金給付',
        '年金生活者支援給付金の支給に必要な経費'  # 事務費・準備費を除外するため具体的に指定
    ]

    # 定番事業にマッチする事業名を特定
    def is_regular_project(project_name):
        for pattern in regular_projects_patterns:
            if pattern in project_name:
                # 細分化事業を除外（カッコ内に詳細が含まれるもの）
                exclude_patterns = ['防災・安全交付金', '社会資本整備総合交付金']
                if pattern in exclude_patterns and '(' in project_name:
                    return False
                return True
        return False

    # 定番事業フラグを追加
    merged['定番事業'] = merged['事業名'].apply(is_regular_project)

    # 定番事業の一覧を取得
    regular_projects = merged[merged['定番事業']].groupby('事業名').size().reset_index(name='出現回数')
    regular_projects = regular_projects.sort_values('出現回数', ascending=False)

    print(f"**定番大型事業**: {len(regular_projects)}件\n")

    # 定番事業の年度別予算推移
    print("### 定番大型事業の予算推移（2014-2024年度）\n")

    # 各事業の年度別予算を取得
    budget_by_year = budget[budget['予算年度'] == budget['データ年度']].copy()
    budget_pivot = budget_by_year.groupby(['事業名', 'データ年度']).agg({
        '当初予算(合計)': 'sum'
    }).reset_index()

    # 定番事業のみフィルタ
    regular_budget = budget_pivot[budget_pivot['事業名'].apply(is_regular_project)]

    # ピボットテーブルに変換
    budget_wide = regular_budget.pivot(index='事業名', columns='データ年度', values='当初予算(合計)')

    # 2024年度予算で降順ソート
    if 2024 in budget_wide.columns:
        budget_wide = budget_wide.sort_values(2024, ascending=False)

    # テーブル出力
    print("| 事業名 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |")
    print("|--------|------|------|------|------|------|------|------|------|------|------|------|")

    for project_name, row in budget_wide.iterrows():
        name_short = project_name[:40] + ('...' if len(project_name) > 40 else '')
        line = f"| {name_short} |"
        for year in range(2014, 2025):
            if year in budget_wide.columns:
                val = row.get(year)
                if pd.notna(val):
                    # すべて百万円単位で統一表記
                    if val >= 1000000:  # 1兆円以上
                        line += f" {val:,.0f} |"
                    elif val >= 10:  # 10百万円以上
                        line += f" {val:,.0f} |"
                    else:  # 10百万円未満
                        line += f" {val:.1f} |"
                else:
                    line += " - |"
            else:
                line += " - |"
        print(line)

    # 定番事業を除いたTOP 20とTOP 10
    print("\n### 定番大型事業を除いた予算額TOP 20（2015-2024年度）\n")

    merged_non_regular = merged[~merged['定番事業']]
    merged_2015_2024_non_regular = merged_non_regular[merged_non_regular['データ年度'] >= 2015]
    top20_non_regular = merged_2015_2024_non_regular.nlargest(20, '当初予算(合計)')

    print("| 順位 | 年度 | 予算額（百万円） | 事業名 | 府省庁 |")
    print("|------|------|------------------|--------|--------|")
    for rank, (idx, row) in enumerate(top20_non_regular.iterrows(), 1):
        budget_str = f"{row['当初予算(合計)']:,.1f}"
        project_name = row['事業名'][:45] + ('...' if len(row['事業名']) > 45 else '')
        print(f"| {rank} | {row['データ年度']} | {budget_str} | {project_name} | {row['府省庁']} |")

    # 定番事業を除いた年度別TOP 10
    print("\n### 定番大型事業を除いた年度別TOP 10\n")
    for year in range(2014, 2025):
        year_data_non_regular = merged_non_regular[merged_non_regular['データ年度'] == year].nlargest(10, '当初予算(合計)')
        if len(year_data_non_regular) > 0:
            print(f"#### {year}年度\n")
            print("| 順位 | 予算額（百万円） | 事業名 | 府省庁 |")
            print("|------|------------------|--------|--------|")
            for rank, (idx, row) in enumerate(year_data_non_regular.iterrows(), 1):
                budget_str = f"{row['当初予算(合計)']:,.1f}"
                project_name = row['事業名'][:55] + ('...' if len(row['事業名']) > 55 else '')
                print(f"| {rank} | {budget_str} | {project_name} | {row['府省庁']} |")
            print()


def analyze_continuous_projects():
    """すべての年度に存在する事業名を分析"""
    print("\n---\n")
    print("# 2. すべての年度に存在する事業名\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # 事業名ごとに存在する年度数をカウント
    project_years = overview.groupby('事業名')['年度'].agg(['count', 'min', 'max', list]).reset_index()
    project_years.columns = ['事業名', '年度数', 'データ開始年度', 'データ終了年度', '年度リスト']

    # 全11年度（2014-2024）に存在する事業
    continuous_all = project_years[project_years['年度数'] == 11].sort_values('事業名')

    # 全10年度（2014-2023）に存在する事業
    continuous_10 = project_years[project_years['年度数'] == 10].sort_values('事業名')

    print(f"## サマリー\n")
    print(f"- **全11年度（2014-2024）に継続する事業数**: {len(continuous_all)}件")
    print(f"- **全10年度（2014-2023）に継続する事業数**: {len(continuous_10)}件")

    # 9年度に存在する事業（1年欠けている）
    near_continuous = project_years[project_years['年度数'] == 9].sort_values('事業名')
    print(f"- **9年度に存在する事業数**: {len(near_continuous)}件（参考）\n")

    # 分析対象を11年度継続事業に変更
    continuous_projects = continuous_all if len(continuous_all) > 0 else continuous_10

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
        # 実際の「事業開始年度」列を取得（最新のデータを使用）
        # 事業名ごとに最新年度の事業開始年度を取得
        project_start_years = overview[overview['事業名'].isin(continuous_projects['事業名'])].copy()
        # 事業開始年度を数値に変換（NaNや空文字を処理）
        project_start_years['事業開始年度'] = pd.to_numeric(project_start_years['事業開始年度'], errors='coerce')
        # 各事業名の最新年度のレコードを取得
        latest_records = project_start_years.sort_values('年度').groupby('事業名').last().reset_index()
        # continuous_projectsにマージ
        continuous_with_start = continuous_projects.merge(
            latest_records[['事業名', '事業開始年度', '府省庁']],
            on='事業名',
            how='left'
        )

        # 開始年度を10年代に分類
        continuous_with_start_valid = continuous_with_start.dropna(subset=['事業開始年度']).copy()
        continuous_with_start_valid['年代'] = (continuous_with_start_valid['事業開始年度'] // 10 * 10).astype(int)

        # 年代ごとのTOP 10を表示（1920年代から）
        decades = sorted(continuous_with_start_valid['年代'].unique())

        print("\n## 開始年度を10年代ごとに分類（TOP 10と予算推移）\n")

        # 全年度の予算データを取得
        # 各事業の年度別予算を取得（予算年度 == データ年度）
        budget_by_year = budget[budget['予算年度'] == budget['データ年度']].copy()

        # 事業名と年度で予算を集計（予算事業IDは年度によって変わるため）
        budget_pivot = budget_by_year.groupby(['事業名', 'データ年度']).agg({
            '当初予算(合計)': 'sum'
        }).reset_index()

        # ピボットテーブルに変換（事業名 × 年度）
        budget_wide = budget_pivot.pivot(index='事業名', columns='データ年度', values='当初予算(合計)')

        for decade in decades:
            if decade < 1920:  # 1920年代より前は表示しない
                continue

            decade_projects = continuous_with_start_valid[
                continuous_with_start_valid['年代'] == decade
            ].nsmallest(10, '事業開始年度')

            if len(decade_projects) > 0:
                decade_label = f"{decade}年代"

                # 予算データをマージ
                decade_projects_with_budget = decade_projects.merge(
                    budget_wide,
                    left_on='事業名',
                    right_index=True,
                    how='left'
                )

                print(f"### {decade_label}\n")

                # ヘッダー行を動的に生成（事業名、開始年度、府省庁 + 2014-2024年度）
                header = "| 順位 | 事業名 | 開始年度 | 府省庁 |"
                separator = "|------|--------|----------|--------|"
                for year in range(2014, 2025):
                    header += f" {year} |"
                    separator += "------|"

                print(header)
                print(separator)

                for rank, (_, row) in enumerate(decade_projects_with_budget.iterrows(), 1):
                    project_name = row['事業名'][:30] + ('...' if len(row['事業名']) > 30 else '')
                    start_year = int(row['事業開始年度'])
                    line = f"| {rank} | {project_name} | {start_year} | {row['府省庁']} |"

                    # 各年度の予算を追加
                    for year in range(2014, 2025):
                        if year in budget_wide.columns:
                            budget_val = row.get(year)
                            if pd.notna(budget_val):
                                # 1000以上は千円単位、1000未満は小数点1桁
                                if budget_val >= 1000:
                                    budget_str = f"{budget_val:,.0f}"
                                else:
                                    budget_str = f"{budget_val:.1f}"
                            else:
                                budget_str = "-"
                        else:
                            budget_str = "-"
                        line += f" {budget_str} |"

                    print(line)

                # この年代の予算統計を追加（2024年度基準）
                decade_budget_2024 = decade_projects_with_budget[2024].dropna()
                if len(decade_budget_2024) > 0:
                    stats = {
                        '事業数': len(decade_budget_2024),
                        '平均予算': decade_budget_2024.mean(),
                        '中央値': decade_budget_2024.median(),
                        '標準偏差': decade_budget_2024.std(),
                        '最小予算': decade_budget_2024.min(),
                        '最大予算': decade_budget_2024.max()
                    }

                    print(f"\n**{decade_label}全体の統計（{stats['事業数']}件、2024年度予算・百万円）**:")
                    print(f"- 平均: {stats['平均予算']:,.1f}、中央値: {stats['中央値']:,.1f}、標準偏差: {stats['標準偏差']:,.1f}")
                    print(f"- 最小: {stats['最小予算']:,.1f}、最大: {stats['最大予算']:,.1f}")

                print()

        # 年代ごとの予算統計を追加
        analyze_decade_budget_statistics(continuous_with_start_valid, budget)


def analyze_decade_budget_statistics(continuous_with_start_valid, budget):
    """年代ごとの予算統計を分析"""
    print("\n## 開始年度別・年代ごとの予算統計（2024年度）\n")

    # 2024年度の予算データを取得
    budget_2024 = budget[(budget['データ年度'] == 2024) & (budget['予算年度'] == 2024)]

    # 事業名をキーにしてマージ（予算事業IDは年度によって変わるため）
    # まず、continuous_with_start_validから事業名と年代を取得
    decade_mapping = continuous_with_start_valid[['事業名', '年代']].drop_duplicates()

    # budget_2024と事業名でマージ
    budget_with_decade = budget_2024.merge(
        decade_mapping,
        on='事業名',
        how='inner'
    )

    if len(budget_with_decade) > 0:
        # 年代ごとの統計を計算
        decade_stats = budget_with_decade.groupby('年代').agg({
            '当初予算(合計)': ['count', 'mean', 'std', 'min', 'max']
        }).reset_index()

        decade_stats.columns = ['年代', '事業数', '平均予算', '標準偏差', '最小予算', '最大予算']

        # 1920年代以降のみ表示
        decade_stats = decade_stats[decade_stats['年代'] >= 1920].sort_values('年代')

        print("**注**: 2024年度の当初予算額（百万円）に基づく統計\n")
        print("| 年代 | 事業数 | 平均予算（百万円） | 標準偏差 | 最小予算 | 最大予算 |")
        print("|------|--------|-------------------|----------|----------|----------|")
        for _, row in decade_stats.iterrows():
            decade_label = f"{int(row['年代'])}年代"
            count = int(row['事業数'])
            mean = row['平均予算']
            std = row['標準偏差']
            min_val = row['最小予算']
            max_val = row['最大予算']

            print(f"| {decade_label} | {count} | {mean:,.1f} | {std:,.1f} | {min_val:,.1f} | {max_val:,.1f} |")

        print()
        print("**統計の解釈**:")
        print("- **平均予算**: 各年代に開始された事業の2024年度予算の平均値")
        print("- **標準偏差**: 予算のばらつき（大きいほど予算規模に差がある）")
        print("- **最小/最大予算**: その年代で最も予算が少ない/多い事業の金額")
        print()
    else:
        print("**注**: 2024年度予算データとのマッチングができませんでした\n")


def analyze_covid_impact():
    """コロナ前後で影響を受けている事業を分析（2019-2024年の推移）"""
    print("\n---\n")
    print("# 3. コロナ前後で影響を受けている事業\n")
    print("**注**: コロナ禍の影響を測るため、2019、2020、2021、2022、2023、2024年度の6年間の予算推移を分析します。\n")

    overview = load_all_overview_data()
    budget = load_all_budget_data()

    # 事業名で紐づけるため、事業名ベースで分析
    # 各年度の予算を事業名で集計
    budget_by_name = budget[budget['予算年度'] == budget['データ年度']].groupby(['事業名', 'データ年度']).agg({
        '当初予算(合計)': 'sum'
    }).reset_index()

    # 2019-2024年の6年間のデータをピボット
    budget_pivot = budget_by_name[budget_by_name['データ年度'].isin([2019, 2020, 2021, 2022, 2023, 2024])].pivot(
        index='事業名',
        columns='データ年度',
        values='当初予算(合計)'
    ).reset_index()

    # 列名を変更
    budget_pivot.columns = ['事業名', '予算2019', '予算2020', '予算2021', '予算2022', '予算2023', '予算2024']

    # 6年間すべてのデータがある事業のみ抽出
    budget_pivot = budget_pivot.dropna(subset=['予算2019', '予算2020', '予算2021', '予算2022', '予算2023', '予算2024'])

    # 増減額・増減率を計算（2019→2024）
    budget_pivot['増減額'] = budget_pivot['予算2024'] - budget_pivot['予算2019']
    budget_pivot['増減率'] = (budget_pivot['増減額'] / budget_pivot['予算2019'] * 100).round(1)

    # 府省庁情報を追加（2024年度のデータから取得）
    overview_2024 = overview[overview['年度'] == 2024][['事業名', '府省庁']].drop_duplicates(subset=['事業名'])
    budget_pivot = budget_pivot.merge(overview_2024, on='事業名', how='left')

    # 2019年を基準に各年度との増減額・増減率を計算
    for year in [2020, 2021, 2022, 2023, 2024]:
        budget_pivot[f'増減額_{year}'] = budget_pivot[f'予算{year}'] - budget_pivot['予算2019']
        budget_pivot[f'増減率_{year}'] = (budget_pivot[f'増減額_{year}'] / budget_pivot['予算2019'] * 100).round(1)

    # 各年度で最大の増減額・増減率を記録
    budget_pivot['最大増減額'] = budget_pivot[['増減額_2020', '増減額_2021', '増減額_2022', '増減額_2023', '増減額_2024']].max(axis=1)
    budget_pivot['最小増減額'] = budget_pivot[['増減額_2020', '増減額_2021', '増減額_2022', '増減額_2023', '増減額_2024']].min(axis=1)
    budget_pivot['最大増減率'] = budget_pivot[['増減率_2020', '増減率_2021', '増減率_2022', '増減率_2023', '増減率_2024']].max(axis=1)
    budget_pivot['最小増減率'] = budget_pivot[['増減率_2020', '増減率_2021', '増減率_2022', '増減率_2023', '増減率_2024']].min(axis=1)

    # ピーク年度を特定
    increase_cols = ['増減額_2020', '増減額_2021', '増減額_2022', '増減額_2023', '増減額_2024']
    budget_pivot['ピーク年度'] = budget_pivot[increase_cols].idxmax(axis=1).str.replace('増減額_', '').astype(int)
    budget_pivot['ピーク増減額'] = budget_pivot[increase_cols].max(axis=1)

    # 統計サマリ
    print("## 全体統計（2019年度→2024年度）\n")
    print("| 項目 | 件数・値 |")
    print("|------|---------|")
    print(f"| 6年間継続事業数 | {len(budget_pivot)}件 |")
    print(f"| 2019→2024増加事業数 | {len(budget_pivot[budget_pivot['増減額_2024'] > 0])}件 |")
    print(f"| 2019→2024減少事業数 | {len(budget_pivot[budget_pivot['増減額_2024'] < 0])}件 |")
    print(f"| 2019→2024変化なし | {len(budget_pivot[budget_pivot['増減額_2024'] == 0])}件 |")
    print(f"| 平均増減率（2024時点） | {budget_pivot['増減率_2024'].replace([float('inf'), float('-inf')], 0).mean():.1f}% |")
    print(f"| 中央値増減率（2024時点） | {budget_pivot['増減率_2024'].median():.1f}% |")

    # 大幅増加した事業（いずれかの年度で増加率 > 100% かつ 増加額 > 10,000百万円）
    print("\n## 大幅増加した事業（2019年比でいずれかの年度が増加率100%以上 & 増加額10,000百万円（100億円）以上）\n")
    print("**注**: 2019年を基準に、2020-2024年のいずれかの年度で条件を満たした事業を表示します。\n")
    print("**ピーク年度**: 2019年比で最も増加額が大きかった年度を表示しています。\n")
    print("**ソート順**: 最大増減率の降順で表示（コロナ禍の影響を率として捉えるため）\n")

    increased = budget_pivot[
        (budget_pivot['最大増減率'] > 100) & (budget_pivot['最大増減額'] > 10000)
    ].sort_values('最大増減率', ascending=False).head(30)

    print("| 順位 | 事業名 | 府省庁 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | ピーク年度 | 最大増減率 | 最大増減額 |")
    print("|------|--------|--------|------|------|------|------|------|------|-----------|-----------|-----------|")
    for rank, (idx, row) in enumerate(increased.iterrows(), 1):
        project_name = row['事業名'][:30] + ('...' if len(row['事業名']) > 30 else '')
        ministry = row['府省庁'] if pd.notna(row['府省庁']) else '-'
        peak_year = int(row['ピーク年度'])
        print(f"| {rank} | {project_name} | {ministry} | {row['予算2019']:,.0f} | {row['予算2020']:,.0f} | {row['予算2021']:,.0f} | {row['予算2022']:,.0f} | {row['予算2023']:,.0f} | {row['予算2024']:,.0f} | {peak_year} | +{row['最大増減率']:.1f}% | +{row['最大増減額']:,.0f} |")

    # 大幅減少した事業（いずれかの年度で減少率 > 50% かつ 減少額 > 10,000百万円）
    print("\n## 大幅減少した事業（2019年比でいずれかの年度が減少率50%以上 & 減少額10,000百万円（100億円）以上）\n")
    print("**注**: 2019年を基準に、2020-2024年のいずれかの年度で条件を満たした事業を表示します。\n")
    print("**ボトム年度**: 2019年比で最も減少額が大きかった年度を表示しています。\n")
    print("**ソート順**: 最小増減率の昇順で表示（減少率が大きい順）\n")

    decreased = budget_pivot[
        (budget_pivot['最小増減率'] < -50) & (budget_pivot['最小増減額'] < -10000)
    ].sort_values('最小増減率').head(20)

    # ボトム年度を特定（最も減少した年度）
    decrease_cols = ['増減額_2020', '増減額_2021', '増減額_2022', '増減額_2023', '増減額_2024']
    decreased = decreased.copy()
    decreased['ボトム年度'] = decreased[decrease_cols].idxmin(axis=1).str.replace('増減額_', '').astype(int)
    decreased['ボトム増減額'] = decreased[decrease_cols].min(axis=1)

    print("| 順位 | 事業名 | 府省庁 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | ボトム年度 | 最小増減率 | 最大減少額 |")
    print("|------|--------|--------|------|------|------|------|------|------|-----------|-----------|-----------|")
    for rank, (idx, row) in enumerate(decreased.iterrows(), 1):
        project_name = row['事業名'][:30] + ('...' if len(row['事業名']) > 30 else '')
        ministry = row['府省庁'] if pd.notna(row['府省庁']) else '-'
        bottom_year = int(row['ボトム年度'])
        print(f"| {rank} | {project_name} | {ministry} | {row['予算2019']:,.0f} | {row['予算2020']:,.0f} | {row['予算2021']:,.0f} | {row['予算2022']:,.0f} | {row['予算2023']:,.0f} | {row['予算2024']:,.0f} | {bottom_year} | {row['最小増減率']:.1f}% | {row['ボトム増減額']:,.0f} |")

    # 雇用調整助成金の検索
    print("\n## 雇用調整助成金の推移（2019-2024年）\n")
    print("**注**: この事業は2021年にピークを迎え、その後正常化しました。最終年度（2024年）の増減額・増減率は2019年比の値です。\n")
    employment_subsidy = budget_pivot[budget_pivot['事業名'].str.contains('雇用調整', na=False)]
    if len(employment_subsidy) > 0:
        print("| 事業名 | 府省庁 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | ピーク年度 | 最大増減率 | 最大増減額 | 2024増減額 | 2024増減率 |")
        print("|--------|--------|------|------|------|------|------|------|-----------|-----------|-----------|-----------|-----------|")
        for _, row in employment_subsidy.iterrows():
            project_name = row['事業名'][:35] + ('...' if len(row['事業名']) > 35 else '')
            ministry = row['府省庁'] if pd.notna(row['府省庁']) else '-'
            peak_year = int(row['ピーク年度'])
            print(f"| {project_name} | {ministry} | {row['予算2019']:,.0f} | {row['予算2020']:,.0f} | {row['予算2021']:,.0f} | {row['予算2022']:,.0f} | {row['予算2023']:,.0f} | {row['予算2024']:,.0f} | {peak_year} | +{row['最大増減率']:.1f}% | +{row['最大増減額']:,.0f} | {row['増減額']:+,.0f} | {row['増減率']:+.1f}% |")
    else:
        print("**注**: 「雇用調整」を含む事業が2019-2024年の6年間継続データに見つかりませんでした。\n")
        print("可能性:")
        print("- 事業名が年度によって異なる")
        print("- 6年間のいずれかの年度でデータが欠損している")
        print("\n")


def main():
    """メイン処理"""
    # 出力ファイルのパス
    output_file = project_root / "data_quality" / "reports" / "historical_data_analysis_report.md"

    try:
        # ファイルを開いて標準出力をリダイレクト
        with open(output_file, 'w', encoding='utf-8') as f:
            # 標準出力を一時的にファイルにリダイレクト
            original_stdout = sys.stdout
            sys.stdout = f

            try:
                # ヘッダー
                print("# 2014-2024年度 行政事業レビューデータ 横断分析レポート\n")
                print(f"**生成日時**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                print(f"**対象年度**: 2014-2023年度（過去データ） + 2024年度（RSシステムデータ）\n")
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

            finally:
                # 標準出力を元に戻す
                sys.stdout = original_stdout

        # 成功メッセージを表示
        print(f"レポートを生成しました: {output_file}")

    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
