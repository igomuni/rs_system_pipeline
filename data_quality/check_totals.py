#!/usr/bin/env python3
"""各年度の予算総額と支出総額を確認"""
import pandas as pd
from pathlib import Path

output_dir = Path("output/processed")

print("=" * 80)
print("各年度の予算総額と支出総額")
print("=" * 80)
print()

for year in range(2014, 2024):
    year_dir = output_dir / f"year_{year}"

    budget_file = year_dir / f"2-1_{year}_予算・執行_サマリ.csv"
    expenditure_file = year_dir / f"5-1_{year}_支出先_支出情報.csv"

    if not budget_file.exists():
        print(f"【{year}年】 予算ファイル未生成")
        continue

    print(f"【{year}年】")
    print("-" * 80)

    # 予算データ
    budget_df = pd.read_csv(budget_file, low_memory=False)

    # 当該年度の予算のみ抽出
    current_year_budget = budget_df[budget_df['予算年度'] == year]

    # 予算関連の列（実際のカラム名）
    budget_columns = {
        '当初予算(合計)': '当初予算',
        '補正予算(合計)': '補正予算',
        '前年度からの繰越し(合計)': '繰越額',
        '執行額(合計)': '執行額'
    }

    print(f"  予算年度={year}のレコード数: {len(current_year_budget):,}件")
    print()

    for col_name, display_name in budget_columns.items():
        if col_name in current_year_budget.columns:
            # 数値に変換（エラーは0にする）
            values = pd.to_numeric(current_year_budget[col_name], errors='coerce').fillna(0)
            total = values.sum()

            # 統計情報
            non_zero = values[values > 0]
            if len(non_zero) > 0:
                print(f"  【{display_name}】")
                print(f"    合計: {total:,.1f} 百万円 ({total/1000:,.1f} 億円)")
                print(f"    件数: {len(non_zero):,}件 / {len(values):,}件")
                print(f"    最小: {non_zero.min():,.1f}, 最大: {non_zero.max():,.1f}, 平均: {non_zero.mean():,.1f}")

                # 異常値チェック（極端に大きい/小さい値）
                if non_zero.max() > 1000000:  # 100万百万円 = 1兆円以上
                    print(f"    ⚠️ 異常に大きい値を検出: {non_zero.max():,.1f} 百万円")
                if non_zero.min() < 0.01 and non_zero.min() > 0:  # 1万円未満
                    print(f"    ⚠️ 異常に小さい値を検出: {non_zero.min():.6f} 百万円")
                print()

    # 支出データ
    if expenditure_file.exists():
        exp_df = pd.read_csv(expenditure_file, low_memory=False)

        if '支出額' in exp_df.columns:
            exp_values = pd.to_numeric(exp_df['支出額'], errors='coerce').fillna(0)
            exp_total = exp_values.sum()
            exp_non_zero = exp_values[exp_values > 0]

            print(f"  【支出先への支出額】")
            print(f"    合計: {exp_total:,.1f} 百万円 ({exp_total/1000:,.1f} 億円)")
            print(f"    件数: {len(exp_non_zero):,}件 / {len(exp_values):,}件")
            if len(exp_non_zero) > 0:
                print(f"    最小: {exp_non_zero.min():,.1f}, 最大: {exp_non_zero.max():,.1f}, 平均: {exp_non_zero.mean():,.1f}")

                # 異常値チェック
                if exp_non_zero.max() > 1000000:
                    print(f"    ⚠️ 異常に大きい値を検出: {exp_non_zero.max():,.1f} 百万円")
                if exp_non_zero.min() < 0.01 and exp_non_zero.min() > 0:
                    print(f"    ⚠️ 異常に小さい値を検出: {exp_non_zero.min():.6f} 百万円")

    print()
    print()

print("=" * 80)
print("注意:")
print("  - 金額は百万円単位")
print("  - 元データの入力ミス（単位間違い等）が含まれる可能性があります")
print("  - 予算現額 ≈ 当初予算額 + 補正予算額 + 繰越額")
print("  - 執行額が予算現額を超える場合もあり得ます（流用等）")
print("=" * 80)
