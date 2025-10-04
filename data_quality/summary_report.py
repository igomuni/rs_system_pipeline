#!/usr/bin/env python3
"""年度別サマリーレポート"""
import pandas as pd
from pathlib import Path

output_dir = Path("output/processed")

print("=" * 100)
print("年度別予算・執行額サマリー")
print("=" * 100)
print()

summary_data = []

for year in range(2014, 2024):
    year_dir = output_dir / f"year_{year}"
    budget_file = year_dir / f"2-1_{year}_予算・執行_サマリ.csv"

    if not budget_file.exists():
        continue

    budget_df = pd.read_csv(budget_file, low_memory=False)
    current_year_budget = budget_df[budget_df['予算年度'] == year]

    if len(current_year_budget) == 0:
        summary_data.append({
            '年度': year,
            '事業数': 0,
            '当初予算(10億円)': 0,
            '執行額(10億円)': 0,
            '執行率(%)': 0
        })
        continue

    # 数値変換
    initial_budget = pd.to_numeric(current_year_budget['当初予算(合計)'], errors='coerce').fillna(0).sum()
    execution = pd.to_numeric(current_year_budget['執行額(合計)'], errors='coerce').fillna(0).sum()

    # 10億円単位に変換（百万円 → 10億円は1/10,000）
    initial_budget_oku = initial_budget / 10000
    execution_oku = execution / 10000

    # 執行率計算（当初予算に対する）
    execution_rate = (execution / initial_budget * 100) if initial_budget > 0 else 0

    summary_data.append({
        '年度': year,
        '事業数': len(current_year_budget),
        '当初予算(10億円)': initial_budget_oku,
        '執行額(10億円)': execution_oku,
        '執行率(%)': execution_rate
    })

# DataFrame化
summary_df = pd.DataFrame(summary_data)

# テーブル表示
print(f"{'年度':<8} {'事業数':<10} {'当初予算(10億円)':>20} {'執行額(10億円)':>20} {'執行率(%)':>15}")
print("-" * 100)

for _, row in summary_df.iterrows():
    year = int(row['年度'])
    count = int(row['事業数'])
    budget = row['当初予算(10億円)']
    execution = row['執行額(10億円)']
    rate = row['執行率(%)']

    # 異常値フラグ
    flag = ""
    if budget > 100000:  # 100兆円以上
        flag = " ⚠️ 異常値"

    print(f"{year:<8} {count:<10,} {budget:>20,.1f} {execution:>20,.1f} {rate:>14.1f}%{flag}")

print("-" * 100)
print()

# 統計サマリー
valid_data = summary_df[summary_df['事業数'] > 0]
print("【統計サマリー】")
print(f"  対象年度数: {len(valid_data)}年")
print(f"  平均事業数: {valid_data['事業数'].mean():,.0f}件")
print(f"  平均当初予算: {valid_data['当初予算(10億円)'].mean():,.1f} 10億円")
print(f"  平均執行額: {valid_data['執行額(10億円)'].mean():,.1f} 10億円")
print(f"  平均執行率: {valid_data['執行率(%)'].mean():.1f}%")
print()

# 異常値検出
print("【データ品質チェック】")
anomalies = []
for _, row in summary_df.iterrows():
    if row['当初予算(10億円)'] > 100000:  # 100兆円以上
        anomalies.append(f"  - {int(row['年度'])}年: 当初予算が異常に大きい ({row['当初予算(10億円)']:,.1f} 10億円)")
    if row['執行率(%)'] > 200:
        anomalies.append(f"  - {int(row['年度'])}年: 執行率が異常に高い ({row['執行率(%)']:.1f}%)")
    if row['執行率(%)'] < 50 and row['事業数'] > 0:
        anomalies.append(f"  - {int(row['年度'])}年: 執行率が低い ({row['執行率(%)']:.1f}%)")

if anomalies:
    for anomaly in anomalies:
        print(anomaly)
else:
    print("  異常値は検出されませんでした")

print()
print("=" * 100)
print("注記:")
print("  - 金額は百万円を10億円に換算（1兆円 = 1,000 × 10億円）")
print("  - 2014年の異常値は元データの単位間違いの可能性が高い")
print("  - 執行率 = 執行額 / 当初予算 × 100")
print("  - 補正予算を考慮すると執行率はより正確になります")
print("=" * 100)
