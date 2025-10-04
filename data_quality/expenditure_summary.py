#!/usr/bin/env python3
"""年度別支出先サマリーレポート"""
import pandas as pd
from pathlib import Path

output_dir = Path("output/processed")

print("=" * 120)
print("年度別支出先データサマリー")
print("=" * 120)
print()

summary_data = []

for year in range(2014, 2024):
    year_dir = output_dir / f"year_{year}"
    exp_file = year_dir / f"5-1_{year}_支出先_支出情報.csv"

    if not exp_file.exists():
        continue

    exp_df = pd.read_csv(exp_file, low_memory=False)

    # 支出額合計（百万円）
    total_expenditure = pd.to_numeric(exp_df['支出額（百万円）'], errors='coerce').fillna(0).sum()
    avg_expenditure = pd.to_numeric(exp_df['支出額（百万円）'], errors='coerce').fillna(0).mean()

    # 10億円単位に変換
    total_exp_10b = total_expenditure / 10000
    avg_exp_million = avg_expenditure  # 平均は百万円のまま

    summary_data.append({
        '年度': year,
        '支出先件数': len(exp_df),
        '支出額合計(10億円)': total_exp_10b,
        '平均支出額(百万円)': avg_exp_million
    })

# DataFrame化
summary_df = pd.DataFrame(summary_data)

# テーブル表示
print(f"{'年度':<8} {'支出先件数':>12} {'支出額合計(10億円)':>22} {'平均支出額(百万円)':>22}")
print("-" * 120)

for _, row in summary_df.iterrows():
    year = int(row['年度'])
    count = int(row['支出先件数'])
    total = row['支出額合計(10億円)']
    avg = row['平均支出額(百万円)']

    # 異常値フラグ
    flag = ""
    if total > 100000:  # 100兆円以上
        flag = " ⚠️ 異常値"
    elif avg > 1000000:  # 平均1兆円以上
        flag = " ⚠️ 異常値"

    print(f"{year:<8} {count:>12,} {total:>22,.2f} {avg:>22,.2f}{flag}")

print("-" * 120)
print()

# 統計サマリー
print("【統計サマリー】")
print(f"  対象年度数: {len(summary_df)}年")
print(f"  平均支出先件数: {summary_df['支出先件数'].mean():,.0f}件")
print(f"  平均支出額合計: {summary_df['支出額合計(10億円)'].mean():,.2f} 10億円")
print(f"  平均の平均支出額: {summary_df['平均支出額(百万円)'].mean():,.2f} 百万円")
print()

# 異常値検出
print("【データ品質チェック】")
anomalies = []
for _, row in summary_df.iterrows():
    if row['支出額合計(10億円)'] > 100000:  # 100兆円以上
        anomalies.append(f"  - {int(row['年度'])}年: 支出額合計が異常に大きい ({row['支出額合計(10億円)']:,.1f} 10億円 = {row['支出額合計(10億円)']/1000:.1f}兆円)")
    if row['平均支出額(百万円)'] > 1000000:  # 平均1兆円以上
        anomalies.append(f"  - {int(row['年度'])}年: 平均支出額が異常に大きい ({row['平均支出額(百万円)']:,.1f} 百万円)")

if anomalies:
    for anomaly in anomalies:
        print(anomaly)
else:
    print("  異常値は検出されませんでした")

print()
print("=" * 120)
print("注記:")
print("  - 支出額合計は百万円を10億円に換算（1兆円 = 1,000 × 10億円）")
print("  - 平均支出額は百万円単位")
print("  - 2014年、2016年、2021年に異常値が見られます（元データの単位間違いの可能性）")
print("=" * 120)
