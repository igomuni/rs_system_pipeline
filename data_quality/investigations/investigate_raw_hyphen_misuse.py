#!/usr/bin/env python3
"""
rawデータで長音（ー）の代わりにハイフン（-）が使われているケースを調査

目的:
1. all_longvowel_words_2014-2023.csvの長音含有単語リストを読み込む
2. 各単語の長音をハイフンに置換したパターンを生成
3. rawデータ内でハイフン版が使われているケースを検出
4. 元データの表記揺れを定量化
"""

import pandas as pd
from pathlib import Path
import re
from collections import Counter, defaultdict

project_root = Path(__file__).parent.parent


def main():
    print("# rawデータにおける長音の誤用（ハイフン使用）調査\n")

    # 長音含有単語リストを読み込み
    csv_file = project_root / "data_quality/all_longvowel_words_2014-2023.csv"

    if not csv_file.exists():
        print(f"エラー: {csv_file} が見つかりません")
        return

    print("## 1. 長音含有単語リストの読み込み\n")
    longvowel_df = pd.read_csv(csv_file)

    print(f"- 総単語数: {len(longvowel_df):,}語")
    print(f"- 最高出現回数: {longvowel_df['出現回数'].max():,}回")
    print(f"- 最低出現回数: {longvowel_df['出現回数'].min():,}回\n")

    # ハイフン版パターンを生成
    print("## 2. ハイフン版パターンの生成\n")

    word_patterns = {}  # 長音版 -> ハイフン版
    for _, row in longvowel_df.iterrows():
        word_long = row['単語']
        word_hyphen = word_long.replace('ー', '-')
        word_patterns[word_long] = word_hyphen

    print(f"- 生成したパターン数: {len(word_patterns):,}個\n")

    # 全年度のrawデータでハイフン版が使われているケースを検出
    print("## 3. rawデータにおけるハイフン使用の検出\n")

    years = range(2014, 2024)
    misuse_counter = Counter()  # 長音版 -> ハイフン版の出現回数
    misuse_examples = defaultdict(list)  # 長音版 -> [(year, column, value)]

    for year in years:
        year_dir = project_root / f"output/raw/year_{year}"

        if not year_dir.exists():
            continue

        csv_files = list(year_dir.glob("*.csv"))
        if not csv_files:
            continue

        raw_file = csv_files[0]
        raw_df = pd.read_csv(raw_file, dtype=str)

        # 各セルをチェック
        for col in raw_df.columns:
            for value in raw_df[col].dropna():
                if not isinstance(value, str) or '-' not in value:
                    continue

                # 長音含有単語のハイフン版がこのセルに含まれているかチェック
                for word_long, word_hyphen in word_patterns.items():
                    if word_hyphen in value:
                        misuse_counter[word_long] += 1

                        # 例を最大5件まで保存
                        if len(misuse_examples[word_long]) < 5:
                            # 該当箇所の前後50文字を取得
                            idx = value.find(word_hyphen)
                            start = max(0, idx - 50)
                            end = min(len(value), idx + len(word_hyphen) + 50)
                            context = value[start:end]

                            misuse_examples[word_long].append({
                                'year': year,
                                'column': col,
                                'context': context,
                                'hyphen_version': word_hyphen
                            })

        print(f"✓ {year}年度: {len(raw_df):,}行を検索")

    print()

    # 結果の集計
    total_misuse = len(misuse_counter)
    total_occurrences = sum(misuse_counter.values())

    print("## 4. 検出結果サマリー\n")
    print(f"- **ハイフン使用が検出された単語数**: {total_misuse:,}語")
    print(f"- **総検出回数**: {total_occurrences:,}回")
    print(f"- **長音含有単語総数**: {len(word_patterns):,}語")
    print(f"- **ハイフン使用率**: {total_misuse/len(word_patterns)*100:.1f}%\n")

    if total_misuse == 0:
        print("✅ **素晴らしい結果**: rawデータで長音の代わりにハイフンが使われているケースは検出されませんでした。\n")
        print("→ 元データの品質が非常に高く、長音とハイフンの使い分けが適切です。\n")
        print("---\n")
        print("**調査完了**")
        return

    # 頻出順にソート
    misuse_sorted = misuse_counter.most_common()

    print("## 5. ハイフン使用が検出された単語（上位50語）\n")
    print("| 順位 | 長音版（正） | ハイフン版（誤） | 検出回数 | 元の出現回数 | 誤用率 |")
    print("|------|------------|----------------|---------|------------|-------|")

    for i, (word_long, count) in enumerate(misuse_sorted[:50], 1):
        word_hyphen = word_patterns[word_long]

        # 元の出現回数を取得
        original_count = longvowel_df[longvowel_df['単語'] == word_long]['出現回数'].values[0]

        # 誤用率を計算（ハイフン版 / (長音版 + ハイフン版)）
        misuse_rate = count / (original_count + count) * 100

        print(f"| {i} | {word_long} | {word_hyphen} | {count:,} | {original_count:,} | {misuse_rate:.1f}% |")

    if len(misuse_sorted) > 50:
        print(f"\n*（他{len(misuse_sorted) - 50}語省略）*\n")

    # 誤用率が高い単語
    print("\n## 6. 誤用率が高い単語（50%以上）\n")

    high_misuse = []
    for word_long, count in misuse_sorted:
        original_count = longvowel_df[longvowel_df['単語'] == word_long]['出現回数'].values[0]
        misuse_rate = count / (original_count + count) * 100

        if misuse_rate >= 50:
            high_misuse.append((word_long, count, original_count, misuse_rate))

    if high_misuse:
        print("| 長音版（正） | ハイフン版検出回数 | 長音版出現回数 | 誤用率 |")
        print("|------------|------------------|--------------|-------|")

        for word_long, hyphen_count, long_count, rate in sorted(high_misuse, key=lambda x: x[3], reverse=True):
            print(f"| {word_long} | {hyphen_count:,} | {long_count:,} | {rate:.1f}% |")

        print(f"\n**誤用率50%以上の単語**: {len(high_misuse)}語\n")
    else:
        print("誤用率50%以上の単語はありませんでした。\n")

    # 具体例を表示
    print("\n## 7. 具体的な誤用例（上位10単語）\n")

    for i, (word_long, count) in enumerate(misuse_sorted[:10], 1):
        word_hyphen = word_patterns[word_long]

        print(f"### 7.{i} {word_long} → {word_hyphen} ({count:,}回検出)\n")

        examples = misuse_examples[word_long]

        print("| 年度 | 列名 | 前後の文脈 |")
        print("|------|------|-----------|")

        for ex in examples[:3]:  # 最大3件
            context = ex['context'].replace('\n', ' ')
            if len(context) > 100:
                context = context[:97] + '...'

            print(f"| {ex['year']} | {ex['column']} | {context} |")

        print()

    # CSV出力
    print("\n## 8. CSV出力\n")

    output_data = []
    for word_long, count in misuse_sorted:
        word_hyphen = word_patterns[word_long]
        original_count = longvowel_df[longvowel_df['単語'] == word_long]['出現回数'].values[0]
        misuse_rate = count / (original_count + count) * 100

        # Phase情報を取得
        phase = longvowel_df[longvowel_df['単語'] == word_long]['保持Phase'].values[0]

        # 例を取得
        examples = misuse_examples[word_long]
        example_years = ', '.join(sorted(set(str(ex['year']) for ex in examples)))

        output_data.append({
            '順位': len(output_data) + 1,
            '長音版（正）': word_long,
            'ハイフン版（誤）': word_hyphen,
            'ハイフン検出回数': count,
            '長音出現回数': original_count,
            '誤用率（%）': round(misuse_rate, 1),
            '保持Phase': phase,
            '検出年度': example_years
        })

    output_df = pd.DataFrame(output_data)
    output_file = project_root / "data_quality/raw_hyphen_misuse_2014-2023.csv"
    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✓ CSV出力完了: {output_file}")
    print(f"  - 誤用検出単語数: {len(output_df):,}語")
    print(f"  - ファイルサイズ: {output_file.stat().st_size / 1024:.1f} KB\n")

    # 統計分析
    print("\n## 9. 統計分析\n")

    print("### 9.1 誤用率の分布\n")

    misuse_rate_dist = {
        '0-10%': 0,
        '10-20%': 0,
        '20-30%': 0,
        '30-40%': 0,
        '40-50%': 0,
        '50-60%': 0,
        '60-70%': 0,
        '70-80%': 0,
        '80-90%': 0,
        '90-100%': 0
    }

    for _, row in output_df.iterrows():
        rate = row['誤用率（%）']
        if rate < 10:
            misuse_rate_dist['0-10%'] += 1
        elif rate < 20:
            misuse_rate_dist['10-20%'] += 1
        elif rate < 30:
            misuse_rate_dist['20-30%'] += 1
        elif rate < 40:
            misuse_rate_dist['30-40%'] += 1
        elif rate < 50:
            misuse_rate_dist['40-50%'] += 1
        elif rate < 60:
            misuse_rate_dist['50-60%'] += 1
        elif rate < 70:
            misuse_rate_dist['60-70%'] += 1
        elif rate < 80:
            misuse_rate_dist['70-80%'] += 1
        elif rate < 90:
            misuse_rate_dist['80-90%'] += 1
        else:
            misuse_rate_dist['90-100%'] += 1

    for rate_range, count in misuse_rate_dist.items():
        print(f"- {rate_range}: {count}語")

    print()

    print("### 9.2 Phase別の誤用状況\n")

    phase_misuse = output_df.groupby('保持Phase').agg({
        'ハイフン検出回数': 'sum',
        '長音出現回数': 'sum'
    })

    print("| Phase | ハイフン検出回数 | 長音出現回数 | 平均誤用率 |")
    print("|-------|----------------|------------|----------|")

    for phase, row in phase_misuse.iterrows():
        hyphen_count = row['ハイフン検出回数']
        long_count = row['長音出現回数']
        avg_rate = hyphen_count / (long_count + hyphen_count) * 100 if (long_count + hyphen_count) > 0 else 0

        print(f"| {phase} | {hyphen_count:,} | {long_count:,} | {avg_rate:.1f}% |")

    print()

    print("## 10. まとめ\n")
    print(f"- rawデータにおいて、長音の代わりにハイフンが使われているケースが**{total_misuse:,}語**で検出されました")
    print(f"- 総検出回数: **{total_occurrences:,}回**")
    print(f"- これは元データの品質問題であり、normalizeステージでの統一処理の妥当性を裏付けています\n")

    if high_misuse:
        print(f"- 特に誤用率が高い単語（50%以上）が**{len(high_misuse)}語**存在します")
        print("- これらは元データでの表記揺れが深刻な単語です\n")

    print("**推奨対応**:")
    print("1. rawデータでの表記揺れを許容し、normalizeステージで統一する（現行方針）")
    print("2. ただし、長音記号を保持すべき高頻出語は除外リストで保護する")
    print("3. ハイフン版も検索対象に含めることで、表記揺れに対応する\n")

    print("---\n")
    print("**調査完了**")


if __name__ == "__main__":
    main()
