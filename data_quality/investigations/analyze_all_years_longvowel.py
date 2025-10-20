#!/usr/bin/env python3
"""
全年度（2014-2023）のrawとnormalizedを比較して長音→ハイフン変換された単語を抽出

目的:
1. 全10年分のデータから長音を含むカタカナ語を網羅的に抽出
2. 1回以上出現した単語をすべてリストアップ
3. 頻出度別に分類して保持候補を提案
"""

import pandas as pd
from pathlib import Path
import re
from collections import Counter

project_root = Path(__file__).parent.parent


def extract_katakana_words_with_long_vowel(text):
    """長音記号を含むカタカナ語を抽出"""
    if not isinstance(text, str):
        return []

    # カタカナ + 長音記号のパターン
    pattern = r'[ァ-ヴー]+'
    words = re.findall(pattern, text)

    # 長音記号を含む、3文字以上のカタカナ語のみ
    return [w for w in words if len(w) >= 3 and 'ー' in w]


def main():
    print("# 全年度（2014-2023）長音→ハイフン変換単語の調査\n")

    years = range(2014, 2024)
    all_word_counter = Counter()
    year_stats = []

    print("## 1. 各年度のデータ読み込みと単語抽出\n")

    for year in years:
        # 年度ごとにファイル名が異なる可能性があるため、ディレクトリ内のCSVを探す
        year_dir = project_root / f"output/raw/year_{year}"

        if not year_dir.exists():
            print(f"⚠️  {year}年度: ディレクトリが見つかりません - スキップ")
            continue

        # ディレクトリ内のCSVファイルを探す
        csv_files = list(year_dir.glob("*.csv"))

        if not csv_files:
            print(f"⚠️  {year}年度: CSVファイルが見つかりません - スキップ")
            continue

        # 最初のCSVファイルを使用（データベース.csv または Sheet1.csv など）
        raw_file = csv_files[0]

        # データ読み込み
        raw_df = pd.read_csv(raw_file, dtype=str)

        # 長音を含むカタカナ語を抽出
        word_counter = Counter()
        for col in raw_df.columns:
            for value in raw_df[col].dropna():
                if isinstance(value, str):
                    words = extract_katakana_words_with_long_vowel(value)
                    word_counter.update(words)

        unique_words = len(word_counter)
        total_occurrences = sum(word_counter.values())

        year_stats.append({
            'year': year,
            'rows': len(raw_df),
            'unique_words': unique_words,
            'total_occurrences': total_occurrences
        })

        # 全体のカウンターに追加
        all_word_counter.update(word_counter)

        print(f"✓ {year}年度: {len(raw_df):,}行, {unique_words:,}語, {total_occurrences:,}回")

    print(f"\n## 2. 全年度統合結果\n")

    total_unique = len(all_word_counter)
    total_occurrences = sum(all_word_counter.values())

    print(f"- **対象年度**: 2014-2023年（10年間）")
    print(f"- **ユニークな長音含有カタカナ語**: {total_unique:,}語")
    print(f"- **総出現回数**: {total_occurrences:,}回")
    print(f"- **平均出現回数**: {total_occurrences/total_unique:.1f}回/語\n")

    # 頻出度別の分類
    print("## 3. 頻出度別の分類\n")

    freq_categories = [
        (1000, "超高頻出"),
        (500, "高頻出"),
        (100, "中頻出"),
        (50, "準中頻出"),
        (10, "低頻出"),
        (5, "極低頻出"),
        (1, "1-4回")
    ]

    print("| カテゴリ | 閾値 | 単語数 | 累計単語数 | 出現回数合計 | 累計出現回数 |")
    print("|---------|------|--------|-----------|------------|------------|")

    cumulative_words = 0
    cumulative_occurrences = 0

    for threshold, category in freq_categories:
        words_in_category = [(w, c) for w, c in all_word_counter.items() if c >= threshold]

        if threshold == 1:
            # 1回以上（全体）
            count = len(words_in_category)
            occurrences = sum(c for w, c in words_in_category)
        else:
            # 次の閾値との差分
            next_threshold = freq_categories[freq_categories.index((threshold, category)) + 1][0] if freq_categories.index((threshold, category)) < len(freq_categories) - 1 else 0
            words_in_category = [(w, c) for w, c in all_word_counter.items() if next_threshold < c <= threshold or (threshold == 1 and c >= 1)]
            count = len(words_in_category)
            occurrences = sum(c for w, c in words_in_category)

        cumulative_words += count
        cumulative_occurrences += occurrences

        print(f"| {category} | {threshold}回以上 | {count:,} | {cumulative_words:,} | {occurrences:,} | {cumulative_occurrences:,} |")

    print()

    # Phase分類の提案
    print("## 4. 保持対象のPhase分類提案\n")

    phase1 = [(w, c) for w, c in all_word_counter.items() if c >= 100]
    phase2 = [(w, c) for w, c in all_word_counter.items() if 50 <= c < 100]
    phase3 = [(w, c) for w, c in all_word_counter.items() if 10 <= c < 50]

    print("| Phase | 基準 | 単語数 | 出現回数合計 | カバー率 |")
    print("|-------|------|--------|------------|---------|")
    print(f"| **Phase 1** | 100回以上 | {len(phase1):,} | {sum(c for w, c in phase1):,} | {sum(c for w, c in phase1)/total_occurrences*100:.1f}% |")
    print(f"| **Phase 2** | 50-99回 | {len(phase2):,} | {sum(c for w, c in phase2):,} | {sum(c for w, c in phase2)/total_occurrences*100:.1f}% |")
    print(f"| **Phase 3** | 10-49回 | {len(phase3):,} | {sum(c for w, c in phase3):,} | {sum(c for w, c in phase3)/total_occurrences*100:.1f}% |")
    print(f"| **合計** | 10回以上 | {len(phase1)+len(phase2)+len(phase3):,} | {sum(c for w, c in phase1+phase2+phase3):,} | {sum(c for w, c in phase1+phase2+phase3)/total_occurrences*100:.1f}% |")
    print()

    # Phase 1の上位50語を表示
    print("## 5. Phase 1: 100回以上の高頻出語（上位50語）\n")
    print("| 順位 | 単語 | 出現回数 |")
    print("|------|------|---------|")

    phase1_sorted = sorted(phase1, key=lambda x: x[1], reverse=True)

    for i, (word, count) in enumerate(phase1_sorted[:50], 1):
        print(f"| {i} | {word} | {count:,} |")

    if len(phase1_sorted) > 50:
        print(f"\n*（他{len(phase1_sorted) - 50}語省略）*\n")

    # Phase 2の上位30語を表示
    print("\n## 6. Phase 2: 50-99回の中頻出語（上位30語）\n")
    print("| 順位 | 単語 | 出現回数 |")
    print("|------|------|---------|")

    phase2_sorted = sorted(phase2, key=lambda x: x[1], reverse=True)

    for i, (word, count) in enumerate(phase2_sorted[:30], 1):
        print(f"| {i} | {word} | {count:,} |")

    if len(phase2_sorted) > 30:
        print(f"\n*（他{len(phase2_sorted) - 30}語省略）*\n")

    # ファイル出力
    print("\n## 7. 出力ファイル生成\n")

    # Phase 1をテキストファイルに出力
    output_file = project_root / "data_quality/PRESERVE_LONG_VOWEL_WORDS_ALL_YEARS.txt"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 長音記号を保持すべきカタカナ語リスト（全年度統合版）\n")
        f.write("# 対象: 2014-2023年度（10年間）\n")
        f.write("# Phase 1: 出現回数100回以上の高頻出語\n")
        f.write(f"# 生成日: 2025-10-20\n")
        f.write(f"# 単語数: {len(phase1)}語\n")
        f.write("#\n")
        f.write("# 使用方法:\n")
        f.write("# このファイルの単語は、src/utils/normalization.pyのPRESERVE_LONG_VOWEL_WORDSに含めてください。\n")
        f.write("# これらの単語は長音記号（ー）をハイフン（-）に変換せず、元の表記を保持します。\n")
        f.write("\n")

        for word, count in phase1_sorted:
            f.write(f"{word}\t{count}\n")

    print(f"✓ Phase 1リスト: {output_file}")
    print(f"  - 単語数: {len(phase1)}語\n")

    # Python辞書形式も出力
    python_output = project_root / "data_quality/PRESERVE_LONG_VOWEL_WORDS_ALL_YEARS.py"

    with open(python_output, 'w', encoding='utf-8') as f:
        f.write('"""\n')
        f.write('長音記号を保持すべきカタカナ語のセット（全年度統合版）\n')
        f.write('対象: 2014-2023年度（10年間）\n')
        f.write('Phase 1: 出現回数100回以上の高頻出語\n')
        f.write('\n')
        f.write('このファイルは自動生成されています。\n')
        f.write('生成スクリプト: data_quality/analyze_all_years_longvowel.py\n')
        f.write('"""\n\n')
        f.write('# 長音記号を保持すべき高頻出カタカナ語（全年度統合）\n')
        f.write('PRESERVE_LONG_VOWEL_WORDS = {\n')

        for i, (word, count) in enumerate(phase1_sorted):
            comma = ',' if i < len(phase1_sorted) - 1 else ''
            f.write(f"    '{word}'{comma}  # {count:,}回\n")

        f.write('}\n')

    print(f"✓ Python辞書形式: {python_output}")
    print(f"  - 単語数: {len(phase1)}語\n")

    # Phase 2+3のリストも出力（参考用）
    extended_output = project_root / "data_quality/PRESERVE_LONG_VOWEL_WORDS_EXTENDED.txt"

    with open(extended_output, 'w', encoding='utf-8') as f:
        f.write("# 長音記号を保持すべきカタカナ語リスト（拡張版）\n")
        f.write("# 対象: 2014-2023年度（10年間）\n")
        f.write("# Phase 2+3: 出現回数10回以上の中・低頻出語\n")
        f.write(f"# 生成日: 2025-10-20\n")
        f.write(f"# 単語数: {len(phase2)+len(phase3)}語\n")
        f.write("\n")

        combined = sorted(phase2 + phase3, key=lambda x: x[1], reverse=True)

        for word, count in combined:
            f.write(f"{word}\t{count}\n")

    print(f"✓ Phase 2+3リスト（参考用）: {extended_output}")
    print(f"  - 単語数: {len(phase2)+len(phase3)}語\n")

    # 統計サマリー
    print("## 8. 統計サマリー\n")

    print("### 8.1 年度別統計\n")
    print("| 年度 | 行数 | ユニーク単語数 | 総出現回数 | 平均出現回数 |")
    print("|------|------|--------------|-----------|------------|")

    for stat in year_stats:
        avg = stat['total_occurrences'] / stat['unique_words'] if stat['unique_words'] > 0 else 0
        print(f"| {stat['year']} | {stat['rows']:,} | {stat['unique_words']:,} | {stat['total_occurrences']:,} | {avg:.1f} |")

    print()

    print("### 8.2 推奨実装方針\n")
    print("**段階的アプローチ**:\n")
    print(f"1. **Phase 1のみ実装** ({len(phase1)}語)")
    print(f"   - カバー率: {sum(c for w, c in phase1)/total_occurrences*100:.1f}%")
    print(f"   - リスク: 低（明らかに一般的な語のみ）")
    print(f"   - 推奨度: ★★★★★\n")

    print(f"2. **Phase 1+2実装** ({len(phase1)+len(phase2)}語)")
    print(f"   - カバー率: {sum(c for w, c in phase1+phase2)/total_occurrences*100:.1f}%")
    print(f"   - リスク: 中（やや専門的な語も含む）")
    print(f"   - 推奨度: ★★★★☆\n")

    print(f"3. **Phase 1+2+3実装** ({len(phase1)+len(phase2)+len(phase3)}語)")
    print(f"   - カバー率: {sum(c for w, c in phase1+phase2+phase3)/total_occurrences*100:.1f}%")
    print(f"   - リスク: 中（固有名詞も含む可能性）")
    print(f"   - 推奨度: ★★★☆☆\n")

    print("---\n")
    print("**調査完了**")


if __name__ == "__main__":
    main()
