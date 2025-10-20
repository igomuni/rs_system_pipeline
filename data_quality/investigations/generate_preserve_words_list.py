#!/usr/bin/env python3
"""
長音記号を保持すべきカタカナ語のリストを生成

目的:
1. rawデータから長音を含むカタカナ語を抽出
2. 頻出度に基づいて保持すべき単語をリスト化
3. テキストファイルとして出力
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
    print("# 長音記号を保持すべきカタカナ語リストの生成\n")

    # データ読み込み
    raw_file = project_root / "output/raw/year_2014/2014_データベース.csv"

    if not raw_file.exists():
        print(f"エラー: ファイルが見つかりません - {raw_file}")
        return

    print("## 1. データ読み込み\n")
    raw_df = pd.read_csv(raw_file, dtype=str)
    print(f"- raw: {len(raw_df)}行\n")

    # rawから長音を含むカタカナ語を抽出
    print("## 2. 長音記号を含むカタカナ語の抽出\n")

    word_counter = Counter()

    for col in raw_df.columns:
        for value in raw_df[col].dropna():
            if isinstance(value, str):
                words = extract_katakana_words_with_long_vowel(value)
                word_counter.update(words)

    print(f"- ユニークな長音含有カタカナ語: {len(word_counter)}語")
    print(f"- 総出現回数: {sum(word_counter.values())}回\n")

    # 頻出順にソート
    most_common_words = word_counter.most_common()

    # Phase 1: 出現50回以上
    phase1_words = [(word, count) for word, count in most_common_words if count >= 50]

    # Phase 2: 出現10回以上
    phase2_words = [(word, count) for word, count in most_common_words if 10 <= count < 50]

    print(f"## 3. 抽出結果\n")
    print(f"- Phase 1（50回以上）: {len(phase1_words)}語")
    print(f"- Phase 2（10回以上50回未満）: {len(phase2_words)}語")
    print(f"- 合計: {len(phase1_words) + len(phase2_words)}語\n")

    # Phase 1のリストをテキストファイルに出力
    output_file = project_root / "data_quality/PRESERVE_LONG_VOWEL_WORDS.txt"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 長音記号を保持すべきカタカナ語リスト\n")
        f.write("# Phase 1: 出現回数50回以上の高頻出語\n")
        f.write(f"# 生成日: 2025-10-20\n")
        f.write(f"# 対象データ: 2014年度\n")
        f.write(f"# 単語数: {len(phase1_words)}語\n")
        f.write("#\n")
        f.write("# 使用方法:\n")
        f.write("# このファイルの単語は、src/utils/normalization.pyのPRESERVE_LONG_VOWEL_WORDSに含めてください。\n")
        f.write("# これらの単語は長音記号（ー）をハイフン（-）に変換せず、元の表記を保持します。\n")
        f.write("\n")

        for word, count in phase1_words:
            # 長音が含まれていることを再確認
            if 'ー' in word:
                f.write(f"{word}\t{count}\n")

    print(f"## 4. 出力完了\n")
    print(f"- ファイル: {output_file}")
    print(f"- Phase 1 単語数: {len(phase1_words)}語\n")

    # Phase 1のトップ30を表示
    print("## 5. Phase 1 単語一覧（上位30語）\n")
    print("| 順位 | 単語 | 出現回数 |")
    print("|------|------|---------|")

    valid_count = 0
    for i, (word, count) in enumerate(phase1_words, 1):
        if 'ー' in word:
            print(f"| {i} | {word} | {count:,} |")
            valid_count += 1
            if valid_count >= 30:
                break

    if len(phase1_words) > 30:
        print(f"\n*（他{len(phase1_words) - 30}語省略）*\n")

    # Python辞書形式も出力
    python_output = project_root / "data_quality/PRESERVE_LONG_VOWEL_WORDS.py"

    with open(python_output, 'w', encoding='utf-8') as f:
        f.write('"""\n')
        f.write('長音記号を保持すべきカタカナ語のセット\n')
        f.write('Phase 1: 出現回数50回以上の高頻出語\n')
        f.write('\n')
        f.write('このファイルは自動生成されています。\n')
        f.write('生成スクリプト: data_quality/generate_preserve_words_list.py\n')
        f.write('"""\n\n')
        f.write('# 長音記号を保持すべき高頻出カタカナ語\n')
        f.write('PRESERVE_LONG_VOWEL_WORDS = {\n')

        # 長音を含む単語のみをフィルタ
        valid_words = [(word, count) for word, count in phase1_words if 'ー' in word]

        for i, (word, count) in enumerate(valid_words):
            comma = ',' if i < len(valid_words) - 1 else ''
            f.write(f"    '{word}'{comma}  # {count:,}回\n")

        f.write('}\n')

    print(f"\n## 6. Python辞書形式も出力\n")
    print(f"- ファイル: {python_output}")
    print(f"- 単語数: {len([w for w, c in phase1_words if 'ー' in w])}語\n")

    print("---\n")
    print("**生成完了**")


if __name__ == "__main__":
    main()
