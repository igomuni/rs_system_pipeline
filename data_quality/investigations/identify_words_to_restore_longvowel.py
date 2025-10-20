#!/usr/bin/env python3
"""
rawとnormalizedを比較して、長音に戻すべき単語を特定する

目的:
1. rawで長音（ー）が使われている単語を抽出
2. normalizedでハイフン（-）に変換されている箇所を特定
3. 頻出度と一般性を考慮して、長音に戻すべき単語をリスト化
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


def is_general_katakana_word(word):
    """
    一般的なカタカナ語かどうかを判定

    基準:
    - 外来語として一般的
    - ビジネス・技術用語として頻出
    - 固有名詞ではない
    """
    # 一般的なカタカナ語のパターン
    general_patterns = [
        r'.*ー(ション|ジ|ス|タ|ク|ル|プ|ト)$',  # -tion, -ge, -se, -ta, -ck, -le, -p, -t
        r'^(サー|ユー|デー|ネッ|コン|シス|プロ|マネ)',  # 一般的な接頭辞
    ]

    for pattern in general_patterns:
        if re.match(pattern, word):
            return True

    return False


def main():
    print("# rawとnormalizedの比較: 長音に戻すべき単語の特定\n")
    print("**調査対象**: 2014年度データ\n")

    # データ読み込み
    raw_file = project_root / "output/raw/year_2014/2014_データベース.csv"
    normalized_file = project_root / "output/normalized/year_2014/2014_データベース.csv"

    if not raw_file.exists() or not normalized_file.exists():
        print("エラー: ファイルが見つかりません")
        return

    print("## 1. データ読み込み\n")
    raw_df = pd.read_csv(raw_file, dtype=str)
    normalized_df = pd.read_csv(normalized_file, dtype=str)

    print(f"- raw: {len(raw_df)}行")
    print(f"- normalized: {len(normalized_df)}行\n")

    # rawから長音を含むカタカナ語を抽出
    print("## 2. 長音記号を含むカタカナ語の抽出（rawから）\n")

    word_counter = Counter()

    for col in raw_df.columns:
        for value in raw_df[col].dropna():
            if isinstance(value, str):
                words = extract_katakana_words_with_long_vowel(value)
                word_counter.update(words)

    print(f"- ユニークな長音含有カタカナ語: {len(word_counter)}語")
    print(f"- 総出現回数: {sum(word_counter.values())}回\n")

    # 頻出順にソート
    most_common_words = word_counter.most_common(200)

    print("## 3. 頻出カタカナ語（長音含む）TOP 50\n")
    print("| 順位 | 単語 | 出現回数 | 一般性 |")
    print("|------|------|---------|-------|")

    for rank, (word, count) in enumerate(most_common_words[:50], 1):
        is_general = "✓" if is_general_katakana_word(word) else ""
        print(f"| {rank} | {word} | {count:,} | {is_general} |")

    print("\n## 4. 長音に戻すべき単語の推奨リスト\n")

    # 頻出度 >= 10 または 一般的な語
    recommended_words = [
        (word, count)
        for word, count in most_common_words
        if count >= 10 or is_general_katakana_word(word)
    ]

    print(f"**推奨単語数**: {len(recommended_words)}語\n")
    print("### 4.1 高頻出語（出現回数10回以上）\n")

    high_freq = [(w, c) for w, c in recommended_words if c >= 10]

    print("| 単語 | 出現回数 | 変換後（現状） | 復元後（推奨） |")
    print("|------|---------|--------------|--------------|")

    for word, count in high_freq[:30]:
        converted = word.replace('ー', '-')
        print(f"| {word} | {count:,} | {converted} | {word} |")

    if len(high_freq) > 30:
        print(f"\n*（他{len(high_freq) - 30}語省略）*\n")

    print("\n### 4.2 一般的なカタカナ語（頻度問わず）\n")

    general_words = [(w, c) for w, c in recommended_words if is_general_katakana_word(w)]

    print("| 単語 | 出現回数 | 変換後（現状） | 復元後（推奨） |")
    print("|------|---------|--------------|--------------|")

    for word, count in general_words[:30]:
        converted = word.replace('ー', '-')
        print(f"| {word} | {count:,} | {converted} | {word} |")

    if len(general_words) > 30:
        print(f"\n*（他{len(general_words) - 30}語省略）*\n")

    print("\n## 5. 実装用の除外リスト（Python辞書形式）\n")
    print("```python")
    print("# 長音→ハイフン変換の除外リスト")
    print("# これらの単語は長音記号（ー）を保持する")
    print("LONG_VOWEL_PRESERVE_WORDS = {")

    # 頻出上位20語を実装例として出力
    top_words = [w for w, c in high_freq[:20]]

    for i, word in enumerate(top_words):
        comma = "," if i < len(top_words) - 1 else ""
        print(f"    '{word}'{comma}  # 出現回数: {word_counter[word]}回")

    print("}")
    print("```\n")

    print("## 6. カテゴリ別分類\n")

    # カテゴリ分類
    categories = {
        'ビジネス・組織': [],
        '技術・IT': [],
        '地名': [],
        '政策・制度': [],
        'その他': []
    }

    # パターンマッチングでカテゴリ分類
    for word, count in high_freq:
        if re.search(r'(マネー|サービス|ビジネス|プロジェクト)', word):
            categories['ビジネス・組織'].append((word, count))
        elif re.search(r'(システム|ネットワーク|データ|サーバー|コンピュー)', word):
            categories['技術・IT'].append((word, count))
        elif re.search(r'(アジア|アフリカ|ヨーロッパ|アメリカ)', word):
            categories['地名'].append((word, count))
        elif re.search(r'(セキュリティ|エネルギー|インフラ)', word):
            categories['政策・制度'].append((word, count))
        else:
            categories['その他'].append((word, count))

    for category, words in categories.items():
        if words:
            print(f"### 6.{list(categories.keys()).index(category) + 1} {category}\n")
            print("| 単語 | 出現回数 |")
            print("|------|---------|")
            for word, count in words[:10]:
                print(f"| {word} | {count:,} |")
            print()

    print("## 7. 実装方針の提案\n")

    print("### 7.1 段階的アプローチ\n")
    print("**フェーズ1: 高頻出語のみ保持**")
    print(f"- 対象: 出現回数50回以上の単語（{sum(1 for w, c in high_freq if c >= 50)}語）")
    print("- リスク: 低（明らかに一般的な語のみ）")
    print("- 効果: 可読性の大幅改善\n")

    print("**フェーズ2: 一般的なカタカナ語を追加**")
    print(f"- 対象: 出現回数10回以上の単語（{sum(1 for w, c in high_freq if c >= 10)}語）")
    print("- リスク: 中（やや専門的な語も含む）")
    print("- 効果: さらなる可読性向上\n")

    print("**フェーズ3: 全カタカナ語の長音保持**")
    print("- 対象: すべての長音記号")
    print("- リスク: 高（表記揺れが残る）")
    print("- 効果: 元データに忠実\n")

    print("### 7.2 推奨実装（フェーズ1）\n")

    very_high_freq = [(w, c) for w, c in high_freq if c >= 50]

    print(f"**除外リスト**: {len(very_high_freq)}語\n")
    print("```python")
    print("# src/utils/normalization.py に追加")
    print()
    print("# 長音記号を保持すべき高頻出カタカナ語")
    print("PRESERVE_LONG_VOWEL_WORDS = {")

    for i, (word, count) in enumerate(very_high_freq):
        comma = "," if i < len(very_high_freq) - 1 else ""
        print(f"    '{word}'{comma}")

    print("}")
    print()
    print("def normalize_hyphens(text: str) -> str:")
    print('    """')
    print("    各種ハイフン・ダッシュ記号を標準的な「-」に統一")
    print("    ただし、高頻出カタカナ語の長音記号は保持")
    print('    """')
    print("    # 一時的に保護")
    print("    protected_words = {}")
    print("    for word in PRESERVE_LONG_VOWEL_WORDS:")
    print("        if word in text:")
    print("            placeholder = f'__PRESERVE_{len(protected_words)}__'")
    print("            protected_words[placeholder] = word")
    print("            text = text.replace(word, placeholder)")
    print()
    print("    # ハイフン統一処理")
    print("    for char in HYPHEN_CHARS:")
    print("        text = text.replace(char, '-')")
    print()
    print("    # 保護した単語を復元")
    print("    for placeholder, word in protected_words.items():")
    print("        text = text.replace(placeholder, word)")
    print()
    print("    return text")
    print("```\n")

    print("---\n")
    print("**調査完了**")


if __name__ == "__main__":
    main()
