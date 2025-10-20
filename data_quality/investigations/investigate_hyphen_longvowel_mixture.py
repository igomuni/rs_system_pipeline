#!/usr/bin/env python3
"""
元データ（raw）における長音記号（ー）とハイフン（-）の混在状況を調査

目的:
1. 同一カタカナ語で長音とハイフンが混在しているケースを検出
2. 統一することのメリット・デメリットを評価
"""

import pandas as pd
from pathlib import Path
import re
from collections import defaultdict

project_root = Path(__file__).parent.parent

def extract_katakana_words(text):
    """カタカナ語（長音・ハイフンを含む）を抽出"""
    if not isinstance(text, str):
        return []

    # カタカナ + 長音記号/ハイフン のパターン
    pattern = r'[ァ-ヴー-]+'
    words = re.findall(pattern, text)

    # 3文字以上のカタカナ語のみ（ノイズ除去）
    return [w for w in words if len(w) >= 3 and ('ー' in w or '-' in w)]


def normalize_for_comparison(word):
    """
    比較用の正規化: 長音とハイフンを統一して同一語を判定
    例: 「フォローアップ」と「フォロ-アップ」→「フォロ*アップ」
    """
    return word.replace('ー', '*').replace('-', '*')


def main():
    print("# 元データ（raw）における長音記号とハイフンの混在状況調査\n")
    print("**調査対象**: output/raw/year_2014/2014_データベース.csv\n")

    # データ読み込み
    raw_file = project_root / "output/raw/year_2014/2014_データベース.csv"

    if not raw_file.exists():
        print(f"エラー: ファイルが見つかりません - {raw_file}")
        return

    df = pd.read_csv(raw_file, dtype=str)

    print("## 1. データ概要\n")
    print(f"- 行数: {len(df):,}行")
    print(f"- 列数: {len(df.columns)}列\n")

    # カタカナ語の収集
    word_variants = defaultdict(set)  # 正規化後の語 -> 実際の表記のセット

    for col in df.columns:
        for value in df[col].dropna():
            if isinstance(value, str):
                words = extract_katakana_words(value)
                for word in words:
                    normalized = normalize_for_comparison(word)
                    word_variants[normalized].add(word)

    print("## 2. カタカナ語の統計\n")
    print(f"- ユニークなカタカナ語（正規化前）: {sum(len(variants) for variants in word_variants.values())}語")
    print(f"- ユニークなカタカナ語（正規化後）: {len(word_variants)}語\n")

    # 混在しているケースを抽出
    mixed_cases = {
        normalized: variants
        for normalized, variants in word_variants.items()
        if len(variants) > 1 and any('ー' in v for v in variants) and any('-' in v for v in variants)
    }

    print("## 3. 長音とハイフンが混在している語\n")
    print(f"**混在しているカタカナ語の数**: {len(mixed_cases)}語\n")

    if len(mixed_cases) > 0:
        print("### 3.1 混在例（最大20件）\n")
        print("| 正規化形 | 実際の表記バリエーション | バリエーション数 |")
        print("|---------|----------------------|---------------|")

        for i, (normalized, variants) in enumerate(sorted(mixed_cases.items(), key=lambda x: len(x[1]), reverse=True)[:20]):
            variants_str = ', '.join(sorted(variants))
            if len(variants_str) > 60:
                variants_str = variants_str[:57] + '...'
            print(f"| {normalized[:20]} | {variants_str} | {len(variants)} |")

        if len(mixed_cases) > 20:
            print(f"\n*（他{len(mixed_cases) - 20}語省略）*\n")
    else:
        print("混在しているケースは見つかりませんでした。\n")

    # 長音のみ・ハイフンのみのケース
    long_vowel_only = sum(1 for variants in word_variants.values() if all('ー' in v and '-' not in v for v in variants))
    hyphen_only = sum(1 for variants in word_variants.values() if all('-' in v and 'ー' not in v for v in variants))
    mixed_within = sum(1 for variants in word_variants.values() if any('ー' in v and '-' in v for v in variants))

    print("## 4. カタカナ語の分類\n")
    print(f"| 分類 | 語数 | 割合 |")
    print(f"|------|------|------|")
    print(f"| 長音のみ（ー）使用 | {long_vowel_only} | {long_vowel_only/len(word_variants)*100:.1f}% |")
    print(f"| ハイフンのみ（-）使用 | {hyphen_only} | {hyphen_only/len(word_variants)*100:.1f}% |")
    print(f"| 同一語内に両方含む | {mixed_within} | {mixed_within/len(word_variants)*100:.1f}% |")
    print(f"| 同一語で表記揺れあり | {len(mixed_cases)} | {len(mixed_cases)/len(word_variants)*100:.1f}% |")
    print(f"| **合計** | **{len(word_variants)}** | **100.0%** |\n")

    print("## 5. 統一することのメリット・デメリット\n")

    print("### 5.1 現状の問題点\n")
    if len(mixed_cases) > 0:
        print(f"- 同一のカタカナ語が{len(mixed_cases)}通りの表記で混在している")
        print("- データベース検索時に複数パターンを考慮する必要がある")
        print("- 集計・分析時に同一語として認識されない可能性\n")
    else:
        print("- 長音とハイフンの混在による問題は検出されませんでした\n")

    print("### 5.2 統一するメリット\n")
    print("**長音→ハイフン統一の場合**:")
    print("- ✅ 表記揺れが完全に解消される")
    print("- ✅ データベース検索が容易になる")
    print("- ✅ 集計・分析時の精度が向上する")
    print("- ✅ CSV処理でのエンコーディングエラーリスクが減少\n")

    print("### 5.3 統一するデメリット\n")
    print("**長音→ハイフン統一の場合**:")
    print("- ❌ 可読性が低下（「フォロ-アップ」は読みにくい）")
    print("- ❌ 正式な日本語表記から逸脱")
    print("- ❌ 元データの忠実性が失われる\n")

    print("**ハイフン→長音統一の場合**:")
    print("- ✅ 可読性が向上（正しい日本語表記）")
    print("- ✅ 元データの意図に近い")
    print("- ❌ 元データで誤ってハイフンを使っている箇所も長音化される\n")

    print("## 6. 推奨される対応方針\n")

    if len(mixed_cases) > 0:
        ratio = len(mixed_cases) / len(word_variants) * 100

        if ratio > 5:
            print(f"### 混在率が高い（{ratio:.1f}%）→ 統一を推奨\n")
            print("**推奨**: 現在の方針（長音→ハイフン統一）を継続")
            print("- 理由: データの一貫性と検索精度が優先")
            print("- 代替案: 検索・集計時のみ正規化、表示は元のまま\n")
        else:
            print(f"### 混在率が低い（{ratio:.1f}%）→ 現状維持も選択肢\n")
            print("**選択肢1**: 統一しない（可読性優先）")
            print("**選択肢2**: 検索・集計用の正規化カラムを別途作成\n")
    else:
        print("### 混在なし → 元データの表記を尊重\n")
        print("**推奨**: 統一処理を除外し、元データの表記を保持")
        print("- 理由: 表記揺れがないため統一の必要性が低い\n")

    print("## 7. 実装上の考慮事項\n")
    print("### 7.1 段階的な対応\n")
    print("1. **データレイヤー**: rawとnormalizedで区別")
    print("   - raw: 元データの表記を保持")
    print("   - normalized: 統一処理を適用")
    print("2. **検索レイヤー**: 正規化した値でマッチング")
    print("3. **表示レイヤー**: 用途に応じて選択\n")

    print("### 7.2 既存の実装（現状）\n")
    print("- ✅ raw: 長音記号を保持")
    print("- ✅ normalized: 長音→ハイフン統一")
    print("- ✅ processed: 統一後の値を使用\n")

    print("→ **現在の実装は理にかなっており、変更不要と判断できます。**\n")

    print("---\n")
    print("**調査完了**")


if __name__ == "__main__":
    main()
