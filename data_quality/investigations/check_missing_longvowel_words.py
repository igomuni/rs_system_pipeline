#!/usr/bin/env python3
"""
rawとnormalizedを比較して、82語の保持リストに含まれていない変換済み単語を特定

目的:
1. rawで長音（ー）が使われている単語を抽出
2. normalizedで同じ位置がハイフン（-）に変換されているか確認
3. 82語の推奨リストに含まれていない単語をリストアップ
4. 追加すべき単語を提案
"""

import pandas as pd
from pathlib import Path
import re
from collections import Counter

project_root = Path(__file__).parent.parent

# 既に推奨リストに含まれている82語（出現50回以上）
ALREADY_IN_LIST = {
    'エネルギー', 'センター', 'データ', 'サービス', 'イノベーション',
    'ネットワーク', 'システム', 'ユーザー', 'サポート', 'インフラ',
    'セキュリティー', 'テクノロジー', 'マネージャー', 'パートナー', 'スキーム',
    'プロジェクト', 'コーディネーター', 'モニタリング', 'プログラム', 'ステークホルダー',
    'ワークショップ', 'サマリー', 'コンテンツ', 'インターネット', 'プラットフォーム',
    'ケア', 'フォローアップ', 'ヘルスケア', 'レビュー', 'アウトソーシング',
    'ドナー', 'インキュベーター', 'レーザー', 'リーダー', 'トレーニング',
    'サプライヤー', 'オペレーター', 'アジア', 'コンピューター', 'デザイナー',
    'インフォメーション', 'スーパー', 'チェーン', 'アフリカ', 'マネジメント',
    'シミュレーション', 'ソフトウェア', 'ハードウェア', 'ソリューション', 'リサーチ',
    'アナリスト', 'デザイン', 'メンテナンス', 'カスタマー', 'サプライチェーン',
    'ファイナンス', 'クリエイティブ', 'マーケティング', 'ディレクター', 'スタンダード',
    'トレーサビリティー', 'アーカイブ', 'リソース', 'トレード', 'エンジニアリング',
    'コントロール', 'セミナー', 'キャリア', 'アウトリーチ', 'インターフェース',
    'ストレージ', 'ビジネスモデル', 'テーマ', 'パーソナル', 'ワーキンググループ',
    'コラボレーション', 'イニシアティブ', 'エージェンシー', 'コーポレーション', 'インスティテュート',
    'リテラシー', 'シナリオ'
}


def extract_katakana_words_with_long_vowel(text):
    """長音記号を含むカタカナ語を抽出"""
    if not isinstance(text, str):
        return []

    # カタカナ + 長音記号のパターン
    pattern = r'[ァ-ヴー]+'
    words = re.findall(pattern, text)

    # 長音記号を含む、3文字以上のカタカナ語のみ
    return [w for w in words if len(w) >= 3 and 'ー' in w]


def extract_katakana_words_with_hyphen(text):
    """ハイフンを含むカタカナ語を抽出（normalized用）"""
    if not isinstance(text, str):
        return []

    # カタカナ + ハイフンのパターン
    pattern = r'[ァ-ヴ-]+'
    words = re.findall(pattern, text)

    # ハイフンを含む、3文字以上のカタカナ語のみ
    return [w for w in words if len(w) >= 3 and '-' in w]


def normalize_for_comparison(word):
    """比較用に正規化: 長音とハイフンを統一"""
    return word.replace('ー', '*').replace('-', '*')


def main():
    print("# rawとnormalizedの比較: 82語リスト外の変換済み単語\n")
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
    print("## 2. rawから長音を含むカタカナ語を抽出\n")

    raw_words = Counter()
    for col in raw_df.columns:
        for value in raw_df[col].dropna():
            if isinstance(value, str):
                words = extract_katakana_words_with_long_vowel(value)
                raw_words.update(words)

    print(f"- ユニークな長音含有カタカナ語: {len(raw_words)}語")
    print(f"- 総出現回数: {sum(raw_words.values())}回\n")

    # normalizedからハイフンを含むカタカナ語を抽出
    print("## 3. normalizedからハイフンを含むカタカナ語を抽出\n")

    normalized_words = Counter()
    for col in normalized_df.columns:
        for value in normalized_df[col].dropna():
            if isinstance(value, str):
                words = extract_katakana_words_with_hyphen(value)
                normalized_words.update(words)

    print(f"- ユニークなハイフン含有カタカナ語: {len(normalized_words)}語")
    print(f"- 総出現回数: {sum(normalized_words.values())}回\n")

    # 長音→ハイフン変換されたペアを特定
    print("## 4. 長音→ハイフン変換されたペアを特定\n")

    converted_pairs = {}  # 長音版 -> (ハイフン版, 出現回数)

    for raw_word, raw_count in raw_words.items():
        normalized_form = normalize_for_comparison(raw_word)

        # 対応するハイフン版を探す
        for norm_word, norm_count in normalized_words.items():
            if normalize_for_comparison(norm_word) == normalized_form and '-' in norm_word:
                converted_pairs[raw_word] = (norm_word, raw_count)
                break

    print(f"- 変換されたペア数: {len(converted_pairs)}組\n")

    # 82語リストに含まれていない単語を抽出
    print("## 5. 82語リスト外の変換済み単語\n")

    missing_words = {
        word: (converted, count)
        for word, (converted, count) in converted_pairs.items()
        if word not in ALREADY_IN_LIST
    }

    print(f"**リスト外の変換済み単語**: {len(missing_words)}語\n")

    # 頻出順にソート
    sorted_missing = sorted(missing_words.items(), key=lambda x: x[1][1], reverse=True)

    print("### 5.1 頻出上位50語（リスト外）\n")
    print("| 順位 | 元の単語（長音） | 変換後（ハイフン） | 出現回数 | 推奨 |")
    print("|------|----------------|------------------|---------|------|")

    recommendations = []

    for rank, (word, (converted, count)) in enumerate(sorted_missing[:50], 1):
        # 推奨基準: 出現10回以上 または 一般的な単語
        is_recommended = count >= 10
        recommend_mark = "✓" if is_recommended else ""

        if is_recommended:
            recommendations.append((word, count))

        print(f"| {rank} | {word} | {converted} | {count:,} | {recommend_mark} |")

    if len(sorted_missing) > 50:
        print(f"\n*（他{len(sorted_missing) - 50}語省略）*\n")

    # 追加推奨リスト
    print("\n## 6. 82語リストへの追加推奨\n")

    print(f"**追加推奨単語数**: {len(recommendations)}語\n")

    if len(recommendations) > 0:
        print("### 6.1 追加すべき単語（出現10回以上）\n")
        print("| 単語 | 出現回数 | 理由 |")
        print("|------|---------|------|")

        for word, count in recommendations[:30]:
            reason = "高頻出" if count >= 50 else "中頻出"
            print(f"| {word} | {count:,} | {reason} |")

        if len(recommendations) > 30:
            print(f"\n*（他{len(recommendations) - 30}語省略）*\n")

        print("\n### 6.2 更新後の実装コード\n")
        print("```python")
        print("# 長音記号を保持すべきカタカナ語（更新版）")
        print("PRESERVE_LONG_VOWEL_WORDS = {")

        # 既存82語 + 追加推奨語をマージ
        all_recommended = sorted(list(ALREADY_IN_LIST) + [w for w, c in recommendations])

        for i, word in enumerate(all_recommended[:20]):  # 最初の20語のみ表示
            comma = "," if i < len(all_recommended) - 1 else ""
            print(f"    '{word}'{comma}")

        print(f"    # ... 他{len(all_recommended) - 20}語")
        print("}")
        print("```\n")

        print(f"**更新後の合計**: {len(all_recommended)}語\n")
    else:
        print("**結論**: 現在の82語リストは十分に包括的です。追加推奨はありません。\n")

    # カバレッジ分析
    print("\n## 7. カバレッジ分析\n")

    total_converted = len(converted_pairs)
    covered = len(ALREADY_IN_LIST & set(converted_pairs.keys()))
    coverage_rate = covered / total_converted * 100 if total_converted > 0 else 0

    total_occurrences = sum(count for word, (_, count) in converted_pairs.items())
    covered_occurrences = sum(count for word, (_, count) in converted_pairs.items() if word in ALREADY_IN_LIST)
    occurrence_coverage = covered_occurrences / total_occurrences * 100 if total_occurrences > 0 else 0

    print(f"| 指標 | カバー済み | 全体 | カバー率 |")
    print(f"|------|-----------|------|---------|")
    print(f"| ユニーク単語数 | {covered} | {total_converted} | {coverage_rate:.1f}% |")
    print(f"| 総出現回数 | {covered_occurrences:,} | {total_occurrences:,} | {occurrence_coverage:.1f}% |")
    print()

    print("## 8. まとめ\n")

    if len(recommendations) > 0:
        print(f"- 現在の82語リストは、**単語数の{coverage_rate:.1f}%**、**出現回数の{occurrence_coverage:.1f}%** をカバー")
        print(f"- 追加推奨: **{len(recommendations)}語**（出現10回以上）")
        print(f"- 更新後の合計: **{len(ALREADY_IN_LIST) + len(recommendations)}語**\n")
        print("**推奨アクション**: 上記の追加推奨単語を PRESERVE_LONG_VOWEL_WORDS に追加することを検討してください。\n")
    else:
        print(f"- 現在の82語リストは、**単語数の{coverage_rate:.1f}%**、**出現回数の{occurrence_coverage:.1f}%** をカバー")
        print("- 追加推奨なし（出現10回以上の未カバー単語なし）\n")
        print("**結論**: 現在の82語リストは十分に包括的です。そのまま実装可能です。\n")

    print("---\n")
    print("**調査完了**")


if __name__ == "__main__":
    main()
