#!/usr/bin/env python3
"""
全年度（2014-2023）の長音を含むカタカナ語を全てCSV形式で出力

目的:
1. 全10年分のデータから長音を含むカタカナ語を網羅的に抽出
2. 1回以上出現した全単語をCSV化
3. 頻出度、カテゴリ情報を付与
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


def categorize_frequency(count):
    """頻出度をカテゴリ化"""
    if count >= 1000:
        return "超高頻出"
    elif count >= 500:
        return "高頻出"
    elif count >= 100:
        return "中頻出"
    elif count >= 50:
        return "準中頻出"
    elif count >= 10:
        return "低頻出"
    elif count >= 5:
        return "極低頻出"
    else:
        return "稀"


def determine_phase(count):
    """保持対象のPhaseを判定"""
    if count >= 100:
        return "Phase 1"
    elif count >= 50:
        return "Phase 2"
    elif count >= 10:
        return "Phase 3"
    else:
        return "-"


def categorize_word_type(word):
    """単語の種類をカテゴリ化（簡易版）"""
    # ビジネス・組織
    if re.search(r'(センター|グループ|チーム|パートナー|コンソーシアム)', word):
        return "ビジネス・組織"

    # 技術・IT
    if re.search(r'(データ|ネットワーク|システム|サーバ|ソフトウェア|ハードウェア|サイバー|プラットフォーム)', word):
        return "技術・IT"

    # エネルギー・環境
    if re.search(r'(エネルギー|グリーン|クリーン|リサイクル)', word):
        return "エネルギー・環境"

    # スポーツ・文化
    if re.search(r'(スポーツ|レクリエーション|アート|ミュージアム)', word):
        return "スポーツ・文化"

    # 教育・研修
    if re.search(r'(セミナー|ワークショップ|トレーニング|スクール)', word):
        return "教育・研修"

    # マーケティング・広報
    if re.search(r'(ホームページ|ポスター|フォーラム|プロモーション|アンケート)', word):
        return "マーケティング・広報"

    # サービス・サポート
    if re.search(r'(サービス|サポート|ケア|フォローアップ)', word):
        return "サービス・サポート"

    # 交通・インフラ
    if re.search(r'(ヘリコプター|レーダー|ネットワーク)', word):
        return "交通・インフラ"

    # その他
    return "その他"


def main():
    print("# 全長音含有カタカナ語のCSV出力\n")

    years = range(2014, 2024)
    all_word_counter = Counter()

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

        # 全体のカウンターに追加
        all_word_counter.update(word_counter)

        print(f"✓ {year}年度: {len(raw_df):,}行, {unique_words:,}語, {total_occurrences:,}回")

    total_unique = len(all_word_counter)
    total_occurrences = sum(all_word_counter.values())

    print(f"\n## 2. 全年度統合結果\n")
    print(f"- **ユニークな長音含有カタカナ語**: {total_unique:,}語")
    print(f"- **総出現回数**: {total_occurrences:,}回\n")

    # DataFrameの作成
    print("## 3. CSV形式に変換\n")

    data = []
    for word, count in all_word_counter.items():
        data.append({
            '単語': word,
            '出現回数': count,
            '頻出度カテゴリ': categorize_frequency(count),
            '保持Phase': determine_phase(count),
            '単語種類': categorize_word_type(word),
            '文字数': len(word),
            '長音数': word.count('ー')
        })

    df = pd.DataFrame(data)

    # 出現回数でソート
    df = df.sort_values('出現回数', ascending=False).reset_index(drop=True)

    # 順位を追加
    df.insert(0, '順位', range(1, len(df) + 1))

    print(f"✓ DataFrame作成完了: {len(df):,}行\n")

    # CSV出力
    output_file = project_root / "data_quality/all_longvowel_words_2014-2023.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"## 4. CSV出力完了\n")
    print(f"- ファイル: {output_file}")
    print(f"- 総単語数: {len(df):,}語")
    print(f"- ファイルサイズ: {output_file.stat().st_size / 1024:.1f} KB\n")

    # 統計サマリー
    print("## 5. 統計サマリー\n")

    print("### 5.1 頻出度カテゴリ別の分布\n")
    freq_dist = df['頻出度カテゴリ'].value_counts()
    print(freq_dist.to_string())
    print()

    print("\n### 5.2 保持Phase別の分布\n")
    phase_dist = df['保持Phase'].value_counts()
    print(phase_dist.to_string())
    print()

    print("\n### 5.3 単語種類別の分布（Phase 1のみ）\n")
    phase1_df = df[df['保持Phase'] == 'Phase 1']
    type_dist = phase1_df['単語種類'].value_counts()
    print(type_dist.to_string())
    print()

    print("\n### 5.4 文字数別の分布（Phase 1のみ）\n")
    length_dist = phase1_df['文字数'].value_counts().sort_index()
    print(length_dist.to_string())
    print()

    # トップ20とボトム20を表示
    print("## 6. サンプルデータ\n")

    print("### 6.1 トップ20単語\n")
    print(df.head(20).to_string(index=False))
    print()

    print("\n### 6.2 Phase 1最下位20単語（100回前後）\n")
    phase1_bottom = df[df['保持Phase'] == 'Phase 1'].tail(20)
    print(phase1_bottom.to_string(index=False))
    print()

    print("\n### 6.3 最稀な単語（1回のみ）のサンプル20語\n")
    rare_words = df[df['出現回数'] == 1].head(20)
    print(rare_words.to_string(index=False))
    print()

    print("---\n")
    print("**CSV出力完了**")
    print(f"\n全{len(df):,}語の長音含有カタカナ語を以下のファイルに出力しました:")
    print(f"→ {output_file}")


if __name__ == "__main__":
    main()
