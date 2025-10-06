#!/usr/bin/env python3
"""
2023年データをRSシステム形式に変換するためのギャップ分析

RSシステム2024の全15ファイルと2023年の既存3ファイルを比較し、
不足しているファイル・データ項目を特定します。
"""

import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

RS_DATA_DIR = PROJECT_ROOT / "data" / "unzipped"
YEAR_2023_DIR = PROJECT_ROOT / "output" / "processed" / "year_2023"


def get_columns(file_path):
    """CSVファイルのカラム名を取得"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)
        return list(df.columns)
    except Exception as e:
        print(f"  ❌ エラー: {e}")
        return []


def analyze_file_structure():
    """RSシステムと2023年のファイル構造を比較"""
    print("=" * 80)
    print("RSシステム形式への変換ギャップ分析（2023年対象）")
    print("=" * 80)

    # RSシステム2024のファイル一覧
    rs_files = sorted(RS_DATA_DIR.glob("*_RS_2024_*.csv"))

    print(f"\n📁 RSシステム2024: {len(rs_files)}ファイル")
    print(f"📁 2023年既存データ: 3ファイル")

    # ファイルカテゴリ別に整理
    categories = {
        "1-基本情報": [],
        "2-予算・執行": [],
        "3-効果発現経路": [],
        "4-点検・評価": [],
        "5-支出先": [],
        "6-その他": []
    }

    for file in rs_files:
        name = file.name
        prefix = name.split("_")[0]

        if prefix.startswith("1-"):
            categories["1-基本情報"].append(file)
        elif prefix.startswith("2-"):
            categories["2-予算・執行"].append(file)
        elif prefix.startswith("3-"):
            categories["3-効果発現経路"].append(file)
        elif prefix.startswith("4-"):
            categories["4-点検・評価"].append(file)
        elif prefix.startswith("5-"):
            categories["5-支出先"].append(file)
        elif prefix.startswith("6-"):
            categories["6-その他"].append(file)

    # カテゴリ別に表示
    gap_summary = []

    for category, files in categories.items():
        print(f"\n## {category}")
        print("-" * 60)

        for file in files:
            file_id = file.name.split("_")[0]
            file_desc = file.name.replace("_RS_2024_", "_").replace(".csv", "")

            # 2023年に対応するファイルがあるか確認
            year_2023_file = YEAR_2023_DIR / f"{file_id}_2023_{file.name.split('_', 3)[3]}"

            if year_2023_file.exists():
                status = "✅ 既存"
                # カラム数を比較
                rs_cols = get_columns(file)
                y23_cols = get_columns(year_2023_file)

                col_diff = len(rs_cols) - len(y23_cols)
                if col_diff > 0:
                    status += f" (RS: {len(rs_cols)}列, 2023: {len(y23_cols)}列, +{col_diff}列)"
                    gap_summary.append({
                        "ファイル": file_id,
                        "状態": "既存（列追加必要）",
                        "RS列数": len(rs_cols),
                        "2023列数": len(y23_cols),
                        "差分": col_diff
                    })
                else:
                    status += f" ({len(rs_cols)}列)"
                    gap_summary.append({
                        "ファイル": file_id,
                        "状態": "既存（完全一致）",
                        "RS列数": len(rs_cols),
                        "2023列数": len(y23_cols),
                        "差分": 0
                    })
            else:
                rs_cols = get_columns(file)
                status = f"❌ 新規作成必要 ({len(rs_cols)}列)"
                gap_summary.append({
                    "ファイル": file_id,
                    "状態": "新規作成必要",
                    "RS列数": len(rs_cols),
                    "2023列数": 0,
                    "差分": len(rs_cols)
                })

            print(f"  {file_id}: {status}")
            print(f"    {file_desc}")

    return gap_summary, categories


def analyze_column_details(categories):
    """既存ファイルのカラム詳細比較"""
    print("\n" + "=" * 80)
    print("既存ファイルのカラム詳細比較")
    print("=" * 80)

    # 対応するファイルのマッピング
    file_mapping = {
        "1-2": ("1-2_RS_2024_基本情報_事業概要等.csv", "1-2_2023_基本情報_事業概要.csv"),
        "2-1": ("2-1_RS_2024_予算・執行_サマリ.csv", "2-1_2023_予算・執行_サマリ.csv"),
        "5-1": ("5-1_RS_2024_支出先_支出情報.csv", "5-1_2023_支出先_支出情報.csv")
    }

    column_diffs = {}

    for file_id, (rs_name, y23_name) in file_mapping.items():
        print(f"\n## {file_id}: {rs_name.split('_', 3)[3].replace('.csv', '')}")
        print("-" * 60)

        rs_file = RS_DATA_DIR / rs_name
        y23_file = YEAR_2023_DIR / y23_name

        if not y23_file.exists():
            print(f"  ⚠️  2023年ファイルが見つかりません: {y23_name}")
            continue

        rs_cols = get_columns(rs_file)
        y23_cols = get_columns(y23_file)

        # 共通カラム
        common = set(rs_cols) & set(y23_cols)
        # RSにのみ存在
        rs_only = set(rs_cols) - set(y23_cols)
        # 2023にのみ存在
        y23_only = set(y23_cols) - set(rs_cols)

        print(f"\n  📊 カラム統計:")
        print(f"    RSシステム: {len(rs_cols)}列")
        print(f"    2023年: {len(y23_cols)}列")
        print(f"    共通: {len(common)}列")
        print(f"    RS追加: {len(rs_only)}列")
        print(f"    2023独自: {len(y23_only)}列")

        if rs_only:
            print(f"\n  ➕ RSシステムに追加されたカラム ({len(rs_only)}個):")
            for col in sorted(rs_only)[:10]:  # 最大10個表示
                print(f"    - {col}")
            if len(rs_only) > 10:
                print(f"    ... 他{len(rs_only) - 10}個")

        if y23_only:
            print(f"\n  ➖ 2023年のみのカラム ({len(y23_only)}個):")
            for col in sorted(y23_only):
                print(f"    - {col}")

        column_diffs[file_id] = {
            "rs_cols": rs_cols,
            "y23_cols": y23_cols,
            "common": common,
            "rs_only": rs_only,
            "y23_only": y23_only
        }

    return column_diffs


def generate_report(gap_summary, column_diffs):
    """レポート生成"""
    report_path = PROJECT_ROOT / "data_quality" / "rs_conversion_gap_2023.md"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# 2023年データのRSシステム形式変換ギャップ分析\n\n")
        f.write(f"生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # サマリ
        f.write("## 1. サマリ\n\n")

        new_files = [g for g in gap_summary if g["状態"] == "新規作成必要"]
        existing_files = [g for g in gap_summary if "既存" in g["状態"]]
        needs_update = [g for g in existing_files if g["差分"] > 0]

        f.write(f"- **RSシステム総ファイル数**: 15ファイル\n")
        f.write(f"- **2023年既存ファイル数**: 3ファイル\n")
        f.write(f"- **新規作成必要**: {len(new_files)}ファイル\n")
        f.write(f"- **既存（更新必要）**: {len(needs_update)}ファイル\n")
        f.write(f"- **既存（そのまま利用可）**: {len(existing_files) - len(needs_update)}ファイル\n\n")

        # ファイル別状況
        f.write("## 2. ファイル別の状況\n\n")
        f.write("| ファイル | 状態 | RS列数 | 2023列数 | 差分 |\n")
        f.write("|---------|------|--------|----------|------|\n")

        for g in gap_summary:
            f.write(f"| {g['ファイル']} | {g['状態']} | {g['RS列数']} | {g['2023列数']} | ")
            if g['差分'] > 0:
                f.write(f"+{g['差分']} |\n")
            else:
                f.write(f"{g['差分']} |\n")

        # 新規作成必要なファイル
        f.write("\n## 3. 新規作成が必要なファイル\n\n")

        if new_files:
            for g in new_files:
                f.write(f"### {g['ファイル']}\n\n")
                f.write(f"- **列数**: {g['RS列数']}列\n")
                f.write(f"- **データソース**: 2023年の元データから新規抽出が必要\n\n")
        else:
            f.write("該当なし\n\n")

        # カラム詳細比較
        f.write("\n## 4. 既存ファイルのカラム詳細比較\n\n")

        for file_id, diffs in column_diffs.items():
            f.write(f"### {file_id}\n\n")

            f.write(f"**カラム統計**:\n")
            f.write(f"- RSシステム: {len(diffs['rs_cols'])}列\n")
            f.write(f"- 2023年: {len(diffs['y23_cols'])}列\n")
            f.write(f"- 共通: {len(diffs['common'])}列\n")
            f.write(f"- RS追加: {len(diffs['rs_only'])}列\n")
            f.write(f"- 2023独自: {len(diffs['y23_only'])}列\n\n")

            if diffs['rs_only']:
                f.write(f"**RSシステムに追加されたカラム** ({len(diffs['rs_only'])}個):\n")
                for col in sorted(diffs['rs_only']):
                    f.write(f"- `{col}`\n")
                f.write("\n")

            if diffs['y23_only']:
                f.write(f"**2023年のみのカラム** ({len(diffs['y23_only'])}個):\n")
                for col in sorted(diffs['y23_only']):
                    f.write(f"- `{col}`\n")
                f.write("\n")

        # 変換方針
        f.write("\n## 5. 変換方針の推奨\n\n")

        f.write("### フェーズ1: 既存ファイルの拡張\n\n")
        f.write("既存の3ファイル（1-2, 2-1, 5-1）について、RSシステムで追加されたカラムを追加:\n\n")

        for file_id, diffs in column_diffs.items():
            if diffs['rs_only']:
                f.write(f"**{file_id}**:\n")
                f.write(f"- 追加カラム数: {len(diffs['rs_only'])}\n")
                f.write(f"- データソース: 2023年の元データ（`data/download/*.xlsx`）から再抽出\n\n")

        f.write("### フェーズ2: 新規ファイルの作成\n\n")
        f.write(f"新規作成が必要な{len(new_files)}ファイルについて、元データから抽出:\n\n")

        for g in new_files:
            f.write(f"- **{g['ファイル']}**: {g['RS列数']}列のデータを抽出\n")

        f.write("\n### フェーズ3: 単位統一\n\n")
        f.write("予算・執行データの単位を百万円→円に変換:\n")
        f.write("- 2-1_予算・執行_サマリ.csv: 金額カラムを1,000,000倍\n")
        f.write("- 5-1_支出先_支出情報.csv: 金額カラムを1,000,000倍\n\n")

        f.write("### 実装の優先順位\n\n")
        f.write("1. **高**: フェーズ1（既存ファイル拡張） - 基本的な互換性確保\n")
        f.write("2. **中**: フェーズ3（単位統一） - RSシステムとの比較可能性\n")
        f.write("3. **低**: フェーズ2（新規ファイル作成） - 完全互換性\n\n")

    print(f"\n✓ レポート生成完了: {report_path}")
    return report_path


def main():
    """メイン処理"""
    # ファイル構造分析
    gap_summary, categories = analyze_file_structure()

    # カラム詳細分析
    column_diffs = analyze_column_details(categories)

    # レポート生成
    report_path = generate_report(gap_summary, column_diffs)

    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    print(f"\n詳細レポート: {report_path}")

    # サマリ表示
    new_files = [g for g in gap_summary if g["状態"] == "新規作成必要"]
    existing_files = [g for g in gap_summary if "既存" in g["状態"]]
    needs_update = [g for g in existing_files if g["差分"] > 0]

    print(f"\n新規作成必要: {len(new_files)}ファイル")
    print(f"既存（更新必要）: {len(needs_update)}ファイル")
    print(f"既存（そのまま）: {len(existing_files) - len(needs_update)}ファイル")


if __name__ == '__main__':
    main()
