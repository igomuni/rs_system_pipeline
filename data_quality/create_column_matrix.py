#!/usr/bin/env python3
"""
RSシステムと過去データの列名マトリクス比較レポート生成

各テーブルファイルごとに、RS2024の列名を縦軸、各年度(2014-2024)を横軸とした
列存在マトリクスを作成
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Set
import sys
import unicodedata

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def normalize_column_name(col: str) -> str:
    """列名を正規化（全角/半角括弧、スペース等を統一）"""
    # 全角括弧を半角に統一
    col = col.replace('（', '(').replace('）', ')')
    # 全角スペースを削除
    col = col.replace('　', '')
    # 半角スペースを削除
    col = col.replace(' ', '')
    return col


# 列名の意味的マッピング（RS2024列名 → 過去データの列名パターン）
COLUMN_MAPPINGS = {
    # 1-2 基本情報_事業概要
    "事業終了（予定）年度": ["事業終了(予定)年度"],
    "実施方法ー直接実施": ["実施方法"],
    "実施方法ー補助": ["実施方法"],
    "実施方法ー負担": ["実施方法"],
    "実施方法ー交付": ["実施方法"],
    "実施方法ー分担金・拠出金": ["実施方法"],
    "実施方法ーその他": ["実施方法"],

    # 2-1 予算・執行_サマリ (高優先度: 括弧の違いのみ)
    "当初予算（合計）": ["当初予算(合計)"],
    "補正予算（合計）": ["補正予算(合計)"],
    "前年度からの繰越し（合計）": ["前年度からの繰越し(合計)"],
    "予備費等（合計）": ["予備費等(合計)"],
    "執行額（合計）": ["執行額(合計)"],
    "翌年度への繰越し(合計）": ["翌年度へ繰越し(合計)"],
    "当初予算": ["当初予算(合計)"],
    "執行額": ["執行額(合計)"],
    "前年度から繰越し": ["前年度からの繰越し(合計)"],
    "予備費等1": ["予備費等(合計)"],
    "予備費等2": ["予備費等(合計)"],
    "予備費等3": ["予備費等(合計)"],
    "予備費等4": ["予備費等(合計)"],

    # 4-1 点検・評価
    "行政事業レビュー推進チームの所見": ["行政事業レビュー推進チームの所見ー判定"],
    "行政事業レビュー推進チームの所見の詳細": ["行政事業レビュー推進チームの所見ー所見"],
    "過去に受けた指摘事項－区分": ["過去に受けた指摘事項(指摘主体)"],
    "過去に受けた指摘事項－取りまとめ年度": ["過去に受けた指摘事項(年度)"],
    "過去に受けた指摘事項－取りまとめ内容": ["過去に受けた指摘事項(指摘事項)"],
    "過去に受けた指摘事項－対応状況": ["過去に受けた指摘事項(対応状況)"],

    # 5-1 支出先_支出情報
    "支出先ブロック番号": ["支出先ブロック", "支払先ブロック"],
    "支出先ブロック名": ["支出先ブロック"],
    "入札者数": ["入札者数(応募者数)"],
    "金額": ["支出額(百万円)", "金額(百万円)"],
    "契約概要": ["業務概要"],
    "支出先の合計支出額": ["支出額(百万円)"],

    # 5-3 費目・使途
    # 注: "金額"は5-1と共通なので上記に統合済み

    # 2-2 予算・執行_予算種別
    "備考（歳出予算項目ごと）": ["備考1", "備考2", "備考3"],
}


def is_column_match(rs_col: str, historical_col: str) -> bool:
    """2つの列名が一致するか判定（正規化 + マッピング考慮）"""
    rs_normalized = normalize_column_name(rs_col)
    hist_normalized = normalize_column_name(historical_col)

    # 完全一致
    if rs_normalized == hist_normalized:
        return True

    # マッピングルールチェック
    if rs_col in COLUMN_MAPPINGS:
        for alternative in COLUMN_MAPPINGS[rs_col]:
            if normalize_column_name(alternative) == hist_normalized:
                return True

    # 逆方向のマッピングもチェック
    for mapped_col, alternatives in COLUMN_MAPPINGS.items():
        if normalize_column_name(mapped_col) == hist_normalized:
            for alt in alternatives:
                if normalize_column_name(alt) == rs_normalized:
                    return True

    return False


def get_rs2024_columns() -> Dict[str, List[str]]:
    """RS2024の実ファイルから列名を取得"""
    rs2024_dir = Path("/tmp/rs2024_extracted")
    table_columns = {}

    for csv_file in sorted(rs2024_dir.glob("*.csv")):
        try:
            # ファイル名から表識別子を抽出（例: 1-2_RS_2024_...csv → 1-2）
            filename = csv_file.name
            table_id = filename.split("_")[0]  # "1-2", "2-1", etc.

            # CSVを読み込んで列名取得
            df = pd.read_csv(csv_file, nrows=0, encoding='utf-8-sig')
            columns = list(df.columns)

            # テーブル名をキーとして保存
            table_name = f"{table_id}"
            table_columns[table_name] = {
                "filename": filename,
                "columns": columns
            }

        except Exception as e:
            print(f"⚠️  {csv_file.name} 読み込みエラー: {e}")

    return table_columns


def get_historical_columns(year: int, table_id: str) -> List[str]:
    """指定年度・テーブルの列名を取得"""
    output_dir = project_root / "output" / "processed" / f"year_{year}"

    if not output_dir.exists():
        return []

    # ファイル検索（例: 1-2_2023_基本情報_事業概要.csv）
    files = list(output_dir.glob(f"{table_id}_{year}_*.csv"))

    if not files:
        return []

    try:
        df = pd.read_csv(files[0], nrows=0, encoding='utf-8-sig')
        return list(df.columns)
    except Exception:
        return []


def create_matrix_report(rs2024_data: Dict[str, Dict], years: List[int]) -> str:
    """マトリクスレポート生成"""
    lines = []
    lines.append("# RSシステム vs 過去データ 列名対応マトリクス")
    lines.append("")
    lines.append(f"生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## 凡例")
    lines.append("")
    lines.append("- ✓: 列が存在")
    lines.append("- -: 列が存在しない")
    lines.append("")

    # 各テーブルごとにマトリクス生成
    for table_id in sorted(rs2024_data.keys()):
        table_info = rs2024_data[table_id]
        rs2024_columns = table_info["columns"]
        filename = table_info["filename"]

        lines.append("---")
        lines.append("")
        lines.append(f"## {table_id}: {filename}")
        lines.append("")
        lines.append(f"**RS2024列数**: {len(rs2024_columns)}列")
        lines.append("")

        # 各年度の列名を取得
        year_column_data = {}
        for year in years:
            cols = get_historical_columns(year, table_id)
            year_column_data[year] = cols

        # マトリクステーブルヘッダー
        header = "| 列名 | " + " | ".join([str(y) for y in years]) + " |"
        separator = "|------|" + "|".join(["---" for _ in years]) + "|"
        lines.append(header)
        lines.append(separator)

        # 各列について年度ごとの存在チェック（正規化 + マッピング考慮）
        for col in rs2024_columns:
            row_data = [col[:50]]  # 列名を50文字に制限

            for year in years:
                # いずれかの過去データ列とマッチするか
                matched = any(is_column_match(col, hist_col) for hist_col in year_column_data[year])
                row_data.append("✓" if matched else "-")

            lines.append("| " + " | ".join(row_data) + " |")

        lines.append("")

        # 年度別サマリ
        lines.append("### 年度別対応状況")
        lines.append("")
        lines.append("| 年度 | 対応列数 | 対応率 |")
        lines.append("|------|---------|--------|")

        for year in years:
            match_count = sum(
                1 for col in rs2024_columns
                if any(is_column_match(col, hist_col) for hist_col in year_column_data[year])
            )
            match_rate = (match_count / len(rs2024_columns) * 100) if rs2024_columns else 0
            lines.append(f"| {year} | {match_count}/{len(rs2024_columns)} | {match_rate:.1f}% |")

        lines.append("")

    return "\n".join(lines)


def main():
    """メイン処理"""
    print("=" * 80)
    print("RSシステム vs 過去データ 列名マトリクス比較")
    print("=" * 80)
    print()

    # RS2024列定義取得
    print("📋 RS2024ファイルから列名を取得中...")
    rs2024_data = get_rs2024_columns()
    print(f"  ✓ {len(rs2024_data)}テーブル検出")
    print()

    # 対象年度（2024は除外 - RS2024の列定義元なので比較不要）
    years = list(range(2014, 2024))  # 2014-2023

    # レポート生成
    print("📊 マトリクスレポート生成中...")
    report = create_matrix_report(rs2024_data, years)

    # 出力
    output_file = project_root / "data_quality" / "reports" / "column_matrix_report.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)

    print("=" * 80)
    print(f"✅ レポート生成完了: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
