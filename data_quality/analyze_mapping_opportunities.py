#!/usr/bin/env python3
"""
列名変換マップで改善可能な箇所を分析

マトリクスレポートから対応率が低いテーブルを抽出し、
RS2024と過去データの列名差分を分析して、マッピングで
埋められそうな箇所を洗い出す
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_column_diff(rs2024_file: Path, historical_file: Path) -> Tuple[List[str], List[str], List[str]]:
    """
    RS2024と過去データの列名差分を取得

    Returns:
        (RS2024のみにある列, 過去データのみにある列, 共通列)
    """
    rs_df = pd.read_csv(rs2024_file, nrows=0, encoding='utf-8-sig')
    hist_df = pd.read_csv(historical_file, nrows=0, encoding='utf-8-sig')

    rs_cols = set(rs_df.columns)
    hist_cols = set(hist_df.columns)

    only_rs = sorted(rs_cols - hist_cols)
    only_hist = sorted(hist_cols - rs_cols)
    common = sorted(rs_cols & hist_cols)

    return only_rs, only_hist, common


def normalize_for_similarity(text: str) -> str:
    """類似性判定用の正規化"""
    # 括弧内を削除
    import re
    text = re.sub(r'[（(].*?[）)]', '', text)
    # 記号・スペースを削除
    text = text.replace('（', '').replace('）', '').replace('(', '').replace(')', '')
    text = text.replace('　', '').replace(' ', '')
    text = text.replace('・', '').replace('-', '').replace('ー', '')
    text = text.replace('、', '').replace('。', '')
    return text


def find_similar_columns(rs_col: str, hist_cols: List[str]) -> List[Tuple[str, float]]:
    """
    RS2024の列と類似する過去データの列を検索

    Returns:
        (過去データ列名, 類似度スコア)のリスト
    """
    rs_normalized = normalize_for_similarity(rs_col)
    candidates = []

    for hist_col in hist_cols:
        hist_normalized = normalize_for_similarity(hist_col)

        # 完全一致
        if rs_normalized == hist_normalized:
            candidates.append((hist_col, 1.0))
            continue

        # 部分一致（包含関係）
        if rs_normalized in hist_normalized or hist_normalized in rs_normalized:
            similarity = min(len(rs_normalized), len(hist_normalized)) / max(len(rs_normalized), len(hist_normalized))
            candidates.append((hist_col, similarity))
            continue

        # 共通部分文字列の割合
        common_chars = set(rs_normalized) & set(hist_normalized)
        if common_chars:
            similarity = len(common_chars) / max(len(set(rs_normalized)), len(set(hist_normalized)))
            if similarity > 0.5:  # 50%以上共通
                candidates.append((hist_col, similarity))

    # スコア順にソート
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:3]  # 上位3件


def analyze_table(table_id: str, year: int = 2023) -> Dict:
    """テーブルの列名差分を分析"""
    rs2024_dir = Path("/tmp/rs2024_extracted")
    output_dir = project_root / "output" / "processed" / f"year_{year}"

    # RS2024ファイル
    rs_files = list(rs2024_dir.glob(f"{table_id}_*.csv"))
    if not rs_files:
        return {"error": "RS2024ファイルが見つかりません"}

    # 過去データファイル
    hist_files = list(output_dir.glob(f"{table_id}_{year}_*.csv"))
    if not hist_files:
        return {"error": f"{year}年のファイルが見つかりません"}

    # 列名差分取得
    only_rs, only_hist, common = get_column_diff(rs_files[0], hist_files[0])

    # マッピング候補を検索
    mapping_suggestions = {}
    for rs_col in only_rs:
        similar = find_similar_columns(rs_col, only_hist)
        if similar:
            mapping_suggestions[rs_col] = similar

    return {
        "rs2024_file": rs_files[0].name,
        "historical_file": hist_files[0].name,
        "total_rs_cols": len(only_rs) + len(common),
        "total_hist_cols": len(only_hist) + len(common),
        "common_cols": len(common),
        "only_rs": only_rs,
        "only_hist": only_hist,
        "mapping_suggestions": mapping_suggestions,
        "coverage_rate": len(common) / (len(only_rs) + len(common)) * 100 if (len(only_rs) + len(common)) > 0 else 0
    }


def generate_report():
    """改善機会レポート生成"""
    # 対応率が低い〜中程度のテーブルを優先的に分析
    priority_tables = [
        ("1-2", "基本情報_事業概要", 65.6),
        ("2-1", "予算・執行_サマリ", 45.5),
        ("5-1", "支出先_支出情報", 68.8),
        ("4-1", "点検・評価", 73.0),
        ("5-3", "支出先_費目・使途", 85.0),
        ("2-2", "予算・執行_予算種別", 61.5),
        ("5-4", "支出先_国庫債務負担行為", 48.1),
    ]

    lines = []
    lines.append("# 列名変換マップによる改善機会分析")
    lines.append("")
    lines.append(f"分析日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## 概要")
    lines.append("")
    lines.append("対応率が低い〜中程度のテーブルについて、RS2024と2023年データの列名差分を分析し、")
    lines.append("列名変換マップで埋められそうな箇所を抽出しました。")
    lines.append("")

    all_suggestions = {}

    for table_id, table_name, current_rate in priority_tables:
        print(f"📊 分析中: {table_id} - {table_name} (現在{current_rate}%)")

        result = analyze_table(table_id, year=2023)

        if "error" in result:
            print(f"  ⚠️  {result['error']}")
            continue

        lines.append("---")
        lines.append("")
        lines.append(f"## {table_id}: {table_name}")
        lines.append("")
        lines.append(f"**現在の対応率**: {current_rate}%")
        lines.append(f"**RS2024列数**: {result['total_rs_cols']}列")
        lines.append(f"**2023年列数**: {result['total_hist_cols']}列")
        lines.append(f"**共通列数**: {result['common_cols']}列")
        lines.append("")

        # RS2024のみにある列
        lines.append("### RS2024にのみ存在する列（欠けている列）")
        lines.append("")
        lines.append(f"**件数**: {len(result['only_rs'])}列")
        lines.append("")

        if result['mapping_suggestions']:
            lines.append("#### マッピング候補あり ✨")
            lines.append("")
            lines.append("| RS2024列名 | 類似する2023年列名 | 類似度 | 推奨度 |")
            lines.append("|------------|-------------------|--------|--------|")

            for rs_col, candidates in result['mapping_suggestions'].items():
                for hist_col, similarity in candidates:
                    priority = "高" if similarity >= 0.8 else "中" if similarity >= 0.6 else "低"
                    lines.append(f"| {rs_col[:40]} | {hist_col[:40]} | {similarity:.1%} | {priority} |")

                    # 全体の推奨マッピングリストに追加
                    if table_id not in all_suggestions:
                        all_suggestions[table_id] = []
                    all_suggestions[table_id].append({
                        "rs_col": rs_col,
                        "hist_col": hist_col,
                        "similarity": similarity,
                        "priority": priority
                    })

            lines.append("")

        # マッピング候補のない列
        no_mapping = [col for col in result['only_rs'] if col not in result['mapping_suggestions']]
        if no_mapping:
            lines.append("#### マッピング候補なし（新規実装が必要）")
            lines.append("")
            for col in no_mapping[:10]:  # 最初の10件のみ
                lines.append(f"- {col}")
            if len(no_mapping) > 10:
                lines.append(f"- ...他{len(no_mapping) - 10}件")
            lines.append("")

        # 2023年のみにある列（RS2024で削除された列）
        if result['only_hist']:
            lines.append("### 2023年にのみ存在する列（RS2024で削除）")
            lines.append("")
            lines.append(f"**件数**: {len(result['only_hist'])}列")
            lines.append("")
            for col in result['only_hist'][:5]:
                lines.append(f"- {col}")
            if len(result['only_hist']) > 5:
                lines.append(f"- ...他{len(result['only_hist']) - 5}件")
            lines.append("")

        print(f"  ✓ マッピング候補: {len(result['mapping_suggestions'])}件")

    # サマリ
    lines.append("---")
    lines.append("")
    lines.append("## 推奨マッピング一覧（優先度順）")
    lines.append("")

    high_priority = []
    medium_priority = []

    for table_id, suggestions in all_suggestions.items():
        for sug in suggestions:
            if sug['priority'] == '高':
                high_priority.append((table_id, sug))
            elif sug['priority'] == '中':
                medium_priority.append((table_id, sug))

    if high_priority:
        lines.append("### 高優先度（類似度80%以上）")
        lines.append("")
        lines.append("| テーブル | RS2024列名 | → | 2023年列名 | 類似度 |")
        lines.append("|---------|-----------|---|-----------|--------|")
        for table_id, sug in high_priority:
            lines.append(f"| {table_id} | {sug['rs_col'][:30]} | → | {sug['hist_col'][:30]} | {sug['similarity']:.1%} |")
        lines.append("")

    if medium_priority:
        lines.append("### 中優先度（類似度60-80%）")
        lines.append("")
        lines.append("| テーブル | RS2024列名 | → | 2023年列名 | 類似度 |")
        lines.append("|---------|-----------|---|-----------|--------|")
        for table_id, sug in medium_priority[:20]:  # 最初の20件
            lines.append(f"| {table_id} | {sug['rs_col'][:30]} | → | {sug['hist_col'][:30]} | {sug['similarity']:.1%} |")
        lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("列名変換マップによる改善機会分析")
    print("=" * 80)
    print()

    report = generate_report()

    output_file = project_root / "data_quality" / "mapping_opportunities.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)

    print()
    print("=" * 80)
    print(f"✅ レポート生成完了: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
