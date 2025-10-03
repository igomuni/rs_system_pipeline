"""
RSシステム形式テーブル構築

正規化されたCSVからRSシステム互換のテーブルを構築
"""
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

from config import (
    MINISTRY_NAME_MAPPING,
    MINISTRY_MASTER,
    RS_STANDARD_COLUMNS,
)

logger = logging.getLogger(__name__)


class TableBuilder:
    """RSシステム形式テーブル構築クラス"""

    # クラス変数：全年度共通の通し番号
    global_business_id_counter = 1

    def __init__(self, year: int):
        self.year = year

    def identify_sheet_type(self, df: pd.DataFrame, filename: str) -> str:
        """
        シートタイプを判定

        Args:
            df: DataFrame
            filename: ファイル名

        Returns:
            シートタイプ（'review', 'segment', 'unknown'）
        """
        # ファイル名からの判定
        if 'レビューシート' in filename or 'review' in filename.lower():
            return 'review'
        if 'セグメント' in filename or 'segment' in filename.lower():
            return 'segment'

        # カラム名からの判定
        columns = df.columns.tolist()
        column_str = ' '.join([str(c) for c in columns])

        # レビューシート特有のカラム
        review_indicators = ['事業名', '府省', '事業の目的', '予算', '執行']
        if sum(1 for indicator in review_indicators if indicator in column_str) >= 3:
            return 'review'

        # セグメントシート特有のカラム
        segment_indicators = ['セグメント', '達成目標', '測定指標']
        if sum(1 for indicator in segment_indicators if indicator in column_str) >= 2:
            return 'segment'

        return 'unknown'

    def extract_common_columns(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        共通カラムを抽出

        Args:
            df: DataFrame

        Returns:
            共通カラムの辞書
        """
        common_data = {
            'シート種別': 'レビューシート',
            '事業年度': self.year,
            '予算事業ID': None,  # 後で設定
            '事業名': None,
            '府省庁の建制順': None,
            '政策所管府省庁': None,
            '府省庁': None,
            '局・庁': None,
            '部': None,
            '課': None,
            '室': None,
            '班': None,
            '係': None,
        }

        columns = df.columns.tolist()

        # 事業名を探す
        for col in columns:
            if '事業名' in str(col):
                common_data['事業名'] = df[col].iloc[0] if len(df) > 0 else None
                break

        # 府省を探す
        for col in columns:
            col_str = str(col)
            if '府省' in col_str and '建制順' not in col_str:
                ministry_name = df[col].iloc[0] if len(df) > 0 else None
                if ministry_name:
                    # 表記揺れを正規化
                    ministry_name = MINISTRY_NAME_MAPPING.get(ministry_name, ministry_name)
                    common_data['府省庁'] = ministry_name
                    common_data['政策所管府省庁'] = ministry_name

                    # 建制順を設定
                    for ministry in MINISTRY_MASTER:
                        if ministry['name'] == ministry_name:
                            common_data['府省庁の建制順'] = ministry['id']
                            break
                break

        # 組織階層を探す
        org_fields = ['局・庁', '部', '課', '室', '班', '係']
        for org_field in org_fields:
            for col in columns:
                if org_field in str(col):
                    common_data[org_field] = df[col].iloc[0] if len(df) > 0 else None
                    break

        return common_data

    def build_project_overview_table(
        self, df: pd.DataFrame, common_data: Dict, row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        事業概要テーブルを構築

        Args:
            df: 元のDataFrame（全行を含む）
            common_data: 共通カラムデータ（未使用 - 各行から取得）
            row_business_ids: 行インデックス→business_idのマッピング

        Returns:
            事業概要DataFrame
        """
        columns = df.columns.tolist()
        all_overview_records = []

        # 概要フィールドの列を特定（1回だけ実行）
        overview_col_map = {}
        for col in columns:
            col_str = str(col)

            if '事業の目的' in col_str or '目的' == col_str:
                overview_col_map['事業の目的'] = col
            elif '現状' in col_str and '課題' in col_str:
                overview_col_map['現状・課題'] = col
            elif '事業の概要' in col_str or '概要' == col_str:
                overview_col_map['事業の概要'] = col
            elif '事業区分' in col_str:
                overview_col_map['事業区分'] = col
            elif '事業開始年度' in col_str or '開始年度' in col_str:
                overview_col_map['事業開始年度'] = col
            elif '不明' in col_str and '開始' in col_str:
                overview_col_map['開始年度不明'] = col
            elif '終了' in col_str and '年度' in col_str:
                overview_col_map['事業終了年度'] = col
            elif '終了予定なし' in col_str or '継続' in col_str:
                overview_col_map['終了予定なし'] = col

        # 各行を処理（各行=1つの事業）
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 予算事業IDを取得（事前に割り当てられたID）
            overview_data = row_common_data.copy()
            overview_data['予算事業ID'] = row_business_ids[row_idx]

            # 事業概要フィールドを抽出
            if '事業の目的' in overview_col_map:
                overview_data['事業の目的'] = row[overview_col_map['事業の目的']]

            if '現状・課題' in overview_col_map:
                overview_data['現状・課題'] = row[overview_col_map['現状・課題']]

            if '事業の概要' in overview_col_map:
                overview_data['事業の概要'] = row[overview_col_map['事業の概要']]

            if '事業区分' in overview_col_map:
                overview_data['事業区分'] = row[overview_col_map['事業区分']]

            if '事業開始年度' in overview_col_map:
                start_year = row[overview_col_map['事業開始年度']]
                overview_data['事業開始年度'] = self._parse_year(start_year)

            if '開始年度不明' in overview_col_map:
                overview_data['開始年度不明'] = row[overview_col_map['開始年度不明']]

            if '事業終了年度' in overview_col_map:
                end_year = row[overview_col_map['事業終了年度']]
                overview_data['事業終了(予定)年度'] = self._parse_year(end_year)

            if '終了予定なし' in overview_col_map:
                overview_data['終了予定なし'] = row[overview_col_map['終了予定なし']]

            all_overview_records.append(overview_data)

        if all_overview_records:
            return pd.DataFrame(all_overview_records)
        return None

    def build_budget_summary_table(
        self, df: pd.DataFrame, common_data: Dict, row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        予算執行サマリテーブルを構築

        Args:
            df: 元のDataFrame（全行を含む）
            common_data: 共通カラムデータ（未使用 - 各行から取得）
            row_business_ids: 行インデックス→business_idのマッピング

        Returns:
            予算執行サマリDataFrame
        """
        columns = df.columns.tolist()
        all_budget_records = []

        # 予算年度パターンを探す
        # 4桁西暦、令和/平成+数字、または2桁年度（平成と仮定）
        budget_year_pattern = re.compile(r'(\d{4})年度|令和(\d+)年度|平成(\d+)年度|-(\d{2})年度')

        # 予算関連カラムを特定（列マッピングを1回だけ実行）
        budget_col_map = {}
        for col in columns:
            col_str = str(col)

            # 年度を抽出
            match = budget_year_pattern.search(col_str)
            if match:
                if match.group(1):  # 西暦4桁
                    budget_year = int(match.group(1))
                elif match.group(2):  # 令和
                    budget_year = 2018 + int(match.group(2))
                elif match.group(3):  # 平成
                    budget_year = 1988 + int(match.group(3))
                elif match.group(4):  # 2桁年度（平成と仮定）
                    two_digit = int(match.group(4))
                    budget_year = 1988 + two_digit
                else:
                    continue

                if budget_year not in budget_col_map:
                    budget_col_map[budget_year] = {}

                # 予算項目を識別
                if '当初予算' in col_str or '初予算' in col_str:
                    budget_col_map[budget_year]['当初予算'] = col
                elif '補正予算' in col_str:
                    budget_col_map[budget_year]['補正予算'] = col
                elif '繰越' in col_str:
                    budget_col_map[budget_year]['繰越'] = col
                elif '執行額' in col_str or '支出額' in col_str:
                    budget_col_map[budget_year]['執行額'] = col
                elif '執行率' in col_str:
                    budget_col_map[budget_year]['執行率'] = col
                elif '予算' in col_str and '合計' in col_str:
                    budget_col_map[budget_year]['予算合計'] = col

        # 各行を処理（各行=1つの事業）
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 予算事業IDを取得（事前に割り当てられたID）
            business_id = row_business_ids[row_idx]

            # 年度ごとのレコードを作成
            for budget_year, cols in budget_col_map.items():
                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['予算年度'] = budget_year

                # 予算データを抽出
                for key, col in cols.items():
                    if key == '当初予算':
                        record['当初予算(合計)'] = self._parse_number(row[col])
                    elif key == '補正予算':
                        record['補正予算(合計)'] = self._parse_number(row[col])
                    elif key == '繰越':
                        record['前年度からの繰越し(合計)'] = self._parse_number(row[col])
                    elif key == '執行額':
                        record['執行額(合計)'] = self._parse_number(row[col])
                    elif key == '執行率':
                        record['執行率'] = self._parse_number(row[col])
                    elif key == '予算合計':
                        record['計(歳出予算現額合計)'] = self._parse_number(row[col])

                all_budget_records.append(record)

        if all_budget_records:
            return pd.DataFrame(all_budget_records)
        return None

    def build_expenditure_table(
        self, df: pd.DataFrame, common_data: Dict, row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        支出先テーブルを構築

        2つのパターンをサポート:
        - 2014年: 支出先上位１０者リスト-グループ-{field}-{num}
        - 2015年以降: 支出先上位１０者リスト-{Block}.支払先-{num}-{field}

        Args:
            df: 元のDataFrame（全行を含む）
            common_data: 共通カラムデータ（未使用 - 各行から取得）
            row_business_ids: 行インデックス→business_idのマッピング

        Returns:
            支出先DataFrame
        """
        columns = df.columns.tolist()
        all_expenditure_records = []

        # 2015+年パターン: "支出先上位１０者リスト-{Block}.支払先-{Num}-{Field}"
        pattern_2015_on = re.compile(r'支出先上位.*?-([A-Z])\.支払先-(\d+)-')

        # 2014年パターン: "支出先上位１０者リスト-グループ-{Field}-{Num}"
        pattern_2014 = re.compile(r'支出先上位.*?-グループ-')

        # 支出先エントリごとにカラムをグループ化（各行で再利用）
        expenditure_col_groups = {}

        # データ形式を検出 (2014形式か2015+形式か)
        is_2014_format = False
        for col in columns:
            if '支出先上位' in str(col) and 'グループ' in str(col):
                is_2014_format = True
                break

        # 列をグループ化（1回だけ実行）
        for col in columns:
            col_str = str(col)

            # 支出先上位セクションのカラムかチェック
            if '支出先上位' not in col_str:
                continue

            if is_2014_format:
                # 2014形式: グループ-{Field}-{Num}
                if 'グループ' not in col_str:
                    continue

                # 番号を抽出 (最後の-{num}部分)
                num_match = re.search(r'-(\d+)$', col_str)
                if not num_match:
                    continue

                entry_num = num_match.group(1)
                block = "GROUP"  # 2014年はグループなのでGROUPとする
                key = f"{block}-{entry_num}"

                if key not in expenditure_col_groups:
                    expenditure_col_groups[key] = {}

                # フィールドタイプを特定 (2014年はシンプルな構造)
                if '-番号-' in col_str:
                    expenditure_col_groups[key]['番号'] = col
                elif '-支出先-' in col_str:
                    expenditure_col_groups[key]['支出先名'] = col
                elif '-業務概要-' in col_str:
                    expenditure_col_groups[key]['業務概要'] = col
                elif '-支出額' in col_str or '-支出額（百万円）-' in col_str:
                    expenditure_col_groups[key]['支出額'] = col
                elif '-入札者数-' in col_str:
                    expenditure_col_groups[key]['入札者数'] = col
                elif '-落札率-' in col_str:
                    expenditure_col_groups[key]['落札率'] = col
            else:
                # 2015+形式: {Block}.支払先-{Num}-{Field}
                match = pattern_2015_on.search(col_str)
                if not match:
                    continue

                block = match.group(1)  # A, B, C, etc.
                entry_num = match.group(2)  # 1, 2, 3, etc.

                # ブロック+番号の組み合わせをキーとする
                key = f"{block}-{entry_num}"

                if key not in expenditure_col_groups:
                    expenditure_col_groups[key] = {}

                # フィールドタイプを特定
                if '-支出先' in col_str:
                    expenditure_col_groups[key]['支出先名'] = col
                elif '-法人番号' in col_str:
                    expenditure_col_groups[key]['法人番号'] = col
                elif '-業務概要' in col_str:
                    expenditure_col_groups[key]['業務概要'] = col
                elif '-支出額（百万円）' in col_str or '-支出額(百万円)' in col_str:
                    expenditure_col_groups[key]['支出額'] = col
                elif '-契約方式' in col_str:
                    expenditure_col_groups[key]['契約方式等'] = col
                elif '-入札者数' in col_str or '-入札者数（応募者数）' in col_str:
                    expenditure_col_groups[key]['入札者数'] = col
                elif '-落札率' in col_str:
                    expenditure_col_groups[key]['落札率'] = col
                elif '-一者応札' in col_str:
                    expenditure_col_groups[key]['一者応札理由'] = col

        # 各行を処理（各行=1つの事業）
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 予算事業IDを取得（事前に割り当てられたID）
            business_id = row_business_ids[row_idx]

            # グループごとにレコードを作成
            for key, fields in sorted(expenditure_col_groups.items()):
                # 支出先名を取得
                if '支出先名' not in fields:
                    continue

                expenditure_name = row[fields['支出先名']]

                # N/A または空の場合はスキップ
                if pd.isna(expenditure_name) or str(expenditure_name).strip() in ['N/A', '-', '']:
                    continue

                block, entry_num = key.split('-')

                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['支出先ブロック'] = block
                record['支出先番号'] = int(entry_num)
                record['支出先名'] = expenditure_name

                # その他のフィールドを設定
                if '法人番号' in fields:
                    record['法人番号'] = row[fields['法人番号']]

                if '業務概要' in fields:
                    record['業務概要'] = row[fields['業務概要']]

                if '支出額' in fields:
                    record['支出額（百万円）'] = self._parse_number(row[fields['支出額']])

                if '契約方式等' in fields:
                    record['契約方式等'] = row[fields['契約方式等']]

                if '入札者数' in fields:
                    record['入札者数（応募者数）'] = self._parse_number(row[fields['入札者数']])

                if '落札率' in fields:
                    record['落札率'] = self._parse_number(row[fields['落札率']])

                if '一者応札理由' in fields:
                    record['一者応札理由'] = row[fields['一者応札理由']]

                all_expenditure_records.append(record)

        if all_expenditure_records:
            return pd.DataFrame(all_expenditure_records)
        return None

    def _parse_year(self, value: any) -> Optional[int]:
        """年度を解析"""
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            # 数字のみ抽出
            match = re.search(r'\d{4}', value)
            if match:
                return int(match.group(0))

        return None

    def _parse_number(self, value: any) -> Optional[float]:
        """数値を解析"""
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            # カンマを削除して数値に変換
            try:
                cleaned = value.replace(',', '').replace('円', '').strip()
                return float(cleaned)
            except (ValueError, AttributeError):
                return None

        return None


def process_year_data(
    year_dir: Path, output_dir: Path
) -> Tuple[int, int, int]:
    """
    年度データを処理してRSシステム形式のテーブルを構築

    Args:
        year_dir: 年度ディレクトリ
        output_dir: 出力ディレクトリ

    Returns:
        (処理ファイル数, 成功数, 失敗数)
    """
    # 年度を抽出
    year_match = re.search(r'year_(\d{4})', year_dir.name)
    if not year_match:
        year_match = re.search(r'(\d{4})', year_dir.name)

    if year_match:
        year = int(year_match.group(1))
    else:
        logger.warning(f"Cannot extract year from {year_dir.name}")
        return 0, 0, 0

    # 各年度の処理開始時にIDカウンターを1にリセット
    TableBuilder.global_business_id_counter = 1

    builder = TableBuilder(year)

    # 出力ディレクトリを作成
    year_output_dir = output_dir / f"year_{year}"
    year_output_dir.mkdir(parents=True, exist_ok=True)

    # 集約用のDataFrame
    all_project_overview = []
    all_budget_summary = []
    all_expenditure = []

    csv_files = list(year_dir.glob("*.csv"))
    total_files = len(csv_files)
    success_count = 0
    failed_count = 0

    logger.info(f"Processing {total_files} files for year {year}")

    for csv_file in csv_files:
        try:
            # CSVを読み込み
            df = pd.read_csv(csv_file, encoding='utf-8-sig')

            if df.empty:
                logger.warning(f"Empty file: {csv_file.name}")
                continue

            # シートタイプを判定
            sheet_type = builder.identify_sheet_type(df, csv_file.name)
            logger.info(f"  {csv_file.name} -> {sheet_type}")

            if sheet_type == 'unknown':
                logger.warning(f"Unknown sheet type: {csv_file.name}")
                continue

            # 共通カラムを抽出
            common_data = builder.extract_common_columns(df)

            # レビューシートの場合
            if sheet_type == 'review':
                # 各行にbusiness_idを割り当て
                row_business_ids = {}
                for row_idx in range(len(df)):
                    row_business_ids[row_idx] = TableBuilder.global_business_id_counter
                    TableBuilder.global_business_id_counter += 1

                # 事業概要テーブル
                overview_df = builder.build_project_overview_table(df, common_data, row_business_ids)
                if overview_df is not None:
                    all_project_overview.append(overview_df)

                # 予算執行サマリテーブル
                budget_df = builder.build_budget_summary_table(df, common_data, row_business_ids)
                if budget_df is not None:
                    all_budget_summary.append(budget_df)

                # 支出先テーブル
                expenditure_df = builder.build_expenditure_table(df, common_data, row_business_ids)
                if expenditure_df is not None:
                    all_expenditure.append(expenditure_df)

            success_count += 1

        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}", exc_info=True)
            failed_count += 1

    # 集約したテーブルを保存
    if all_project_overview:
        final_overview = pd.concat(all_project_overview, ignore_index=True)
        output_file = year_output_dir / f"1-2_{year}_基本情報_事業概要.csv"
        final_overview.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_overview)} records)")

    if all_budget_summary:
        final_budget = pd.concat(all_budget_summary, ignore_index=True)
        output_file = year_output_dir / f"2-1_{year}_予算・執行_サマリ.csv"
        final_budget.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_budget)} records)")

    if all_expenditure:
        final_expenditure = pd.concat(all_expenditure, ignore_index=True)
        output_file = year_output_dir / f"5-1_{year}_支出先_支出情報.csv"
        final_expenditure.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_expenditure)} records)")

    return total_files, success_count, failed_count
