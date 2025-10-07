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
            elif '事業の概要' in col_str or '事業概要' == col_str:
                overview_col_map['事業の概要'] = col
            elif '事業概要URL' in col_str:
                overview_col_map['事業概要URL'] = col
            elif '事業区分' in col_str:
                overview_col_map['事業区分'] = col
            elif '主要経費' in col_str:
                overview_col_map['主要経費'] = col
            elif '実施方法' == col_str:
                overview_col_map['実施方法'] = col
            elif '補助率等' in col_str or '補助率' in col_str:
                overview_col_map['補助率等'] = col
            elif col_str == '事業番号-1':
                overview_col_map['事業番号-1'] = col
            elif col_str == '事業番号-2':
                overview_col_map['事業番号-2'] = col
            elif col_str == '事業番号-3':
                overview_col_map['事業番号-3'] = col
            elif col_str == '事業番号-4':
                overview_col_map['事業番号-4'] = col
            elif col_str == '事業番号-5':
                overview_col_map['事業番号-5'] = col
            elif '事業開始年度' in col_str or '開始年度' in col_str:
                overview_col_map['事業開始年度'] = col
            elif '不明' in col_str and '開始' in col_str:
                overview_col_map['開始年度不明'] = col
            elif '終了' in col_str and '年度' in col_str and '予定' in col_str:
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

            if '事業概要URL' in overview_col_map:
                overview_data['事業概要URL'] = row[overview_col_map['事業概要URL']]

            if '事業区分' in overview_col_map:
                overview_data['事業区分'] = row[overview_col_map['事業区分']]

            if '主要経費' in overview_col_map:
                overview_data['主要経費'] = row[overview_col_map['主要経費']]

            if '実施方法' in overview_col_map:
                overview_data['実施方法'] = row[overview_col_map['実施方法']]

            if '補助率等' in overview_col_map:
                overview_data['補助率等'] = row[overview_col_map['補助率等']]

            # 旧事業番号を作成（事業番号-1～5を結合）
            old_project_numbers = []
            for i in range(1, 6):
                key = f'事業番号-{i}'
                if key in overview_col_map:
                    value = row[overview_col_map[key]]
                    if pd.notna(value) and str(value).strip():
                        old_project_numbers.append(str(value).strip())

            if old_project_numbers:
                overview_data['旧事業番号'] = '-'.join(old_project_numbers)

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
        # 4桁西暦、令和/平成+数字、または1-2桁年度（-NN年度-形式）
        budget_year_pattern = re.compile(r'(\d{4})年度|令和(\d+)年度|令和元年度|平成(\d+)年度|-(\d{1,2})年度-')

        # 事前に全カラムをスキャンして令和時代かどうか判定
        all_columns_str = ''.join(str(col) for col in columns)
        is_reiwa_era = '令和元年度' in all_columns_str or '令和' in all_columns_str

        # 予算関連カラムを特定（列マッピングを1回だけ実行）
        budget_col_map = {}
        for col in columns:
            col_str = str(col)

            # 年度を抽出
            match = budget_year_pattern.search(col_str)
            if match:
                if match.group(1):  # 西暦4桁
                    budget_year = int(match.group(1))
                elif match.group(2):  # 令和N年度
                    budget_year = 2018 + int(match.group(2))
                elif '令和元年度' in col_str:  # 令和元年度
                    budget_year = 2019
                elif match.group(3):  # 平成N年度
                    budget_year = 1988 + int(match.group(3))
                elif match.group(4):  # -NN年度-形式（1-2桁）
                    year_num = int(match.group(4))
                    # 年度番号から推測：
                    # - 令和は2019年開始（元年=1年目）なので、小さい数字（1-10程度）
                    # - 平成は1989年開始なので、大きい数字（20-31）
                    # ヒューリスティック：年度番号が20以上なら平成、それ以外は文書全体を見て判断
                    if year_num >= 20:
                        # 平成時代（20年度以上は令和ではありえない）
                        budget_year = 1988 + year_num
                    elif is_reiwa_era:
                        # 令和時代のファイルで1-19の小さい数字
                        if year_num == 1:
                            budget_year = 2019  # 令和元年
                        else:
                            budget_year = 2018 + year_num
                    else:
                        # 平成時代
                        budget_year = 1988 + year_num
                else:
                    continue

                if budget_year not in budget_col_map:
                    budget_col_map[budget_year] = {}

                # 予算項目を識別
                if '当初予算' in col_str and '補正' not in col_str:
                    budget_col_map[budget_year]['当初予算'] = col
                elif '補正予算' in col_str and '次' not in col_str:
                    budget_col_map[budget_year]['補正予算'] = col
                elif '前年度から繰越し' in col_str or ('前年度' in col_str and '繰越' in col_str):
                    budget_col_map[budget_year]['前年度から繰越し'] = col
                elif '翌年度へ繰越し' in col_str or ('翌年度' in col_str and '繰越' in col_str):
                    budget_col_map[budget_year]['翌年度へ繰越し'] = col
                elif '予備費等' in col_str or '予備費' in col_str:
                    budget_col_map[budget_year]['予備費等'] = col
                elif '執行額' in col_str and '割合' not in col_str:
                    budget_col_map[budget_year]['執行額'] = col
                elif '執行率' in col_str or ('執行' in col_str and '%' in col_str):
                    budget_col_map[budget_year]['執行率'] = col
                elif '計' == col_str or ('予算' in col_str and '計' in col_str and '内訳' not in col_str):
                    budget_col_map[budget_year]['計'] = col

        # 会計区分カラムを探す（全行共通）
        account_type_col = None
        for col in columns:
            if '会計区分' in str(col):
                account_type_col = col
                break

        # 各行を処理（各行=1つの事業）
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 予算事業IDを取得（事前に割り当てられたID）
            business_id = row_business_ids[row_idx]

            # 会計区分を取得
            account_type = None
            if account_type_col is not None:
                account_type = row[account_type_col]

            # 年度ごとのレコードを作成
            for budget_year, cols in budget_col_map.items():
                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['予算年度'] = budget_year

                # 会計区分を設定
                if account_type:
                    record['会計区分'] = account_type

                # 予算データを抽出
                has_data = False  # 実際にデータがあるかチェック
                for key, col in cols.items():
                    if key == '当初予算':
                        value = self._parse_number(row[col])
                        record['当初予算(合計)'] = value
                        # NaNでなく、かつ0でない場合にデータありと判定
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '補正予算':
                        value = self._parse_number(row[col])
                        record['補正予算(合計)'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '前年度から繰越し':
                        value = self._parse_number(row[col])
                        record['前年度からの繰越し(合計)'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '翌年度へ繰越し':
                        value = self._parse_number(row[col])
                        record['翌年度へ繰越し(合計)'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '予備費等':
                        value = self._parse_number(row[col])
                        record['予備費等(合計)'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '執行額':
                        value = self._parse_number(row[col])
                        record['執行額(合計)'] = value
                        # NaNでなく、かつ0でない場合にデータありと判定
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '執行率':
                        value = self._parse_number(row[col])
                        record['執行率'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True
                    elif key == '計':
                        value = self._parse_number(row[col])
                        record['計(歳出予算現額合計)'] = value
                        if pd.notna(value) and value != 0:
                            has_data = True

                # データが実際に存在する年度のみレコードを追加
                if has_data:
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
                if '-支出先' in col_str and '法人' not in col_str:
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
                elif '-一者応札・一者応募又は競争性のない随意契約となった理由及び改善策' in col_str:
                    expenditure_col_groups[key]['一者応札理由（詳細）'] = col
                elif '-一者応札' in col_str and '理由' in col_str:
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

                if '一者応札理由（詳細）' in fields:
                    record['一者応札・一者応募又は競争性のない随意契約となった理由及び改善策（支出額10億円以上）'] = row[fields['一者応札理由（詳細）']]

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

    def build_organization_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        1-1_組織情報テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            組織情報テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 作成責任者列を検出
        creator_col = None
        for col in columns:
            col_str = str(col)
            if '作成責任者' in col_str:
                creator_col = col
                break

        # 担当部局庁列を検出
        dept_col = None
        for col in columns:
            col_str = str(col)
            if '担当部局庁' in col_str:
                dept_col = col
                break

        # 担当課室列を検出
        section_col = None
        for col in columns:
            col_str = str(col)
            if '担当課室' in col_str:
                section_col = col
                break

        all_org_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])
            record = row_common_data.copy()
            record['予算事業ID'] = business_id

            # 建制順（府省庁の建制順を使用）
            if '府省庁の建制順' in record:
                record['建制順'] = record['府省庁の建制順']

            # 所管府省庁（政策所管府省庁を使用）
            if '政策所管府省庁' in record:
                record['所管府省庁'] = record['政策所管府省庁']

            # その他担当組織_作成責任者_no（常に1、複数ある場合は別ロジックが必要）
            record['その他担当組織_作成責任者_no'] = 1

            # その他担当組織情報（担当部局庁から抽出）
            # 2023データでは「担当部局庁」と「担当課室」が別列
            other_org_ministry = ''
            other_org_bureau = ''
            other_org_dept = ''
            other_org_section = ''

            if dept_col is not None:
                dept_val = row[dept_col]
                if pd.notna(dept_val) and str(dept_val).strip() and str(dept_val).strip() != '-':
                    # 担当部局庁をそのまま使用（局・庁レベルと想定）
                    other_org_bureau = str(dept_val).strip()

            if section_col is not None:
                section_val = row[section_col]
                if pd.notna(section_val) and str(section_val).strip() and str(section_val).strip() != '-':
                    # 担当課室をそのまま使用
                    other_org_section = str(section_val).strip()

            record['府省庁（その他担当組織）'] = other_org_ministry
            record['局・庁（その他担当組織）'] = other_org_bureau
            record['部（その他担当組織）'] = other_org_dept
            record['課（その他担当組織）'] = other_org_section
            record['室（その他担当組織）'] = ''
            record['班（その他担当組織）'] = ''
            record['係（その他担当組織）'] = ''

            # 作成責任者
            creator_value = ''
            if creator_col is not None:
                val = row[creator_col]
                if pd.notna(val) and str(val).strip() and str(val).strip() != '-':
                    creator_value = str(val).strip()

            record['作成責任者'] = creator_value

            all_org_records.append(record)

        if all_org_records:
            # 必要な列のみ選択して順序を整理
            result_df = pd.DataFrame(all_org_records)

            # 列の順序を定義
            column_order = [
                'シート種別',
                '事業年度',
                '予算事業ID',
                '事業名',
                '建制順',
                '所管府省庁',
                '府省庁',
                '局・庁',
                '部',
                '課',
                '室',
                '班',
                '係',
                'その他担当組織_作成責任者_no',
                '府省庁（その他担当組織）',
                '局・庁（その他担当組織）',
                '部（その他担当組織）',
                '課（その他担当組織）',
                '室（その他担当組織）',
                '班（その他担当組織）',
                '係（その他担当組織）',
                '作成責任者'
            ]

            # 存在する列のみ選択
            existing_cols = [col for col in column_order if col in result_df.columns]
            return result_df[existing_cols]

        return None

    def build_policy_law_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        1-3_政策・施策、法令等テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            政策・施策、法令等テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 各列を検出
        policy_col = None
        measure_col = None
        policy_url_col = None
        law_col = None
        plan_col = None

        for col in columns:
            col_str = str(col)
            if col_str == '政策':
                policy_col = col
            elif col_str == '施策':
                measure_col = col
            elif '政策体系' in col_str and 'URL' in col_str:
                policy_url_col = col
            elif '根拠法令' in col_str:
                law_col = col
            elif '関係する計画' in col_str or '通知' in col_str:
                plan_col = col

        all_policy_law_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 政策・施策セクション
            policy_text = row[policy_col] if policy_col is not None else ''
            measure_text = row[measure_col] if measure_col is not None else ''
            policy_url = row[policy_url_col] if policy_url_col is not None else ''

            if pd.notna(policy_text) and str(policy_text).strip() and str(policy_text).strip() != '-':
                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['番号（政策・施策）'] = 1
                record['政策所管府省庁_P'] = row_common_data.get('政策所管府省庁', '')
                record['政策'] = str(policy_text).strip()
                record['施策'] = str(measure_text).strip() if pd.notna(measure_text) and str(measure_text).strip() != '-' else ''
                record['政策・施策URL'] = str(policy_url).strip() if pd.notna(policy_url) and str(policy_url).strip() != '-' else ''
                record['番号（根拠法令）'] = ''
                record['法令名'] = ''
                record['法令番号'] = ''
                record['法令ID'] = ''
                record['条'] = ''
                record['項'] = ''
                record['号・号の細分'] = ''
                record['番号（関係する計画・通知等）'] = ''
                record['計画通知名'] = ''
                record['計画通知等URL'] = ''
                all_policy_law_records.append(record)

            # 根拠法令セクション
            law_text = row[law_col] if law_col is not None else ''
            if pd.notna(law_text) and str(law_text).strip() and str(law_text).strip() != '-':
                law_text = str(law_text).strip()

                # 法令名と条項をパース
                # パターン: "法令名(年月日法律第XX号)第X条第Y項第Z号"
                law_pattern = r'([^(（]+)(?:\(([^)]+)\)|（([^）]+)）)?(?:第([0-9]+)条)?(?:第([0-9]+)項)?(?:第([0-9]+)号)?'
                match = re.search(law_pattern, law_text)

                law_name = ''
                law_number = ''
                article = ''
                paragraph = ''
                item = ''

                if match:
                    law_name = match.group(1).strip()
                    law_number_text = match.group(2) or match.group(3) or ''
                    article = match.group(4) or ''
                    paragraph = match.group(5) or ''
                    item = match.group(6) or ''

                    # 法令番号を抽出（例: "平成二十六年法律第百三十六号"）
                    if law_number_text:
                        law_number = law_number_text.strip()

                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['番号（政策・施策）'] = ''
                record['政策所管府省庁_P'] = ''
                record['政策'] = ''
                record['施策'] = ''
                record['政策・施策URL'] = ''
                record['番号（根拠法令）'] = 1
                record['法令名'] = law_name
                record['法令番号'] = law_number
                record['法令ID'] = ''
                record['条'] = article
                record['項'] = paragraph
                record['号・号の細分'] = item
                record['番号（関係する計画・通知等）'] = ''
                record['計画通知名'] = ''
                record['計画通知等URL'] = ''
                all_policy_law_records.append(record)

            # 関係する計画・通知セクション
            plan_text = row[plan_col] if plan_col is not None else ''
            if pd.notna(plan_text) and str(plan_text).strip() and str(plan_text).strip() != '-':
                plan_text = str(plan_text).strip()

                # URLを抽出
                url_pattern = r'https?://[^\s、。]+'
                urls = re.findall(url_pattern, plan_text)
                plan_url = urls[0] if urls else ''

                # URLを除いた計画名
                plan_name = re.sub(url_pattern, '', plan_text).strip()

                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['番号（政策・施策）'] = ''
                record['政策所管府省庁_P'] = ''
                record['政策'] = ''
                record['施策'] = ''
                record['政策・施策URL'] = ''
                record['番号（根拠法令）'] = ''
                record['法令名'] = ''
                record['法令番号'] = ''
                record['法令ID'] = ''
                record['条'] = ''
                record['項'] = ''
                record['号・号の細分'] = ''
                record['番号（関係する計画・通知等）'] = 1
                record['計画通知名'] = plan_name
                record['計画通知等URL'] = plan_url
                all_policy_law_records.append(record)

        if all_policy_law_records:
            # 必要な列のみ選択して順序を整理
            result_df = pd.DataFrame(all_policy_law_records)

            # 列の順序を定義
            column_order = [
                'シート種別',
                '事業年度',
                '予算事業ID',
                '事業名',
                '府省庁の建制順',
                '政策所管府省庁',
                '府省庁',
                '局・庁',
                '部',
                '課',
                '室',
                '班',
                '係',
                '番号（政策・施策）',
                '政策所管府省庁_P',
                '政策',
                '施策',
                '政策・施策URL',
                '番号（根拠法令）',
                '法令名',
                '法令番号',
                '法令ID',
                '条',
                '項',
                '号・号の細分',
                '番号（関係する計画・通知等）',
                '計画通知名',
                '計画通知等URL'
            ]

            # 存在する列のみ選択
            existing_cols = [col for col in column_order if col in result_df.columns]
            return result_df[existing_cols]

        return None

    def build_inspection_evaluation_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        4-1_点検・評価テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            点検・評価テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 列マッピング: 2023ソース列名 → RS 2024列名
        column_mapping = {
            '事業所管部局による点検・改善-点検結果': '事業所管部局による点検・改善ー点検結果',
            '事業所管部局による点検・改善-改善の方向性': '事業所管部局による点検・改善ー改善の方向性',
            '事業所管部局による点検・改善-目標年度における効果測定に関する評価': '事業所管部局による点検・改善－目標年度における効果測定に関する評価',
            '外部有識者の所見--': '外部有識者による点検ー所見',
            '行政事業レビュー推進チームの所見に至る過程及び所見-判定': '行政事業レビュー推進チームの所見ー判定',
            '行政事業レビュー推進チームの所見に至る過程及び所見-初見': '行政事業レビュー推進チームの所見ー所見',
            '過去に受けた指摘事項と対応状況-公開プロセス・秋の年次公開検証（秋のレビュー）における取りまとめ': '公開プロセス結果概要'
        }

        # 存在する列のみマッピング
        available_mappings = {}
        for src_col, dest_col in column_mapping.items():
            if src_col in columns:
                available_mappings[src_col] = dest_col

        if not available_mappings:
            # マッピング可能な列が1つもない場合は空を返す
            return None

        all_inspection_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            record = row_common_data.copy()
            record['予算事業ID'] = business_id

            # 列マッピングに基づいてデータを転記
            for src_col, dest_col in available_mappings.items():
                value = row[src_col]
                if pd.isna(value):
                    record[dest_col] = ''
                else:
                    record[dest_col] = str(value).strip()

            all_inspection_records.append(record)

        if not all_inspection_records:
            return None

        result_df = pd.DataFrame(all_inspection_records)

        # 列順序を定義（RS System 2024準拠）
        column_order = [
            'シート種別',
            '事業年度',
            '予算事業ID',
            '事業名',
            '府省庁の建制順',
            '政策所管府省庁',
            '府省庁',
            '局・庁',
            '部',
            '課',
            '室',
            '班',
            '係',
            '事業所管部局による点検・改善ー点検結果',
            '事業所管部局による点検・改善ー改善の方向性',
            '事業所管部局による点検・改善－目標年度における効果測定に関する評価',
            '外部有識者による点検ー最終実施年度',
            '外部有識者による点検ー点検対象',
            '外部有識者による点検ー対象の理由',
            '外部有識者による点検ー所見',
            '公開プロセス結果概要',
            '行政事業レビュー推進チームの所見ー判定',
            '行政事業レビュー推進チームの所見ー所見',
            '過去に受けた指摘事項（年度）',
            '過去に受けた指摘事項（指摘主体）',
            '過去に受けた指摘事項（指摘事項）',
            '過去に受けた指摘事項（対応状況）',
            '備考1',
            '備考2',
            '備考3',
            '備考4',
            '備考5',
            '備考6',
            '備考7',
            '備考8',
            '備考9',
            '備考10'
        ]

        # 存在しない列は空文字列で追加
        for col in column_order:
            if col not in result_df.columns:
                result_df[col] = ''

        return result_df[column_order]

    def build_related_projects_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        1-5_関連事業テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            関連事業テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 関連事業番号列を動的検出（正規表現）
        related_project_cols = []
        import re
        pattern = re.compile(r'関連する過去のレビューシートの事業番号-(\d{4})年度-(\d{2})')

        for col in columns:
            col_str = str(col)
            match = pattern.search(col_str)
            if match:
                year = int(match.group(1))
                seq = int(match.group(2))
                related_project_cols.append((col, year, seq))

        if not related_project_cols:
            # 関連事業列が1つもない場合は空を返す
            return None

        all_related_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 関連事業の連番
            related_seq = 1

            # 各関連事業列を処理
            for col, year, _ in related_project_cols:
                value = row[col]
                # 値が存在する場合のみレコードを作成
                if pd.notna(value) and str(value).strip() != '' and str(value).strip() != '-':
                    record = row_common_data.copy()
                    record['予算事業ID'] = business_id
                    record['番号（関連事業）'] = related_seq
                    record['関連事業の事業ID'] = str(value).strip()
                    record['関連事業の事業名'] = ''  # 事業名は2023データに存在しない
                    record['関連性'] = f'{year}年度過去事業'

                    all_related_records.append(record)
                    related_seq += 1

        if not all_related_records:
            return None

        result_df = pd.DataFrame(all_related_records)

        # 列順序を定義（RS System 2024準拠）
        column_order = [
            'シート種別',
            '事業年度',
            '予算事業ID',
            '事業名',
            '府省庁の建制順',
            '政策所管府省庁',
            '府省庁',
            '局・庁',
            '部',
            '課',
            '室',
            '班',
            '係',
            '番号（関連事業）',
            '関連事業の事業ID',
            '関連事業の事業名',
            '関連性'
        ]

        # 存在しない列は空文字列で追加
        for col in column_order:
            if col not in result_df.columns:
                result_df[col] = ''

        return result_df[column_order]

    def build_expense_usage_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        5-3_費目・使途テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            費目・使途テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 費目・使途列を動的検出（正規表現）
        # パターン: 費目・使途（...）-{A,B,C,D}.支払先-{費目,使途,金額（百万円）}-{01-10}
        import re
        expense_cols = []
        pattern = re.compile(r'費目・使途.*-([A-D])\.支払先-(費目|使途|金額.*)-(\d{2})')

        for col in columns:
            col_str = str(col)
            match = pattern.search(col_str)
            if match:
                block = match.group(1)  # A, B, C, D
                field_type = match.group(2)  # 費目, 使途, 金額
                seq = int(match.group(3))  # 01-10
                expense_cols.append((col, block, field_type, seq))

        if not expense_cols:
            # 費目・使途列が1つもない場合は空を返す
            return None

        # ブロック×連番でグループ化
        expense_map = {}
        for col, block, field_type, seq in expense_cols:
            key = (block, seq)
            if key not in expense_map:
                expense_map[key] = {}
            expense_map[key][field_type] = col

        all_expense_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 費目・使途の連番
            expense_seq = 1

            # 各ブロック×連番のデータを処理
            for (block, seq), field_cols in sorted(expense_map.items()):
                # 費目、使途、金額を取得
                expense_item = ''
                usage = ''
                amount = ''

                if '費目' in field_cols:
                    val = row[field_cols['費目']]
                    if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                        expense_item = str(val).strip()

                if '使途' in field_cols:
                    val = row[field_cols['使途']]
                    if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                        usage = str(val).strip()

                if '金額' in field_cols:
                    val = row[field_cols['金額']]
                    if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                        amount = str(val).strip()

                # いずれかのフィールドに値がある場合のみレコードを作成
                if expense_item or usage or amount:
                    record = row_common_data.copy()
                    record['予算事業ID'] = business_id
                    record['番号（費目・使途）'] = expense_seq
                    record['支払先ブロック'] = block
                    record['費目'] = expense_item
                    record['使途'] = usage
                    record['金額（百万円）'] = amount
                    record['備考'] = ''

                    all_expense_records.append(record)
                    expense_seq += 1

        if not all_expense_records:
            return None

        result_df = pd.DataFrame(all_expense_records)

        # 列順序を定義（RS System 2024準拠）
        column_order = [
            'シート種別',
            '事業年度',
            '予算事業ID',
            '事業名',
            '府省庁の建制順',
            '政策所管府省庁',
            '府省庁',
            '局・庁',
            '部',
            '課',
            '室',
            '班',
            '係',
            '番号（費目・使途）',
            '支払先ブロック',
            '費目',
            '使途',
            '金額（百万円）',
            '備考1',
            '備考2'
        ]

        # 存在しない列は空文字列で追加
        for col in column_order:
            if col not in result_df.columns:
                result_df[col] = ''

        # 備考列をマッピング
        if '備考' in result_df.columns:
            result_df['備考1'] = result_df['備考']
            result_df = result_df.drop('備考', axis=1)

        return result_df[column_order]

    def build_budget_category_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        2-2_予算種別・歳出予算項目テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            予算種別・歳出予算項目テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 歳出予算項・目列を動的検出（正規表現）
        # パターン1 (2023年): 2023・2024年度予算内訳（単位：百万円）-歳出予算項・目-{（項）,（目）,令和5年度当初予算,令和6年度要求}-{01-10}
        # パターン2 (2022年): 2022・2023年度予算内訳（単位：百万円）-{歳出予算目,2022年度当初予算,2023年度要求}-{01-10}
        import re
        budget_cols = []
        pattern_2023 = re.compile(r'予算内訳.*歳出予算項・目-(.*)-(\d{2})')
        pattern_2022 = re.compile(r'予算内訳.*-(歳出予算目|20\d{2}年度当初予算|20\d{2}年度要求)-(\d{2})')

        for col in columns:
            col_str = str(col)
            # 2023年形式を試す
            match = pattern_2023.search(col_str)
            if match:
                field_type = match.group(1)  # （項）, （目）, 令和N年度当初予算, etc.
                seq = int(match.group(2))  # 01-10
                budget_cols.append((col, field_type, seq))
                continue

            # 2022年形式を試す
            match = pattern_2022.search(col_str)
            if match:
                field_type = match.group(1)  # 歳出予算目, 2022年度当初予算, etc.
                seq = int(match.group(2))  # 01-10
                # 2022年形式を2023年形式にマッピング
                if '歳出予算目' in field_type:
                    field_type = '（目）'
                elif '当初予算' in field_type:
                    field_type = '当初予算'
                elif '要求' in field_type:
                    field_type = '要求'
                budget_cols.append((col, field_type, seq))

        if not budget_cols:
            # 歳出予算項・目列が1つもない場合は空を返す
            return None

        # 連番でグループ化
        budget_map = {}
        for col, field_type, seq in budget_cols:
            if seq not in budget_map:
                budget_map[seq] = {}
            budget_map[seq][field_type] = col

        # 会計区分列を検出
        account_col = None
        for col in columns:
            if '会計区分' in str(col):
                account_col = col
                break

        all_budget_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 会計区分を取得
            account_category = ''
            if account_col is not None:
                val = row[account_col]
                if pd.notna(val) and str(val).strip() != '':
                    account_category = str(val).strip()

            # 予算内訳の連番
            budget_seq = 1

            # 各連番のデータを処理
            for seq in sorted(budget_map.keys()):
                field_cols = budget_map[seq]

                # 項、目、予算額を取得
                item_kou = ''
                item_moku = ''
                current_budget = ''
                next_budget = ''

                if '（項）' in field_cols:
                    val = row[field_cols['（項）']]
                    if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                        item_kou = str(val).strip()

                if '（目）' in field_cols:
                    val = row[field_cols['（目）']]
                    if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                        item_moku = str(val).strip()

                # 当初予算（令和5年度または2022年度など）
                for key in field_cols.keys():
                    if '当初予算' in key:
                        val = row[field_cols[key]]
                        if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                            current_budget = str(val).strip()
                            break

                # 要求（令和6年度または2023年度など）
                for key in field_cols.keys():
                    if '要求' in key:
                        val = row[field_cols[key]]
                        if pd.notna(val) and str(val).strip() != '' and str(val).strip() != '-':
                            next_budget = str(val).strip()
                            break

                # いずれかのフィールドに値がある場合のみレコードを作成
                if item_kou or item_moku or current_budget or next_budget:
                    record = row_common_data.copy()
                    record['予算事業ID'] = business_id
                    record['番号（予算内訳）'] = budget_seq
                    record['会計区分'] = account_category
                    record['歳出予算項（項）'] = item_kou
                    record['歳出予算項（目）'] = item_moku
                    record['令和5年度当初予算（百万円）'] = current_budget
                    record['令和6年度要求（百万円）'] = next_budget

                    all_budget_records.append(record)
                    budget_seq += 1

        if not all_budget_records:
            return None

        result_df = pd.DataFrame(all_budget_records)

        # 列順序を定義（RS System 2024準拠）
        column_order = [
            'シート種別',
            '事業年度',
            '予算事業ID',
            '事業名',
            '府省庁の建制順',
            '政策所管府省庁',
            '府省庁',
            '局・庁',
            '部',
            '課',
            '室',
            '班',
            '係',
            '番号（予算内訳）',
            '会計区分',
            '会計',
            '勘定',
            '歳出予算項（項）',
            '歳出予算項（目）',
            '令和5年度当初予算（百万円）',
            '令和6年度要求（百万円）',
            '備考1',
            '備考2',
            '備考3',
            '備考4',
            '備考5'
        ]

        # 存在しない列は空文字列で追加
        for col in column_order:
            if col not in result_df.columns:
                result_df[col] = ''

        return result_df[column_order]

    def build_multi_year_contract_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        5-4_国庫債務負担行為等による契約テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            国庫債務負担行為等による契約テーブル（DataFrame）
        """
        import re

        columns = df.columns.tolist()

        # 国庫債務負担行為等による契約列を動的検出
        # パターン: 国庫債務負担行為等による契約先上位10者リスト-{連番}-{フィールド名}
        contract_pattern = re.compile(r'国庫債務負担行為等による契約先上位10者リスト-(\d+)-(.*)')
        contract_data = {}  # {seq: {field: col}}

        for col in columns:
            match = contract_pattern.search(str(col))
            if match:
                seq = int(match.group(1))
                field = match.group(2).strip()

                if seq not in contract_data:
                    contract_data[seq] = {}
                contract_data[seq][field] = col

        if not contract_data:
            return None

        all_contract_records = []

        # 共通カラム名を取得（1回だけ実行）
        common_cols = ['シート種別', '事業名', '府省庁の建制順', '政策所管府省庁',
                      '府省庁', '局・庁', '部', '課', '室', '班', '係']
        available_common_cols = [col for col in common_cols if col in df.columns]

        # 各行を処理
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを抽出（1回だけ）
            row_common_data = {}
            row_common_data['事業年度'] = common_data.get('事業年度', '')
            for col in available_common_cols:
                val = row[col]
                row_common_data[col] = str(val).strip() if pd.notna(val) else ''

            # 契約番号カウンター
            contract_number = 1

            # 各連番のデータを処理
            for seq in sorted(contract_data.keys()):
                fields = contract_data[seq]

                # 各フィールドの値を取得（空欄チェックを最小限に）
                block_name_val = row.get(fields.get('ブロック名')) if 'ブロック名' in fields else None
                contractor_val = row.get(fields.get('契約先')) if '契約先' in fields else None
                contract_amount_val = row.get(fields.get('契約額（百万円）')) if '契約額（百万円）' in fields else None

                # いずれかのフィールドに値がある場合のみ処理
                has_data = (
                    (pd.notna(contractor_val) and str(contractor_val).strip()) or
                    (pd.notna(block_name_val) and str(block_name_val).strip()) or
                    (pd.notna(contract_amount_val) and str(contract_amount_val).strip())
                )

                if not has_data:
                    continue

                # 値がある場合のみ詳細取得
                block_name = str(block_name_val).strip() if pd.notna(block_name_val) else ''
                contractor = str(contractor_val).strip() if pd.notna(contractor_val) else ''
                contract_amount = str(contract_amount_val).strip() if pd.notna(contract_amount_val) else ''

                corporate_number_val = row.get(fields.get('法人番号')) if '法人番号' in fields else None
                corporate_number = str(corporate_number_val).strip() if pd.notna(corporate_number_val) else ''

                work_summary_val = row.get(fields.get('業務概要')) if '業務概要' in fields else None
                work_summary = str(work_summary_val).strip() if pd.notna(work_summary_val) else ''

                contract_method_val = row.get(fields.get('契約方式等')) if '契約方式等' in fields else None
                contract_method = str(contract_method_val).strip() if pd.notna(contract_method_val) else ''

                bidders_val = row.get(fields.get('入札者数（応募者数）')) if '入札者数（応募者数）' in fields else None
                bidders = str(bidders_val).strip() if pd.notna(bidders_val) else ''

                bid_rate_val = row.get(fields.get('落札率')) if '落札率' in fields else None
                bid_rate = str(bid_rate_val).strip() if pd.notna(bid_rate_val) else ''

                # 一者応札理由（列名が長い）
                reason = ''
                for key in fields.keys():
                    if '一者応札' in key or '競争性のない随意契約' in key:
                        reason_val = row.get(fields[key])
                        if pd.notna(reason_val):
                            reason = str(reason_val).strip()
                        break

                # レコード作成
                record = row_common_data.copy()
                record['予算事業ID'] = business_id
                record['番号（契約）'] = contract_number
                record['支出先ブロック名'] = block_name
                record['契約先'] = contractor
                record['法人番号'] = corporate_number
                record['業務概要'] = work_summary
                record['契約額（百万円）'] = contract_amount
                record['契約方式'] = contract_method
                record['入札者数（応募者数）'] = bidders
                record['落札率'] = bid_rate
                record['一者応札・一者応募又は競争性のない随意契約となった理由及び改善策'] = reason

                all_contract_records.append(record)
                contract_number += 1

        if not all_contract_records:
            return None

        result_df = pd.DataFrame(all_contract_records)

        # 列順序を定義（RS System 2024準拠、27列）
        column_order = [
            'シート種別',
            '事業年度',
            '予算事業ID',
            '事業名',
            '府省庁の建制順',
            '政策所管府省庁',
            '府省庁',
            '局・庁',
            '部',
            '課',
            '室',
            '班',
            '係',
            '番号（契約）',
            '支出先ブロック名',
            '契約先',
            '法人番号',
            '業務概要',
            '契約額（百万円）',
            '契約方式',
            '入札者数（応募者数）',
            '落札率',
            '一者応札・一者応募又は競争性のない随意契約となった理由及び改善策',
            '備考1',
            '備考2',
            '備考3',
            '備考4'
        ]

        # 存在しない列は空文字列で追加
        for col in column_order:
            if col not in result_df.columns:
                result_df[col] = ''

        return result_df[column_order]

    def build_subsidy_rate_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        1-4_補助率等テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            補助率等テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 補助率等列を検出
        subsidy_col = None
        for col in columns:
            col_str = str(col)
            if '補助率等' in col_str or '補助率' == col_str:
                subsidy_col = col
                break

        if subsidy_col is None:
            # 補助率等列がない場合は空のテーブルを返す
            return None

        all_subsidy_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])

            # 補助率等データを取得
            subsidy_text = row[subsidy_col]
            if pd.isna(subsidy_text) or str(subsidy_text).strip() == '' or str(subsidy_text).strip() == '-':
                continue

            subsidy_text = str(subsidy_text).strip()

            # URLを抽出
            url_pattern = r'https?://[^\s,、。]+'
            urls = re.findall(url_pattern, subsidy_text)
            subsidy_url = urls[0] if urls else ''

            # URLを除いたテキスト
            text_without_url = re.sub(url_pattern, '', subsidy_text).strip()

            # 補助対象を抽出（正規表現パターン）
            subsidy_target = ''
            target_match = re.search(r'補助対象[：:]\s*([^補助率]+)', text_without_url)
            if target_match:
                subsidy_target = target_match.group(1).strip()

            # 補助率を抽出（正規表現パターン）
            subsidy_rate = ''
            rate_patterns = [
                r'補助率[：:]\s*([^\s、,]+)',
                r'([0-9]+/[0-9]+)',
                r'(定額)',
                r'([0-9]+%)'
            ]
            for pattern in rate_patterns:
                rate_match = re.search(pattern, text_without_url)
                if rate_match:
                    subsidy_rate = rate_match.group(1).strip()
                    break

            # 補助上限等を抽出（正規表現パターン）
            subsidy_limit = ''
            limit_patterns = [
                r'補助上限[：:]\s*([^\s、,]+)',
                r'上限額?[：:]\s*([^\s、,]+)',
                r'上限[：:]\s*([^\s、,]+)'
            ]
            for pattern in limit_patterns:
                limit_match = re.search(pattern, text_without_url)
                if limit_match:
                    subsidy_limit = limit_match.group(1).strip()
                    break

            # パースできなかった場合は全テキストを格納
            if not subsidy_target and not subsidy_rate and not subsidy_limit:
                # シンプルなテキストの場合はそのまま使用
                if len(text_without_url) < 100:
                    subsidy_rate = text_without_url
                else:
                    # 長いテキストは補助対象に格納
                    subsidy_target = text_without_url

            # レコード作成
            record = row_common_data.copy()
            record['予算事業ID'] = business_id
            record['番号（補助率等）'] = 1  # 簡略化のため常に1（複数ある場合は別途対応が必要）
            record['補助対象'] = subsidy_target
            record['補助率'] = subsidy_rate
            record['補助上限等'] = subsidy_limit
            record['補助率URL'] = subsidy_url

            all_subsidy_records.append(record)

        if all_subsidy_records:
            # 必要な列のみ選択して順序を整理
            result_df = pd.DataFrame(all_subsidy_records)

            # 列の順序を定義
            column_order = [
                'シート種別',
                '事業年度',
                '予算事業ID',
                '事業名',
                '府省庁の建制順',
                '政策所管府省庁',
                '府省庁',
                '局・庁',
                '部',
                '課',
                '室',
                '班',
                '係',
                '番号（補助率等）',
                '補助対象',
                '補助率',
                '補助上限等',
                '補助率URL'
            ]

            # 存在する列のみ選択
            existing_cols = [col for col in column_order if col in result_df.columns]
            return result_df[existing_cols]

        return None

    def build_remarks_table(
        self,
        df: pd.DataFrame,
        common_data: Dict,
        row_business_ids: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        6-1_その他備考テーブルを構築

        Args:
            df: 元のDataFrame
            common_data: 共通カラムデータ
            row_business_ids: 各行のビジネスID

        Returns:
            備考テーブル（DataFrame）
        """
        columns = df.columns.tolist()

        # 備考関連の列を検出
        remarks_col = None
        for col in columns:
            col_str = str(col)
            if col_str == '備考--' or col_str == '備考':
                remarks_col = col
                break

        # その他の指摘事項も検出（より詳細な情報源）
        other_remarks_col = None
        for col in columns:
            col_str = str(col)
            if 'その他の指摘事項' in col_str:
                other_remarks_col = col
                break

        all_remarks_records = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            business_id = row_business_ids.get(row_idx)
            if business_id is None:
                continue

            # この行の共通データを取得
            row_common_data = self.extract_common_columns(df.iloc[[row_idx]])
            record = row_common_data.copy()
            record['予算事業ID'] = business_id

            # 備考データを抽出
            remarks_value = ''

            # 備考列から取得
            if remarks_col is not None:
                val = row[remarks_col]
                if pd.notna(val) and str(val).strip() and str(val).strip() != '-':
                    remarks_value = str(val).strip()

            # その他の指摘事項も追加（存在する場合）
            if other_remarks_col is not None:
                val = row[other_remarks_col]
                if pd.notna(val) and str(val).strip() and str(val).strip() != '-':
                    other_val = str(val).strip()
                    if remarks_value:
                        remarks_value += '\n\n【その他の指摘事項】\n' + other_val
                    else:
                        remarks_value = other_val

            record['備考'] = remarks_value if remarks_value else ''

            all_remarks_records.append(record)

        if all_remarks_records:
            return pd.DataFrame(all_remarks_records)
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
    all_organization = []
    all_project_overview = []
    all_policy_law = []
    all_subsidy_rate = []
    all_related_projects = []
    all_inspection_evaluation = []
    all_budget_summary = []
    all_budget_category = []
    all_expenditure = []
    all_expense_usage = []
    all_multi_year_contract = []
    all_remarks = []

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

                # 組織情報テーブル
                org_df = builder.build_organization_table(df, common_data, row_business_ids)
                if org_df is not None:
                    all_organization.append(org_df)

                # 事業概要テーブル
                overview_df = builder.build_project_overview_table(df, common_data, row_business_ids)
                if overview_df is not None:
                    all_project_overview.append(overview_df)

                # 政策・施策、法令等テーブル
                policy_law_df = builder.build_policy_law_table(df, common_data, row_business_ids)
                if policy_law_df is not None:
                    all_policy_law.append(policy_law_df)

                # 予算執行サマリテーブル
                budget_df = builder.build_budget_summary_table(df, common_data, row_business_ids)
                if budget_df is not None:
                    all_budget_summary.append(budget_df)

                # 予算種別・歳出予算項目テーブル
                budget_category_df = builder.build_budget_category_table(df, common_data, row_business_ids)
                if budget_category_df is not None:
                    all_budget_category.append(budget_category_df)

                # 補助率等テーブル
                subsidy_df = builder.build_subsidy_rate_table(df, common_data, row_business_ids)
                if subsidy_df is not None:
                    all_subsidy_rate.append(subsidy_df)

                # 関連事業テーブル
                related_df = builder.build_related_projects_table(df, common_data, row_business_ids)
                if related_df is not None:
                    all_related_projects.append(related_df)

                # 点検・評価テーブル
                inspection_df = builder.build_inspection_evaluation_table(df, common_data, row_business_ids)
                if inspection_df is not None:
                    all_inspection_evaluation.append(inspection_df)

                # 支出先テーブル
                expenditure_df = builder.build_expenditure_table(df, common_data, row_business_ids)
                if expenditure_df is not None:
                    all_expenditure.append(expenditure_df)

                # 費目・使途テーブル
                expense_usage_df = builder.build_expense_usage_table(df, common_data, row_business_ids)
                if expense_usage_df is not None:
                    all_expense_usage.append(expense_usage_df)

                # 国庫債務負担行為等による契約テーブル
                multi_year_contract_df = builder.build_multi_year_contract_table(df, common_data, row_business_ids)
                if multi_year_contract_df is not None:
                    all_multi_year_contract.append(multi_year_contract_df)

                # 備考テーブル
                remarks_df = builder.build_remarks_table(df, common_data, row_business_ids)
                if remarks_df is not None:
                    all_remarks.append(remarks_df)

            success_count += 1

        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}", exc_info=True)
            failed_count += 1

    # 集約したテーブルを保存
    if all_organization:
        final_org = pd.concat(all_organization, ignore_index=True)
        output_file = year_output_dir / f"1-1_{year}_基本情報_組織情報.csv"
        final_org.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_org)} records)")

    if all_project_overview:
        final_overview = pd.concat(all_project_overview, ignore_index=True)
        output_file = year_output_dir / f"1-2_{year}_基本情報_事業概要.csv"
        final_overview.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_overview)} records)")

    if all_policy_law:
        final_policy_law = pd.concat(all_policy_law, ignore_index=True)
        output_file = year_output_dir / f"1-3_{year}_基本情報_政策・施策、法令等.csv"
        final_policy_law.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_policy_law)} records)")

    if all_budget_summary:
        final_budget = pd.concat(all_budget_summary, ignore_index=True)
        output_file = year_output_dir / f"2-1_{year}_予算・執行_サマリ.csv"
        final_budget.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_budget)} records)")

    if all_budget_category:
        final_budget_category = pd.concat(all_budget_category, ignore_index=True)
        output_file = year_output_dir / f"2-2_{year}_予算・執行_予算種別・歳出予算項目.csv"
        final_budget_category.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_budget_category)} records)")

    if all_subsidy_rate:
        final_subsidy = pd.concat(all_subsidy_rate, ignore_index=True)
        output_file = year_output_dir / f"1-4_{year}_基本情報_補助率等.csv"
        final_subsidy.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_subsidy)} records)")

    if all_related_projects:
        final_related = pd.concat(all_related_projects, ignore_index=True)
        output_file = year_output_dir / f"1-5_{year}_基本情報_関連事業.csv"
        final_related.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_related)} records)")

    if all_inspection_evaluation:
        final_inspection = pd.concat(all_inspection_evaluation, ignore_index=True)
        output_file = year_output_dir / f"4-1_{year}_点検・評価.csv"
        final_inspection.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_inspection)} records)")

    if all_expenditure:
        final_expenditure = pd.concat(all_expenditure, ignore_index=True)
        output_file = year_output_dir / f"5-1_{year}_支出先_支出情報.csv"
        final_expenditure.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_expenditure)} records)")

    if all_expense_usage:
        final_expense_usage = pd.concat(all_expense_usage, ignore_index=True)
        output_file = year_output_dir / f"5-3_{year}_支出先_費目・使途.csv"
        final_expense_usage.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_expense_usage)} records)")

    if all_multi_year_contract:
        final_multi_year_contract = pd.concat(all_multi_year_contract, ignore_index=True)
        output_file = year_output_dir / f"5-4_{year}_支出先_国庫債務負担行為等による契約.csv"
        final_multi_year_contract.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_multi_year_contract)} records)")

    if all_remarks:
        final_remarks = pd.concat(all_remarks, ignore_index=True)
        output_file = year_output_dir / f"6-1_{year}_その他備考.csv"
        final_remarks.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {output_file} ({len(final_remarks)} records)")

    return total_files, success_count, failed_count
