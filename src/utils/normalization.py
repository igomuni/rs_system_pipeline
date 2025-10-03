"""
テキスト正規化ユーティリティ

日本語特有の表記揺れ、和暦変換、記号統一などを処理
"""
import re
import unicodedata
from typing import Optional

try:
    import neologdn
    HAS_NEOLOGDN = True
except ImportError:
    HAS_NEOLOGDN = False
    print("Warning: neologdn not installed. Using basic normalization only.")


# 和暦→西暦変換用の正規表現パターン
RE_WAREKI_SINGLE = re.compile(
    r'(明治|大正|昭和|平成|令和|M|T|S|H|R)(\d{1,2})年'
)

RE_WAREKI_RANGE = re.compile(
    r'(明治|大正|昭和|平成|令和|M|T|S|H|R)(\d{1,2})[-~〜～](\d{1,2})年'
)

# 和暦開始年の定義
WAREKI_START_YEARS = {
    '明治': 1868, 'M': 1868,
    '大正': 1912, 'T': 1912,
    '昭和': 1926, 'S': 1926,
    '平成': 1989, 'H': 1989,
    '令和': 2019, 'R': 2019,
}

# 丸数字→数字への変換
RE_LIST_MARKER = re.compile(r'[①-⑳]')

CIRCLE_NUMBER_MAP = {
    '①': '1', '②': '2', '③': '3', '④': '4', '⑤': '5',
    '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9', '⑩': '10',
    '⑪': '11', '⑫': '12', '⑬': '13', '⑭': '14', '⑮': '15',
    '⑯': '16', '⑰': '17', '⑱': '18', '⑲': '19', '⑳': '20',
}

# ハイフン・ダッシュの統一
HYPHEN_CHARS = [
    '‐', '‑', '‒', '–', '—', '―', '−', 'ー',  # 各種ハイフン・ダッシュ
    '─', '━', '～', '〜',  # 罫線、波ダッシュ
]

# カタカナの長音記号誤用パターン（例：サービスーの「ー」→「ス」）
RE_KATAKANA_HYPHEN = re.compile(r'([ァ-ヴ])ー(?=[^ァ-ヴー]|$)')


def convert_circle_numbers(text: str) -> str:
    """
    丸数字（①②③...）をアラビア数字に変換

    Args:
        text: 変換対象のテキスト

    Returns:
        変換後のテキスト
    """
    def replace_circle(match):
        return CIRCLE_NUMBER_MAP.get(match.group(0), match.group(0))

    return RE_LIST_MARKER.sub(replace_circle, text)


def convert_wareki_to_seireki(text: str) -> str:
    """
    和暦を西暦に変換

    例:
        - 平成25年 → 2013年
        - H25年 → 2013年
        - 平成25〜28年 → 2013〜2016年
        - 令和元年 → 2019年

    Args:
        text: 変換対象のテキスト

    Returns:
        変換後のテキスト
    """
    # 範囲指定の和暦（例：平成25〜28年）
    def replace_range(match):
        era = match.group(1)
        start_year = match.group(2)
        end_year = match.group(3)

        if era not in WAREKI_START_YEARS:
            return match.group(0)

        base_year = WAREKI_START_YEARS[era]

        # "元年" の処理
        if start_year == '元':
            start_year = '1'
        if end_year == '元':
            end_year = '1'

        try:
            seireki_start = base_year + int(start_year) - 1
            seireki_end = base_year + int(end_year) - 1
            return f"{seireki_start}〜{seireki_end}年"
        except (ValueError, TypeError):
            return match.group(0)

    # 単一の和暦（例：平成25年）
    def replace_single(match):
        era = match.group(1)
        year = match.group(2)

        if era not in WAREKI_START_YEARS:
            return match.group(0)

        base_year = WAREKI_START_YEARS[era]

        # "元年" の処理
        if year == '元':
            year = '1'

        try:
            seireki = base_year + int(year) - 1
            return f"{seireki}年"
        except (ValueError, TypeError):
            return match.group(0)

    # 範囲指定を先に処理
    text = RE_WAREKI_RANGE.sub(replace_range, text)
    # 単一年を処理
    text = RE_WAREKI_SINGLE.sub(replace_single, text)

    return text


def normalize_hyphens(text: str) -> str:
    """
    各種ハイフン・ダッシュ記号を標準的な「-」（ハイフンマイナス）に統一

    Args:
        text: 変換対象のテキスト

    Returns:
        変換後のテキスト
    """
    for char in HYPHEN_CHARS:
        text = text.replace(char, '-')
    return text


def fix_katakana_hyphen_errors(text: str) -> str:
    """
    カタカナの長音記号誤用を修正

    例: サービスー → サービス

    Args:
        text: 変換対象のテキスト

    Returns:
        変換後のテキスト
    """
    # カタカナの後の長音記号で、その後にカタカナが続かない場合は削除
    return RE_KATAKANA_HYPHEN.sub(r'\1', text)


def normalize_text(text: str, use_neologdn: bool = True) -> str:
    """
    テキストの正規化処理（メイン関数）

    処理内容:
    1. 丸数字の変換（①→1）
    2. neologdnによる正規化（オプション）
    3. Unicode NFKC正規化
    4. 和暦→西暦変換
    5. ハイフン・ダッシュの統一
    6. カタカナ長音記号の誤用修正
    7. 連続空白の削除

    Args:
        text: 正規化対象のテキスト
        use_neologdn: neologdnを使用するか（デフォルト: True）

    Returns:
        正規化されたテキスト
    """
    if not isinstance(text, str):
        return text

    if not text or text.strip() == '':
        return text

    # 1. 丸数字の変換
    text = convert_circle_numbers(text)

    # 2. neologdnによる正規化（利用可能な場合）
    if use_neologdn and HAS_NEOLOGDN:
        text = neologdn.normalize(text)

    # 3. Unicode NFKC正規化（全角/半角統一など）
    text = unicodedata.normalize('NFKC', text)

    # 4. 和暦→西暦変換
    text = convert_wareki_to_seireki(text)

    # 5. ハイフン・ダッシュの統一
    text = normalize_hyphens(text)

    # 6. カタカナ長音記号の誤用修正
    text = fix_katakana_hyphen_errors(text)

    # 7. 連続空白を1つの空白に
    text = re.sub(r'\s+', ' ', text)

    # 8. 前後の空白を削除
    text = text.strip()

    return text


def normalize_column_name(column: str) -> str:
    """
    カラム名の正規化

    改行、タブ、連続空白を削除し、前後の空白をトリミング

    Args:
        column: カラム名

    Returns:
        正規化されたカラム名
    """
    if not isinstance(column, str):
        return column

    # 改行・タブを空白に変換
    column = column.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

    # 連続空白を1つに
    column = re.sub(r'\s+', ' ', column)

    # 前後の空白を削除
    column = column.strip()

    return column


def extract_year_from_filename(filename: str) -> Optional[int]:
    """
    ファイル名から年度を抽出

    Args:
        filename: ファイル名

    Returns:
        年度（抽出できない場合はNone）
    """
    # database2014.xlsx のようなパターン
    match = re.search(r'database(\d{4})', filename)
    if match:
        return int(match.group(1))

    # database_220427.xlsx → 令和2年度 → 2020年
    match = re.search(r'database_(\d{2})(\d{2})(\d{2})', filename)
    if match:
        year_code = int(match.group(1))
        # 20年代は令和、それ以外は平成として推定
        if year_code >= 19:
            return 2000 + year_code
        else:
            return 1988 + year_code

    return None


if __name__ == "__main__":
    # テスト
    test_cases = [
        "平成25年度",
        "H25~28年",
        "令和元年",
        "①事業概要",
        "サービスー提供",
        "全角　　　空白",
    ]

    print("=== 正規化テスト ===")
    for text in test_cases:
        normalized = normalize_text(text)
        print(f"{text:20s} → {normalized}")
