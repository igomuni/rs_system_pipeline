#!/usr/bin/env python3
"""
RS_2024データの正規化スクリプト

data/download/RS_2024/にあるCSVファイルを正規化し、
output/processed/year_2024/に出力します。

使用方法:
    python scripts/normalize_rs2024.py
    python scripts/normalize_rs2024.py --input data/download/RS_2024 --output output/processed/year_2024
"""
import sys
import logging
import zipfile
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.normalization import normalize_text, normalize_column_name

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# デフォルトディレクトリパス
DEFAULT_INPUT_DIR = project_root / "data" / "download" / "RS_2024"
DEFAULT_RAW_DIR = project_root / "output" / "raw" / "year_2024"
DEFAULT_OUTPUT_DIR = project_root / "output" / "processed" / "year_2024"


def normalize_rs2024_file(input_path: Path, output_path: Path) -> None:
    """
    RS_2024のCSVファイルを正規化して出力

    Args:
        input_path: 入力CSVファイルパス
        output_path: 出力CSVファイルパス
    """
    logger.info(f"Processing: {input_path.name}")

    # CSVを読み込み
    df = pd.read_csv(input_path, encoding='utf-8-sig', dtype=str, low_memory=False)
    logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

    # カラム名を正規化
    original_columns = df.columns.tolist()
    df.columns = [normalize_column_name(col) for col in df.columns]

    # カラム名変更をログ出力（変更があった場合のみ）
    for orig, norm in zip(original_columns, df.columns):
        if orig != norm:
            logger.debug(f"  Column renamed: '{orig}' -> '{norm}'")

    # データを正規化（文字列型のカラムのみ）
    for col in tqdm(df.columns, desc=f"  Normalizing {input_path.name}", leave=False):
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: normalize_text(x) if isinstance(x, str) else x)

    # 正規化済みCSVを保存
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"  Saved: {output_path.name}")


def extract_zip_files(input_dir: Path, raw_dir: Path) -> list:
    """
    ZIPファイルを展開してCSVファイルを取得

    Args:
        input_dir: ZIPファイルが格納されているディレクトリ
        raw_dir: 展開先のディレクトリ (output/raw/year_2024)

    Returns:
        展開されたCSVファイルのリスト
    """
    zip_files = sorted(input_dir.glob("*_RS_2024_*.zip"))

    if not zip_files:
        logger.info("No ZIP files found, looking for CSV files directly")
        return []

    logger.info(f"Found {len(zip_files)} ZIP files to extract")
    raw_dir.mkdir(parents=True, exist_ok=True)

    extracted_csv_files = []

    for zip_file in zip_files:
        logger.info(f"Extracting: {zip_file.name}")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # ZIPの中身を確認
                csv_files_in_zip = [f for f in zip_ref.namelist() if f.endswith('.csv')]

                if not csv_files_in_zip:
                    logger.warning(f"  No CSV files found in {zip_file.name}")
                    continue

                # CSVファイルを展開
                for csv_name in csv_files_in_zip:
                    zip_ref.extract(csv_name, raw_dir)
                    extracted_path = raw_dir / csv_name
                    extracted_csv_files.append(extracted_path)
                    logger.info(f"  Extracted: {csv_name}")

        except Exception as e:
            logger.error(f"Error extracting {zip_file.name}: {e}", exc_info=True)
            continue

    logger.info(f"Extracted files saved to: {raw_dir}")
    return extracted_csv_files


def normalize_rs2024_data(input_dir: Path = None, raw_dir: Path = None, output_dir: Path = None) -> bool:
    """
    RS2024形式のCSVデータを正規化

    main.pyから呼び出し可能な関数

    Args:
        input_dir: 入力ディレクトリ（デフォルト: data/download/RS_2024）
        raw_dir: ZIP解凍先ディレクトリ（デフォルト: output/raw/year_2024）
        output_dir: 正規化後の出力ディレクトリ（デフォルト: output/processed/year_2024）

    Returns:
        成功した場合True
    """
    # デフォルトパス設定
    if input_dir is None:
        input_dir = DEFAULT_INPUT_DIR
    if raw_dir is None:
        raw_dir = DEFAULT_RAW_DIR
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    # 入力ディレクトリチェック
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        return False

    # 出力ディレクトリ作成
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("RS2024 Data Normalization Pipeline")
    logger.info("=" * 80)
    logger.info(f"Input directory:  {input_dir}")
    logger.info(f"Raw directory:    {raw_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")

    # ZIPファイルを展開
    logger.info("Step 1: Extracting ZIP files")
    logger.info("-" * 80)
    extracted_csv_files = extract_zip_files(input_dir, raw_dir)
    logger.info("")

    # 処理対象ファイルリスト（解凍されたファイル）
    if not extracted_csv_files:
        # ZIPがない場合はrawディレクトリから直接CSVを読む
        extracted_csv_files = sorted(raw_dir.glob("*_RS_2024_*.csv"))
        if not extracted_csv_files:
            extracted_csv_files = sorted(raw_dir.glob("*.csv"))

    if not extracted_csv_files:
        logger.warning(f"No CSV files found in {raw_dir}")
        return False

    logger.info(f"Step 2: Normalizing {len(extracted_csv_files)} CSV files")
    logger.info("-" * 80)

    # 各ファイルを正規化
    processed_count = 0
    error_count = 0

    for input_file in extracted_csv_files:
        # 出力ファイル名（_RS_2024_ → _2024_）
        output_filename = input_file.name.replace('_RS_2024_', '_2024_')
        output_file = output_dir / output_filename

        try:
            normalize_rs2024_file(input_file, output_file)
            processed_count += 1
        except Exception as e:
            logger.error(f"Error processing {input_file.name}: {e}", exc_info=True)
            error_count += 1
            continue

    # サマリー出力
    logger.info("")
    logger.info("=" * 80)
    logger.info("Processing Summary")
    logger.info("=" * 80)
    logger.info(f"Total files:      {len(extracted_csv_files)}")
    logger.info(f"Processed:        {processed_count}")
    logger.info(f"Errors:           {error_count}")
    logger.info("")
    logger.info(f"Raw files:        {raw_dir}")
    logger.info(f"Normalized files: {output_dir}")
    logger.info("=" * 80)

    return error_count == 0


def main():
    """メイン処理（CLI実行用）"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Normalize RS2024 format CSV data"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input directory (default: data/download/RS_2024)",
    )
    parser.add_argument(
        "--raw",
        type=Path,
        default=None,
        help="Raw directory for extracted files (default: output/raw/year_2024)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: output/processed/year_2024)",
    )

    args = parser.parse_args()

    success = normalize_rs2024_data(args.input, args.raw, args.output)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
