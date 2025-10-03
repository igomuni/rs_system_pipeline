"""
スキーマ定義生成モジュール

処理済みCSVからスキーマ定義（JSON）を生成
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class SchemaGenerator:
    """スキーマ生成クラス"""

    @staticmethod
    def infer_data_type(series: pd.Series) -> str:
        """
        pandas Seriesからデータ型を推論

        Args:
            series: pandas Series

        Returns:
            データ型文字列
        """
        dtype = series.dtype

        if pd.api.types.is_integer_dtype(dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(dtype):
            return "number"
        elif pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        else:
            return "string"

    @staticmethod
    def get_sample_values(series: pd.Series, num_samples: int = 5) -> List[Any]:
        """
        サンプル値を取得（NULL以外）

        Args:
            series: pandas Series
            num_samples: サンプル数

        Returns:
            サンプル値のリスト
        """
        non_null_values = series.dropna().unique()

        if len(non_null_values) == 0:
            return []

        # 最大num_samplesまで取得
        samples = non_null_values[:num_samples].tolist()

        # JSON serializable に変換
        json_safe_samples = []
        for value in samples:
            if pd.isna(value):
                continue
            # numpy型をPython標準型に変換
            if hasattr(value, 'item'):  # numpy scalar
                value = value.item()
            if isinstance(value, (int, float, str, bool)):
                json_safe_samples.append(value)
            else:
                json_safe_samples.append(str(value))

        return json_safe_samples

    @staticmethod
    def get_value_stats(series: pd.Series) -> Dict[str, Any]:
        """
        値の統計情報を取得

        Args:
            series: pandas Series

        Returns:
            統計情報の辞書
        """
        # numpy型をPython標準型に変換するヘルパー
        def to_python_type(val):
            if pd.isna(val):
                return None
            if hasattr(val, 'item'):  # numpy scalar
                return val.item()
            return val

        stats = {
            "total_count": int(len(series)),
            "non_null_count": int(series.count()),
            "null_count": int(series.isna().sum()),
            "unique_count": int(series.nunique()),
        }

        # 数値型の場合は統計を追加
        if pd.api.types.is_numeric_dtype(series):
            stats["min"] = to_python_type(series.min())
            stats["max"] = to_python_type(series.max())
            stats["mean"] = to_python_type(series.mean())
            stats["median"] = to_python_type(series.median())

        # 文字列型の場合は長さ統計を追加
        elif series.dtype == 'object':
            str_lengths = series.dropna().astype(str).str.len()
            if len(str_lengths) > 0:
                stats["min_length"] = int(str_lengths.min())
                stats["max_length"] = int(str_lengths.max())
                stats["avg_length"] = float(str_lengths.mean())

        return stats

    @classmethod
    def generate_schema_from_dataframe(
        cls, df: pd.DataFrame, file_name: str
    ) -> Dict[str, Any]:
        """
        DataFrameからスキーマ定義を生成

        Args:
            df: DataFrame
            file_name: ファイル名

        Returns:
            スキーマ定義の辞書
        """
        schema = {
            "file_name": file_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": []
        }

        for idx, column in enumerate(df.columns):
            series = df[column]

            column_schema = {
                "index": idx,
                "name": column,
                "data_type": cls.infer_data_type(series),
                "nullable": bool(series.isna().any()),
                "statistics": cls.get_value_stats(series),
                "sample_values": cls.get_sample_values(series, num_samples=5),
            }

            schema["columns"].append(column_schema)

        return schema

    @classmethod
    def generate_schema_from_csv(
        cls, csv_path: Path, max_rows: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        CSVファイルからスキーマ定義を生成

        Args:
            csv_path: CSVファイルパス
            max_rows: 読み込む最大行数（Noneの場合は全行）

        Returns:
            スキーマ定義の辞書
        """
        try:
            # CSVを読み込み
            df = pd.read_csv(csv_path, encoding='utf-8-sig', nrows=max_rows)

            schema = cls.generate_schema_from_dataframe(df, csv_path.name)

            # ファイル情報を追加
            schema["file_size_bytes"] = csv_path.stat().st_size
            schema["file_path"] = str(csv_path)

            return schema

        except Exception as e:
            logger.error(f"Error generating schema for {csv_path}: {e}")
            return {
                "file_name": csv_path.name,
                "error": str(e),
            }

    @classmethod
    def _convert_numpy_types(cls, obj: Any) -> Any:
        """
        Recursively convert numpy types to Python native types

        Args:
            obj: Object to convert

        Returns:
            Converted object
        """
        # Check for numpy scalar types first
        if hasattr(obj, 'item'):  # numpy scalar
            value = obj.item()
            # Check if the converted value is NaN
            try:
                if pd.isna(value):
                    return None
            except (ValueError, TypeError):
                pass
            return value

        # Check for standard Python NaN
        try:
            if pd.isna(obj):
                return None
        except (ValueError, TypeError):
            pass

        # Recursively convert collections
        if isinstance(obj, dict):
            return {key: cls._convert_numpy_types(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [cls._convert_numpy_types(item) for item in obj]

        return obj

    @classmethod
    def save_schema_to_json(cls, schema: Dict[str, Any], output_path: Path):
        """
        スキーマをJSONファイルに保存

        Args:
            schema: スキーマ定義
            output_path: 出力パス
        """
        try:
            # Convert all numpy types before saving
            schema_converted = cls._convert_numpy_types(schema)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(schema_converted, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved schema: {output_path}")

        except Exception as e:
            logger.error(f"Error saving schema to {output_path}: {e}")

    @classmethod
    def generate_unified_schema(
        cls, schemas: List[Dict[str, Any]], output_path: Path
    ):
        """
        複数のスキーマを統合して1つのJSONに保存

        Args:
            schemas: スキーマ定義のリスト
            output_path: 出力パス
        """
        unified = {
            "total_files": len(schemas),
            "files": schemas,
        }

        # 統計情報を追加
        total_rows = sum(s.get("row_count", 0) for s in schemas)
        total_columns = sum(s.get("column_count", 0) for s in schemas)

        unified["statistics"] = {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "avg_rows_per_file": total_rows / len(schemas) if schemas else 0,
            "avg_columns_per_file": total_columns / len(schemas) if schemas else 0,
        }

        cls.save_schema_to_json(unified, output_path)

    @classmethod
    def compare_schemas(
        cls, schema1: Dict[str, Any], schema2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        2つのスキーマを比較

        Args:
            schema1: スキーマ1
            schema2: スキーマ2

        Returns:
            比較結果の辞書
        """
        comparison = {
            "file1": schema1.get("file_name"),
            "file2": schema2.get("file_name"),
            "differences": [],
        }

        # カラム数の比較
        col_count1 = schema1.get("column_count", 0)
        col_count2 = schema2.get("column_count", 0)

        if col_count1 != col_count2:
            comparison["differences"].append({
                "type": "column_count",
                "file1_value": col_count1,
                "file2_value": col_count2,
            })

        # カラム名の比較
        cols1 = {c["name"] for c in schema1.get("columns", [])}
        cols2 = {c["name"] for c in schema2.get("columns", [])}

        only_in_file1 = cols1 - cols2
        only_in_file2 = cols2 - cols1

        if only_in_file1:
            comparison["differences"].append({
                "type": "columns_only_in_file1",
                "columns": list(only_in_file1),
            })

        if only_in_file2:
            comparison["differences"].append({
                "type": "columns_only_in_file2",
                "columns": list(only_in_file2),
            })

        # 共通カラムのデータ型比較
        common_cols = cols1 & cols2
        for col_name in common_cols:
            col1 = next(c for c in schema1["columns"] if c["name"] == col_name)
            col2 = next(c for c in schema2["columns"] if c["name"] == col_name)

            if col1["data_type"] != col2["data_type"]:
                comparison["differences"].append({
                    "type": "data_type_mismatch",
                    "column": col_name,
                    "file1_type": col1["data_type"],
                    "file2_type": col2["data_type"],
                })

        return comparison


def process_directory_schemas(
    input_dir: Path, output_dir: Path, max_rows: Optional[int] = 10000
) -> int:
    """
    ディレクトリ内の全CSVファイルのスキーマを生成

    Args:
        input_dir: 入力ディレクトリ
        output_dir: 出力ディレクトリ
        max_rows: スキーマ生成時の最大読み込み行数

    Returns:
        処理したファイル数
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_dir.rglob("*.csv"))
    schemas = []

    logger.info(f"Generating schemas for {len(csv_files)} files")

    for csv_file in csv_files:
        logger.info(f"Processing: {csv_file}")

        schema = SchemaGenerator.generate_schema_from_csv(csv_file, max_rows)
        schemas.append(schema)

        # 個別のスキーマJSONを保存
        relative_path = csv_file.relative_to(input_dir)
        schema_filename = relative_path.stem + "_schema.json"
        schema_output_path = output_dir / relative_path.parent / schema_filename
        schema_output_path.parent.mkdir(parents=True, exist_ok=True)

        SchemaGenerator.save_schema_to_json(schema, schema_output_path)

    # 統合スキーマを保存
    unified_output_path = output_dir / "unified_schema.json"
    SchemaGenerator.generate_unified_schema(schemas, unified_output_path)

    logger.info(f"Generated schemas for {len(csv_files)} files")
    return len(csv_files)
