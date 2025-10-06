# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This pipeline converts historical administrative review data (行政事業レビュー, 2014-2023) from various Excel/ZIP formats into standardized CSV files compatible with the RS System format. The output consists of 3 normalized tables per year:
- **1-2_基本情報_事業概要.csv**: Basic project information (~5,000 projects/year)
- **2-1_予算・執行_サマリ.csv**: Budget and execution summary (multi-year data per project)
- **5-1_支出先_支出情報.csv**: Expenditure details (top 10 recipients per project)

Each year's data uses sequential IDs (1-N) to link the three tables via `予算事業ID`.

## Common Commands

### Pipeline Execution
```bash
# Run full pipeline (all stages 1-4)
python main.py

# Run from specific stage
python main.py --stage 2  # Stages: 1, 2, 3, 4

# Run as API server
python main.py --server --host 0.0.0.0 --port 8000
```

### Data Quality Tools
```bash
# Generate comprehensive quality reports for all years (2014-2023)
python data_quality/generate_quality_report.py

# Generate budget/execution summary across all years
python data_quality/summary_report.py

# Generate expenditure summary across all years
python data_quality/expenditure_summary.py
```

### Installation
```bash
pip install -r requirements.txt
```

## Architecture

### Stage-Based Pipeline (src/pipeline/stages.py)

**Stage 1: Excel/ZIP → CSV Conversion** (Stage01_ExtractToCSV)
- Handles multiple Excel formats (single-sheet, two-sheet structures)
- Extracts data from ZIP archives containing multiple files
- Files mapped to years via `FILENAME_TO_YEAR` in config.py

**Stage 2: Text Normalization** (Stage02_NormalizeData)
- Japanese text normalization using neologdn
- Custom normalization for full-width/half-width characters
- Japanese era → Western year conversion (handles 2-digit year codes)
- Column name standardization

**Stage 3: RS System Table Splitting** (Stage03_ProcessTables)
- Delegates to `src/pipeline/table_builder.py:process_year_data()`
- Creates 3 standardized tables per year with unified `予算事業ID`
- Filters empty budget records to reduce file size (~70% reduction in v1.0.3)

**Stage 4: Schema Generation** (Stage04_GenerateSchema)
- Generates JSON schema definitions for each table
- Output to `output/schema/`

### Pipeline Manager (src/pipeline/manager.py)

- **PipelineManager**: Manages job lifecycle (create, run, cancel, status)
- **Job class**: Tracks job state (pending → in-progress → completed/failed/cancelled)
- **JobStatus enum**: Standard status values
- Supports both CLI and API (FastAPI) modes

### Data Flow

```
data/download/*.{xlsx,zip}
  ↓ Stage 1
output/raw/year_YYYY/*.csv
  ↓ Stage 2
output/normalized/year_YYYY/*.csv
  ↓ Stage 3
output/processed/year_YYYY/{1-2_基本情報_事業概要, 2-1_予算・執行_サマリ, 5-1_支出先_支出情報}.csv
  ↓ Stage 4
output/schema/*.json
```

### Configuration (config.py)

- **FILENAME_TO_YEAR**: Maps original filenames to fiscal years (2014-2023)
- **MINISTRY_MASTER**: Ministry/agency master data with sequential IDs (建制順)
- **MINISTRY_NAME_MAPPING**: Handles name variations (e.g., "文科省" → "文部科学省")
- **RS_STANDARD_COLUMNS**: Defines standard column sets for each table type

## Key Data Processing Logic

### Budget Event ID (予算事業ID)
Each year uses sequential numbering (1-N). This ID links all 3 tables for a given project within that year.

### Empty Data Filtering (v1.0.3+)
Budget records are filtered to include only years with actual data, reducing output by ~70% while maintaining data integrity.

### Known Data Quality Issues

**2014**: Some projects have unit errors (~1,000,000x inflation) in budget/expenditure data
**2016**: Expenditure data shows potential unit errors

Always run `python data_quality/generate_quality_report.py` after processing to verify data quality.

## FastAPI Integration

The pipeline can run as a web service:
- **POST /api/pipeline/run**: Start pipeline job
- **GET /api/pipeline/status/{job_id}**: Check job status
- **GET /api/pipeline/jobs**: List all jobs
- **POST /api/pipeline/cancel/{job_id}**: Cancel job
- **GET /api/results/{filename}**: Download processed files

## Testing Workflow

No formal test suite exists yet. Verify changes by:
1. Run pipeline: `python main.py`
2. Generate quality reports: `python data_quality/generate_quality_report.py`
3. Review `data_quality/DATA_QUALITY_REPORT.md` for anomalies
4. Check output files in `output/processed/year_*/`
