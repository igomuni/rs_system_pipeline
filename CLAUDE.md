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

### Data Unit Differences ⚠️

**CRITICAL**: Data has different monetary units depending on the source:

| Data Source | Years | Unit | Example |
|-------------|-------|------|---------|
| Historical Reviews (Excel/ZIP) | 2014-2023 | **Million Yen** | `29` = 29 million yen |
| RS System 2024 | 2024+ | **Yen (1円)** | `29,457,000` = ~29.46 million yen |

**Impact**: Direct comparison between 2014-2023 and 2024+ data will show 1,000,000x difference. Always convert units before comparison.

### Known Data Quality Issues

**2014**: Some projects have unit errors (~1,000x inflation) in budget/expenditure data
- Example: 電源立地地域対策交付金 shows 7.3 trillion yen (likely 7.3 billion yen)
- 64 records with budget > 10 trillion yen (百万円単位)
- **Policy**: Keep original values without correction; document in quality reports

**2016**: Expenditure data shows potential unit errors

Always run `python data_quality/generate_quality_report.py` after processing to verify data quality.

## FastAPI Integration

The pipeline can run as a web service:
- **POST /api/pipeline/run**: Start pipeline job
- **GET /api/pipeline/status/{job_id}**: Check job status
- **GET /api/pipeline/jobs**: List all jobs
- **POST /api/pipeline/cancel/{job_id}**: Cancel job
- **GET /api/results/{filename}**: Download processed files

## Data Quality & Validation Tools

### Budget Continuity Validation
```bash
# Validate cross-year project continuity (2019-2024)
python data_quality/validate_budget_continuity.py

# Generates:
# - data_quality/budget_continuity_validation.md
# - Year-over-year continuation rates
# - All-year continuing projects (1,653 projects across 2019-2024)
```

### RS System Conversion Gap Analysis
```bash
# Analyze gap between current output and RS System 2024 format
python data_quality/analyze_rs_conversion_gap.py

# Generates:
# - data_quality/rs_conversion_gap_2023.md
# - File-by-file comparison (current 3 files vs RS System 15 files)
# - Column difference details
```

**Key Findings**:
- Current output: 3 files per year (1-2, 2-1, 5-1)
- RS System 2024: 15 files with expanded columns
- Gap: 12 new files + column additions to existing files

See `data_quality/rs_conversion_gap_2023.md` for conversion roadmap.

## Testing Workflow

No formal test suite exists yet. Verify changes by:
1. Run pipeline: `python main.py`
2. Generate quality reports: `python data_quality/generate_quality_report.py`
3. Review `data_quality/DATA_QUALITY_REPORT.md` for anomalies
4. Check output files in `output/processed/year_*/`

## Next Tasks (TODO)

### Phase 1: Extend Existing Files to RS System Format (High Priority)

**Objective**: Expand current 3-file output (1-2, 2-1, 5-1) to match RS System 2024 column structure for 2023 data.

**Files to Update**:
1. **1-2_基本情報_事業概要**: Add 16 columns
   - 主要経費, 事業の概要, 事業区分, 事業概要URL, etc.
   - Extract from original 2023 Excel data

2. **2-1_予算・執行_サマリ**: Add 26 columns
   - 会計区分, 会計, 勘定, 執行率, 翌年度要求額, etc.
   - Extract from original 2023 Excel data
   - **IMPORTANT**: Convert monetary units (million yen → yen, multiply by 1,000,000)

3. **5-1_支出先_支出情報**: Add 9 columns
   - 法人種別, 所在地, 具体的な契約方式等, etc.
   - Extract from original 2023 Excel data
   - **IMPORTANT**: Convert monetary units (million yen → yen, multiply by 1,000,000)

**Implementation Steps**:
1. Analyze original 2023 Excel file structure (`data/download/`)
2. Update `src/pipeline/table_builder.py` to extract additional columns
3. Test with 2023 data only
4. Validate column mapping and data integrity
5. Apply to all years (2014-2023) once validated

**Reference**: See `data_quality/rs_conversion_gap_2023.md` for detailed column lists.

### Phase 2: Create New File Types (Lower Priority)

**Objective**: Generate 12 additional file types to match full RS System 2024 structure.

**New Files to Create**:
- 1-1_組織情報 (22 columns)
- 1-3_政策・施策、法令等 (28 columns)
- 1-4_補助率等 (18 columns)
- 1-5_関連事業 (17 columns)
- 2-2_予算種別・歳出予算項目 (26 columns)
- 3-1_効果発現経路_目標・実績 (82 columns)
- 3-2_効果発現経路_目標のつながり (23 columns)
- 4-1_点検・評価 (37 columns)
- 5-2_支出ブロックのつながり (22 columns)
- 5-3_費目・使途 (20 columns)
- 5-4_国庫債務負担行為等による契約 (27 columns)
- 6-1_その他備考 (14 columns)

**Note**: This phase requires extensive analysis of original Excel file structure and may not be necessary unless full RS System compatibility is required.

### Phase 3: Apply to All Years (2014-2023)

Once Phase 1 is validated with 2023 data, apply the same transformations to all historical years.
