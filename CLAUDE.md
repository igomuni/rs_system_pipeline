# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This pipeline converts historical administrative review data (行政事業レビュー, 2014-2023) from various Excel/ZIP formats into standardized CSV files compatible with the RS System 2024 format. **Phase 2 completed (75%)**: The output now consists of **12 normalized tables per year** (9 newly implemented + 3 core tables):

**Core Tables (1-2, 2-1, 5-1)**:
- **1-2_基本情報_事業概要.csv**: Basic project information (~5,000 projects/year)
- **2-1_予算・執行_サマリ.csv**: Budget and execution summary (multi-year data per project)
- **5-1_支出先_支出情報.csv**: Expenditure details (top 10 recipients per project)

**New Tables (Phase 2, 9 files)**:
- **1-1_組織情報.csv**: Organization hierarchy (22 columns)
- **1-3_政策・施策、法令等.csv**: Policy system and legal basis (28 columns)
- **1-4_補助率等.csv**: Subsidy rates (18 columns)
- **1-5_関連事業.csv**: Related projects (17 columns)
- **2-2_予算種別・歳出予算項目.csv**: Budget type details (26 columns)
- **4-1_点検・評価.csv**: Review and evaluation (37 columns)
- **5-3_費目・使途.csv**: Expense purposes (20 columns)
- **5-4_国庫債務負担行為等による契約.csv**: Multi-year contracts (27 columns)
- **6-1_その他備考.csv**: Remarks (14 columns)

Each year's data uses sequential IDs (1-N) to link all 12 tables via `予算事業ID`.

## Common Commands

### Pipeline Execution
```bash
# Run full pipeline (all stages 1-4)
python main.py

# Run from specific stage
python main.py --stage 2  # Stages: 1, 2, 3, 4

# Process specific year only (2014-2023)
python main.py --year 2023

# Combine stage and year parameters
python main.py --stage 3 --year 2023

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
- Creates **12 standardized tables per year** with unified `予算事業ID` (Phase 2 completed, 75% implementation)
- Filters empty budget records to reduce file size (~70% reduction in v1.0.3)
- Implemented table builders:
  - Core 3 files: `build_overview_table()`, `build_budget_table()`, `build_expenditure_table()`
  - New 9 files: `build_organization_table()`, `build_policy_law_table()`, `build_subsidy_rate_table()`, `build_related_projects_table()`, `build_budget_type_table()`, `build_review_evaluation_table()`, `build_expense_purpose_table()`, `build_multi_year_contract_table()`, `build_remarks_table()`

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
output/processed/year_YYYY/{1-1_組織情報, 1-2_基本情報_事業概要, 1-3_政策・施策・法令等, 1-4_補助率等, 1-5_関連事業, 2-1_予算・執行_サマリ, 2-2_予算種別・歳出予算項目, 4-1_点検・評価, 5-1_支出先_支出情報, 5-3_費目・使途, 5-4_国庫債務負担行為等による契約, 6-1_その他備考}.csv (12 files)
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
Each year uses sequential numbering (1-N). This ID links all **12 tables** for a given project within that year (Phase 2 completed).

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
- **GET /api/results/{filename:path}**: Download processed files (supports subdirectories, with path traversal protection)

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
- ~~Current output: 3 files per year (1-2, 2-1, 5-1)~~ **Updated: Now 12 files per year (Phase 2 completed)**
- RS System 2024: 15 files with expanded columns
- ~~Gap: 12 new files + column additions to existing files~~ **Updated: 9 new files implemented, 3 skipped**
  - ✅ Implemented: 1-1, 1-3, 1-4, 1-5, 2-2, 4-1, 5-3, 5-4, 6-1 (9 files)
  - ❌ Skipped: 5-2 (no source data), 3-1/3-2 (complexity vs. utility)

See `data_quality/rs_conversion_gap_2023.md` for original analysis.

## Testing Workflow

No formal test suite exists yet. Verify changes by:
1. Run pipeline: `python main.py`
2. Generate quality reports: `python data_quality/generate_quality_report.py`
3. Review `data_quality/DATA_QUALITY_REPORT.md` for anomalies
4. Check output files in `output/processed/year_*/`

## Next Tasks (TODO)

### Phase 1: Extend Existing Files to RS System Format ⏭️ **DEFERRED**

**Status**: Deferred in favor of completing Phase 2 (new file types) first.

**Objective**: Expand core 3-file output (1-2, 2-1, 5-1) to match RS System 2024 column structure.

**Files to Update**:
1. **1-2_基本情報_事業概要**: Add 16 columns (主要経費, 事業の概要, etc.)
2. **2-1_予算・執行_サマリ**: Add 26 columns (会計区分, 会計, 勘定, etc.)
3. **5-1_支出先_支出情報**: Add 9 columns (法人種別, 所在地, etc.)

**Note**: Monetary unit conversion (million yen → yen) is **NOT recommended** for historical data (2014-2023) to maintain consistency. Units should be converted only when merging with RS System 2024+ data.

**Reference**: See `data_quality/rs_conversion_gap_2023.md` for detailed column lists.

### Phase 2: Create New File Types ✅ **COMPLETED (75%)**

**Objective**: Generate 12 additional file types to match full RS System 2024 structure.

**Implementation Status** (9/12 files completed):

✅ **Completed Files**:
1. **6-1_その他備考** (14 columns) - 基本実装、備考フィールド抽出
2. **1-1_組織情報** (22 columns) - 組織階層情報の構造化
3. **1-4_補助率等** (18 columns) - テキストパース、補助率情報抽出
4. **1-3_政策・施策、法令等** (28 columns) - 政策体系・法令情報
5. **2-2_予算種別・歳出予算項目** (26 columns) - 予算種別の詳細展開
6. **4-1_点検・評価** (37 columns) - 評価結果・改善方針
7. **1-5_関連事業** (17 columns) - 関連事業情報（統合・分割等）
8. **5-3_費目・使途** (20 columns) - 費目別使途詳細
9. **5-4_国庫債務負担行為等による契約** (27 columns) - 複数年度契約情報

❌ **実装見送り（データ不足）**:
- **5-2_支出ブロックのつながり** (22 columns) - 元データに複数ブロック間の関連情報が存在せず実装不可

⏭️ **スキップ（複雑性vs価値）**:
- **3-1_効果発現経路_目標・実績** (82 columns) - 82列の複雑な構造、実用性低い
- **3-2_効果発現経路_目標のつながり** (23 columns) - 3-1に依存、実装見送り

**実装箇所**: `src/pipeline/table_builder.py` 内の各メソッド
- `build_remarks_table()` (6-1)
- `build_organization_table()` (1-1)
- `build_subsidy_rate_table()` (1-4)
- `build_policy_law_table()` (1-3)
- `build_budget_type_table()` (2-2)
- `build_review_evaluation_table()` (4-1)
- `build_related_projects_table()` (1-5)
- `build_expense_purpose_table()` (5-3)
- `build_multi_year_contract_table()` (5-4)

### Phase 3: Apply to All Years (2014-2023)

Once Phase 1 is validated with 2023 data, apply the same transformations to all historical years.
