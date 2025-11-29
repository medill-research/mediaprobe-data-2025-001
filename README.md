# Galvanic Skin Response Analysis

## Overview

This project contains analysis code for studying how emotional responses (measured through Galvanic Skin Response and valence) transfer between media programming and advertisements. The analysis investigates:

- **Bidirectional context effects**: How program content affects ad responses and vice versa
- **Ad-position dynamics**: The impact of ad placement within commercial pods
- **Temporal effects**: Emotional carryover between program segments and ad breaks

The pipeline processes raw sensor data through multiple transformation stages, creating feature-rich datasets for statistical modeling using mixed-effects models.

## Project Structure

```
galvanic-skin/
├── configs/                           # Configuration files
│   └── preprocessing_configs.yaml     # Data staging and loading configurations
├── data/                              # Data directory (DuckDB + CSVs)
│   ├── source/                        # Raw input data files
│   │   ├── ads_metadata.csv           # Ad-level metadata (Round 1)
│   │   ├── ads_metadata_v2.csv        # Ad-level metadata (Round 2 - 11/27/2025)
│   │   ├── session_metadata.csv       # Session and program metadata
│   │   ├── timeline_metadata.csv      # Timestamped GSR data (Round 1)
│   │   └── timeline_metadata_11272025.csv  # Timestamped GSR data (Round 2)
│   ├── export/                        # Final analysis-ready datasets
│   │   ├── master_ads.csv             # Main ad modeling dataset
│   │   ├── master_ads_11272025.csv    # Round 2 ad modeling dataset
│   │   ├── reciprocal_ads.csv         # Reciprocal effects dataset
│   │   └── reciprocal_ads_11272025.csv # Round 2 reciprocal effects dataset
│   ├── icoria/                        # Reference data from ICORIA source
│   │   ├── source_modeling.csv        # ICORIA modeling data
│   │   └── source_reciprocal.parquet  # ICORIA reciprocal data
│   ├── results/                       # Analysis outputs and results
│   └── galvanic.duckdb                # DuckDB database (staging + feature tables)
├── dbt_run/                           # dbt transformation project
│   ├── models/                        # SQL transformation models
│   │   ├── feature/                   # Feature engineering layer
│   │   ├── curated/                   # Business logic layer
│   │   └── export/                    # Export layer (CSV outputs)
│   ├── dbt_project.yml                # dbt project configuration
│   └── profiles.yml                   # DuckDB connection profile
├── scripts/                           # Utility scripts
│   ├── migration/                     # Database schema creation scripts
│   └── notebook/                      # Analysis notebooks
│       └── jar_modeling.rmd           # Main R Markdown modeling notebook
├── src/                               # Python source code
│   ├── preprocessing/                 # Data loading and staging
│   │   └── staging_load.py            # Loads CSVs into DuckDB staging tables
│   ├── schemas/                       # Data validation schemas
│   │   └── source_data.py             # Pydantic models for source data
│   ├── utils/                         # Utility functions
│   │   └── duckdb_utils.py            # DuckDB helper functions
│   └── global_configs.py              # Global configuration paths
├── pyproject.toml                     # Python project dependencies
└── uv.lock                            # Locked dependency versions
```

## Data Sources

### Input Files (data/source/)

1. **session_metadata.csv**: Viewing session and program characteristics
   - Session IDs, country, channel, program format/genre
   - Demographic breakdowns (age groups, gender distribution)
   - Start timestamps

2. **timeline_metadata.csv** / **timeline_metadata_11272025.csv**: Second-by-second GSR measurements during programs
   - Phasic program response (arousal/activation)
   - ARS program response (affective response scale/valence)
   - Timestamps aligned with session timeline

3. **ads_metadata.csv** / **ads_metadata_v2.csv**: Advertisement-level measurements
   - Ad descriptions, categories (Round 1), comparison labels (Round 2)
   - Time in/out timestamps
   - Phasic and ARS measurements for each ad

### Output Files (data/export/)

1. **master_ads.csv**: Primary modeling dataset
   - One row per ad occurrence
   - Features: pod number, ad position, previous program/ad emotional metrics
   - Demographic and program context variables
   - Rolling ad counts, repeated ad indicators

2. **reciprocal_ads.csv**: Reciprocal effects dataset
   - Analyzes how ads affect subsequent program engagement
   - Captures last ad of each pod and next program segment metrics

## Prerequisites

- **Python**: >= 3.13
- **R**: >= 4.0 (for statistical modeling)
- **uv**: Python package manager (recommended) or pip

### Python Dependencies

```toml
dbt-core >= 1.10.15
dbt-duckdb >= 1.10.0
duckdb >= 1.4.2
polars >= 1.35.2
pyarrow >= 22.0.0
pydantic >= 2.12.5
pyyaml >= 6.0.3
```

### R Dependencies

```r
here, nanoparquet, lmerTest, performance, car, forcats,
ggplot2, sandwich, lmtest, caret, dplyr, data.table
```

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd galvanic-skin
```

### 2. Set up Python environment

Using `uv` (recommended):
```bash
uv sync
```

Or using pip:
```bash
pip install -r requirements.txt
```

### 3. Install R packages

```r
install.packages(c("here", "nanoparquet", "lmerTest", "performance",
                   "car", "forcats", "ggplot2", "sandwich", "lmtest",
                   "caret", "dplyr", "data.table"))
```

## Data Pipeline

The data processing pipeline consists of four main stages:

### Stage 1: Data Staging (Python)

**Purpose**: Load raw CSV files into DuckDB staging tables with schema validation

**Script**: `src/preprocessing/staging_load.py`

**Process**:
1. Reads CSV files using Polars with strict schema validation (defined in `src/schemas/source_data.py`)
2. Applies data type enforcement and date parsing
3. Truncates and reloads staging tables in DuckDB

**Key Data Cleaning Steps**:
- Column length validation against Pydantic schemas
- Type coercion for proper data types (Int64, Float64, Datetime, Time)
- Handles missing values with appropriate defaults

**Run manually** (example for Round 2 timeline data):
```python
python -c "from src.preprocessing.staging_load import load_staging_table; load_staging_table('Timeline_Metadata_11272025')"
```

**Available process names** (from `configs/preprocessing_configs.yaml`):
- `Session_Metadata`
- `Timeline_Metadata`
- `Timeline_Metadata_11272025`
- `Ads_Metadata`
- `Ads_Metadata_11272025`

### Stage 2: Feature Engineering (dbt - Feature Layer)

**Location**: `dbt_run/models/feature/`

#### 2.1. ads_expanded.sql / ads_expanded_11272025.sql

**Purpose**: Expand ad-level data to second-by-second granularity

**Data Cleaning**:
- Trims whitespace from description and category fields
- Filters out empty descriptions
- Validates time_in <= time_out
- Handles null phasic/ARS values (replaces with 0)
- **Winsorization**: Clips ARS values at 1st and 99th percentiles to remove outliers
- **Time overlap correction**: Detects overlapping ad times and adjusts by adding 1 second
- **Temporal expansion**: Unnests each ad into second-by-second records using `generate_series()`

**Output**: One row per second for each ad, with aggregated GSR metrics

#### 2.2. program_metadata.sql / program_metadata_11272025.sql

**Purpose**: Join timeline (program) data with expanded ad data

**Data Cleaning**:
- **Winsorization**: Clips ARS program values at 1st and 99th percentiles
- Converts timestamps to TIME type for temporal joins
- Creates ads_indicator flag (0 = program content, 1 = ad content)
- Left joins with ads_expanded to align second-by-second

**Output**: Unified timeline with program and ad measurements at second-level granularity

#### 2.3. vw_program_ranked.sql / vw_program_ranked_11272025.sql

**Purpose**: Detect program segments, ad pods, and ad positions

**Logic**:
1. **Segment detection**: Identifies transitions between program and ad content using LAG() window functions
2. **Run ID assignment**: Creates sequential IDs for consecutive program/ad runs
3. **Ad position detection**: Within each ad pod, detects individual ads and assigns positions
4. **Change point detection**: Uses LAG() to identify when ad descriptions change

**Output**: View with segment_number (program segments), ad_position (position within pod), and ads_indicator

### Stage 3: Business Logic (dbt - Curated Layer)

**Location**: `dbt_run/models/curated/`

#### 3.1. master_ads.sql / master_ads_11272025.sql

**Purpose**: Create primary modeling dataset with contextual features

**Feature Engineering**:
- **Ad-level features**:
  - `ad_position`: Position of ad within its pod
  - `prev_phasic_ads`, `prev_ars_ads`: Lagged emotional response from previous ad in pod
  - `rolling_ads_count`: Cumulative ad count within session
  - `repeated_ad`: Binary indicator if ad appeared earlier in same pod

- **Program context features**:
  - `prev_phasic_program`, `prev_ars_program`: Average GSR from last 30 seconds of preceding program segment
  - Program metadata: country, channel, format, genre, subgenre, program name
  - Demographics: age distributions, gender percentages

**Logic**:
- Deduplicates ads within pods (keeps first occurrence per second)
- Computes LAG() for previous ad metrics within pod
- Averages program metrics over 30-second window before pod
- Joins with session metadata for contextual variables

**Output**: `data/export/master_ads.csv` (via export layer)

#### 3.2. reciprocal_ads.sql / reciprocal_ads_11272025.sql

**Purpose**: Analyze reciprocal effects (how ads influence subsequent program engagement)

**Feature Engineering**:
- Captures **last ad** from each pod (highest ad_position)
- Captures **first 30 seconds** of subsequent program segment
- Links pod N to program segment N+1

**Logic**:
1. Identifies last ad in each pod using ROW_NUMBER() DESC
2. Computes average program GSR from first 30 seconds of next segment
3. Creates segment offset (+1) to join last ad with next program

**Output**: `data/export/reciprocal_ads.csv` (via export layer)

### Stage 4: Export (dbt - Export Layer)

**Location**: `dbt_run/models/export/`

**Purpose**: Materialize curated tables as external CSV files for R analysis

**Models**:
- `export_master_ads.sql` exports to `data/export/master_ads.csv`
- `export_master_ads_11272025.sql` exports to `data/export/master_ads_11272025.csv`
- `export_reciprocal_ads.sql` exports to `data/export/reciprocal_ads.csv`
- `export_reciprocal_ads_11272025.sql` exports to `data/export/reciprocal_ads_11272025.csv`

## Running the Pipeline

### Complete Pipeline Execution

#### Step 1: Load staging data (Python)

```python
from src.preprocessing.staging_load import load_staging_table

# Load all source files
load_staging_table("Session_Metadata")
load_staging_table("Timeline_Metadata")
load_staging_table("Timeline_Metadata_11272025")
load_staging_table("Ads_Metadata")
load_staging_table("Ads_Metadata_11272025")
```

#### Step 2: Run dbt transformations

```bash
cd dbt_run

# Run all models (feature -> curated -> export)
dbt run

# Or run specific layers
dbt run --select feature.*      # Feature engineering only
dbt run --select curated.*      # Curated models only
dbt run --select export.*       # Export to CSV only
```

#### Step 3: Verify outputs

```bash
# Check exported CSV files
ls -lh ../data/export/

# Expected outputs:
# - master_ads.csv
# - master_ads_11272025.csv
# - reciprocal_ads.csv
# - reciprocal_ads_11272025.csv
```

### Incremental Updates (Round 2 Data - 11/27/2025)

For processing updated Round 2 data only:

```python
# Stage Round 2 data
load_staging_table("Timeline_Metadata_11272025")
load_staging_table("Ads_Metadata_11272025")
```

```bash
# Run Round 2 models
dbt run --select "*_11272025"
```

## Important Procedures

### Data Validation

After running the pipeline, validate data quality:

```r
# Check for data consistency (from jar_modeling.rmd)
summary(master_df)
summary(reciprocal_df)

# Compare JAR vs ICORIA
jar_data %>%
  left_join(icoria_data, by = c("session_id", "pod_number", "ad_position")) %>%
  filter(round(ars_ads.x - ars_ads.y, 0) != 0)  # Should have minimal mismatches
```

### Winsorization Thresholds

Outlier clipping is set at 1st and 99th percentiles in SQL models. To adjust:

Edit `dbt_run/models/feature/ads_expanded.sql`:
```sql
quantile_cont(ars_ads::DOUBLE, 0.01)  -- Lower bound (change 0.01 to desired quantile)
quantile_cont(ars_ads::DOUBLE, 0.99)  -- Upper bound (change 0.99 to desired quantile)
```

### Program Context Window

The default 30-second window for program context can be modified:

Edit `dbt_run/models/curated/master_ads.sql`:
```sql
WHERE row_ranking <= 30  -- Change to desired number of seconds
```

## Database Schema

**DuckDB Database**: `data/galvanic.duckdb`

**Schemas**:
- `staging`: Raw data tables loaded from CSVs
  - `staging.session_metadata`
  - `staging.timeline_metadata`
  - `staging.timeline_metadata_11272025`
  - `staging.ads_metadata`
  - `staging.ads_metadata_11272025`

- `feature`: Feature engineering tables
  - `feature.ads_expanded`
  - `feature.ads_expanded_11272025`
  - `feature.program_metadata`
  - `feature.program_metadata_11272025`
  - `feature.vw_program_ranked` (view)
  - `feature.vw_program_ranked_11272025` (view)

- `curated`: Business logic tables
  - `curated.master_ads`
  - `curated.master_ads_11272025`
  - `curated.reciprocal_ads`
  - `curated.reciprocal_ads_11272025`

## Troubleshooting

### Common Issues

1. **DuckDB path errors**: Ensure `dbt_run/profiles.yml` points to correct database path (`../data/galvanic.duckdb`)

2. **CSV encoding issues**: If staging fails, check CSV file encoding (should be UTF-8)

3. **Schema mismatches**: Verify column counts in CSVs match Pydantic schemas in `src/schemas/source_data.py`

4. **dbt dependency errors**: Ensure models run in order (feature -> curated -> export)
   ```bash
   dbt run --select feature.*+  # Run feature models and downstream dependencies
   ```

5. **R package conflicts**: If lmerTest conflicts with lme4, reinstall:
   ```r
   remove.packages("lmerTest")
   install.packages("lmerTest")
   ```