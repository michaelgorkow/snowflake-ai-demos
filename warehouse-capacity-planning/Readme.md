# Warehouse Capacity Planning Demo

An end-to-end MLOps pipeline for warehouse space utilization forecasting using Snowflake's native ML platform and mlforecast (Nixtla) for time-series modeling. This README serves as a step-by-step guide for a coding agent to generate all code artifacts.

## Overview

Build a machine learning solution that forecasts warehouse capacity utilization to optimize storage allocation, prevent overcapacity, and reduce operational costs. The demo generates realistic synthetic data and walks through the entire ML lifecycle — from data exploration to production deployment with monitoring — all within a single Snowflake Notebook.

**Key design decisions:**
- **mlforecast** (Nixtla) handles all time-series feature engineering (lags, rolling windows, date features) and multi-step recursive forecasting. This is critical because mlforecast automatically generates time-series features during both training AND inference — no manual feature replication needed.
- **Snowflake Feature Store** manages non-time-series warehouse attributes via two Feature Views: (1) a **static Feature View** for immutable attributes (region, type) and (2) a **dynamic Feature View** with `timestamp_col` for time-varying attributes (capacity, loading docks, staffing level) that change over time as warehouses expand. Both serve as exogenous features for mlforecast.
- **Snowflake CustomModel** wraps the fitted mlforecast pipeline for registration in the Model Registry, enabling SPCS deployment and monitoring.

## Sample Use Case

**Scenario:** A logistics company operates a network of 25 warehouses across EMEA, APAC, and Americas. Each warehouse has different characteristics (ambient, cold storage, hazmat) and serves multiple clients. The goal is to forecast warehouse space utilization 7-28 days in advance to enable proactive capacity management.

**Data patterns to simulate:**
- 25 warehouses across multiple regions
- 2+ years of daily operational data (730+ rows per warehouse)
- Seasonal patterns: Q4 peaks, holiday surges, weekend dips
- Trend component: gradual growth reflecting business expansion
- Correlations: inbound/outbound volumes drive utilization changes
- Anomalies: sudden spikes from large client onboarding events
- Different utilization profiles per warehouse type (cold storage typically higher)

## Environment

- **Database:** `AI_DEMOS` (already exists)
- **Role:** `AI_DEVELOPER` (already exists)
- **Warehouse:** `AI_WH` (already exists)
- **Compute Pool:** `SYSTEM_COMPUTE_POOL_CPU` (already exists)
- **Schema:** `WAREHOUSE_OPTIMIZATION` (created by the notebook)
- **Package requirement:** `snowflake-ml-python >= 1.7.1`

## Project Structure

```
warehouse-capacity-planning-demo/
├── notebooks/
│   └── end-2-end-warehouse-capacity-planning.ipynb   # Main demo notebook (Snowflake Notebook)
├── src/
│   └── demo_functions/
│       ├── __init__.py
│       ├── data_generation.py                        # Synthetic data generation utilities
│       └── plotting.py                               # Plotly visualization helper functions
├── assets/
│   └── mlops-architecture.png                        # Architecture diagram
├── pyproject.toml                                    # Package dependencies
└── Readme.md
```

## Key Snowflake ML Features Demonstrated

| Feature | Description |
|---------|-------------|
| **Feature Store** | Two Feature Views: static attributes (region, type) + time-varying attributes with `timestamp_col` (capacity, docks, staffing); online retrieval at inference time |
| **Model Registry** | Version-controlled model storage via CustomModel wrapping mlforecast |
| **Experiment Tracking** | MLflow-style experiment logging with metrics comparison (V1 vs V2) |
| **Model Monitoring** | Automated drift detection (PSI) and performance tracking (RMSE, MAE) |
| **Model Retraining** | Retrain on fresher data after drift detection; version promotion V1→V2 |
| **SPCS Deployment** | Containerized model inference with REST API endpoints |
| **Online Scoring** | End-to-end: online feature retrieval → SPCS endpoint → real-time forecast |

---

## Detailed Implementation Guide

The following sections describe each code artifact to be generated and its exact implementation details.

---

### Artifact 1: `pyproject.toml`

Enables the `demo_functions` package to be installed in the notebook via `%pip install -e ../`.

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "demo_functions"
version = "1"
description = "Custom functions for warehouse capacity planning demo."
dependencies = [
    "pandas",
    "numpy",
    "snowflake-snowpark-python>=1.39.1",
    "plotly"
]

[tool.setuptools.packages.find]
where = ["src"]
```

---

### Artifact 2: `src/demo_functions/__init__.py`

Expose the public API of the package:

```python
from .data_generation import setup, generate_warehouse_data
from .plotting import plot_utilization_timeseries, plot_seasonal_patterns, plot_correlation_heatmap, plot_utilization_distribution, plot_feature_importance, plot_actual_vs_predicted, plot_forecast, plot_warehouse_attributes_timeline
```

---

### Artifact 3: `src/demo_functions/data_generation.py`

This module handles schema creation and synthetic data generation. It follows the same pattern as the predictive maintenance demo: a `setup()` function that creates schemas and generates all demo data, and a `generate_warehouse_data()` helper.

**Tables to create in `AI_DEMOS.WAREHOUSE_OPTIMIZATION`:**

#### Table 1: `WAREHOUSES` (static dimension table)

| Column | Type | Description |
|--------|------|-------------|
| `WAREHOUSE_ID` | VARCHAR | Unique identifier (e.g., `WH_001`) |
| `WAREHOUSE_NAME` | VARCHAR | Human-readable name |
| `CITY` | VARCHAR | City location |
| `COUNTRY` | VARCHAR | Country |
| `REGION` | VARCHAR | Operating region (`EMEA`, `APAC`, `Americas`) |
| `WAREHOUSE_TYPE` | VARCHAR | One of: `Ambient`, `Cold Storage`, `Hazmat` |
| `OPENING_DATE` | DATE | When the warehouse opened |

#### Table 2: `WAREHOUSE_ATTRIBUTES` (slowly changing dimension — time-varying)

This table stores warehouse physical attributes that change over time (e.g., capacity expansions, dock additions, staffing changes). Each row represents a new effective state. This is the data source for the **dynamic Feature View** with `timestamp_col='EFFECTIVE_DATE'`.

| Column | Type | Description |
|--------|------|-------------|
| `WAREHOUSE_ID` | VARCHAR | FK to `WAREHOUSES` |
| `EFFECTIVE_DATE` | DATE | Date this attribute configuration became effective |
| `TOTAL_CAPACITY_SQM` | NUMBER | Total capacity in square meters (5000-50000) |
| `NUM_LOADING_DOCKS` | NUMBER | Number of loading docks (5-30) |
| `STAFFING_LEVEL` | NUMBER | Number of warehouse staff (20-200) |

Each warehouse should have 2-4 attribute change events over the 2+ year period (e.g., capacity expansion at month 8, dock addition at month 14, staffing increase at month 20). The initial row per warehouse uses the warehouse's `OPENING_DATE` (or `start_date`) as the `EFFECTIVE_DATE`.

#### Table 3: `WAREHOUSE_OPERATIONS` (fact table, daily grain)

| Column | Type | Description |
|--------|------|-------------|
| `WAREHOUSE_ID` | VARCHAR | FK to `WAREHOUSES` |
| `DATE` | DATE | Observation date |
| `OCCUPIED_SQM` | FLOAT | Occupied space in square meters |
| `UTILIZATION_PCT` | FLOAT | **Target variable** = `OCCUPIED_SQM / TOTAL_CAPACITY_SQM` (using the capacity effective on that date) |
| `INBOUND_SHIPMENTS` | NUMBER | Number of inbound shipments that day |
| `OUTBOUND_SHIPMENTS` | NUMBER | Number of outbound shipments that day |
| `INBOUND_VOLUME_CBM` | FLOAT | Inbound volume in cubic meters |
| `OUTBOUND_VOLUME_CBM` | FLOAT | Outbound volume in cubic meters |
| `NUM_SKUS_STORED` | NUMBER | Unique SKUs currently in warehouse |
| `AVG_DWELL_TIME_DAYS` | FLOAT | Average goods dwell time in days |
| `NUM_ACTIVE_CLIENTS` | NUMBER | Active clients using the warehouse |
| `TEMPERATURE_C` | FLOAT | Average outside temperature (Celsius) |
| `IS_HOLIDAY` | BOOLEAN | Public holiday flag |
| `DAY_OF_WEEK` | NUMBER | 0=Monday through 6=Sunday |

**Function signatures:**

```python
def setup(session, schema='WAREHOUSE_OPTIMIZATION'):
    """
    Creates the schema (CREATE OR REPLACE SCHEMA) and generates all demo data.
    Called from the notebook during initialization.
    
    Steps:
    1. CREATE OR REPLACE SCHEMA AI_DEMOS.<schema>
    2. Generate warehouse dimension data (25 warehouses)
    3. Generate warehouse attributes SCD data (2-4 change events per warehouse)
    4. Generate daily operations data (2+ years per warehouse)
    5. Write all three tables to Snowflake via session.write_pandas()
    """

def generate_warehouse_data(session, schema, n_warehouses=25, start_date='2022-01-01'):
    """
    Generates and writes WAREHOUSES, WAREHOUSE_ATTRIBUTES, and WAREHOUSE_OPERATIONS tables.
    Called by setup(). Can also be called standalone to regenerate data.
    Returns tuple of (warehouses_df, attributes_df, operations_df) as pandas DataFrames.
    """
```

**Data generation requirements:**
- Use `numpy` and `pandas` for generation.
- Simulate 25 warehouses with realistic city/country distributions across EMEA (10), APAC (8), Americas (7).
- Generate 2+ years of daily data per warehouse (starting ~2022-01-01 through current date).
- Utilization must stay within 0.10-0.98 range (clamp if needed).
- Build in seasonal patterns: sinusoidal yearly cycle (peak in October-November), weekly cycle (dip on weekends).
- Build in trend: gradual 5-10% growth over the 2-year period.
- Cold storage warehouses should have higher baseline utilization (0.70-0.85) vs ambient (0.50-0.70).
- Inbound/outbound volumes should correlate with utilization changes (net inbound increases utilization).
- Include 3-5 random anomaly events per warehouse (sudden 10-15% utilization spikes lasting 5-20 days).
- For `WAREHOUSE_ATTRIBUTES`: generate 2-4 SCD rows per warehouse. The initial row uses `start_date` as `EFFECTIVE_DATE`. Subsequent rows simulate capacity expansions (+10-30%), dock additions (+2-5), or staffing changes (±10-20%) at random dates spread across the data period. The `UTILIZATION_PCT` in `WAREHOUSE_OPERATIONS` should be calculated against the capacity that was effective on that date (i.e., join attributes by `EFFECTIVE_DATE <= DATE` to get the correct capacity).
- Write all three tables to Snowflake via `session.write_pandas()` with `overwrite=True`.

**Drift injection for future data (critical for model monitor demo):**

When `generate_warehouse_data()` is called with `mode='append'` for the post-training period (`2024-07-01` onward), the generated data must include **deliberate distributional shifts** so the model monitor detects drift:
- **Structural shift for 3-5 warehouses**: Permanently increase baseline utilization by +15-20% starting from a random date after `2024-07-01` (simulating a large new client onboarding or capacity reduction). For example, `WH_003` jumps from ~60% to ~80% utilization.
- **Trend acceleration**: For all warehouses, increase the growth trend from the original 5-10% annual rate to 15-20% in the future period (simulating accelerating demand).
- **Seasonal pattern change**: Shift the seasonal peak from Q4 to Q3 for a subset of warehouses (simulating a market change).
- **Increased noise**: Double the random noise variance for the future period vs the training period.

These shifts ensure:
1. **Feature drift (PSI)**: The input feature distributions change significantly vs the baseline.
2. **Performance degradation (RMSE/MAE)**: The model trained on historical patterns will produce less accurate forecasts.
3. **Visible monitoring signal**: The model monitor dashboard will clearly show degradation over time.

---

### Artifact 4: `src/demo_functions/plotting.py`

Plotly visualization helper functions used in the notebook's EDA and evaluation sections.

**Functions to implement:**

```python
def plot_utilization_timeseries(df: pd.DataFrame, warehouse_ids: list[str] = None) -> go.Figure:
    """Line chart of UTILIZATION_PCT over time, one trace per warehouse."""

def plot_seasonal_patterns(df: pd.DataFrame) -> go.Figure:
    """Side-by-side box plots: utilization by month (left) and by day-of-week (right)."""

def plot_correlation_heatmap(df: pd.DataFrame) -> go.Figure:
    """Correlation heatmap between all numeric columns."""

def plot_utilization_distribution(df: pd.DataFrame) -> go.Figure:
    """Histogram/violin of utilization distribution grouped by REGION or WAREHOUSE_TYPE."""

def plot_feature_importance(feature_names: list[str], importances: list[float]) -> go.Figure:
    """Horizontal bar chart of feature importances."""

def plot_actual_vs_predicted(actual: pd.Series, predicted: pd.Series, dates: pd.Series = None) -> go.Figure:
    """Overlay line chart of actual vs predicted utilization."""

def plot_forecast(historical_df: pd.DataFrame, forecast_df: pd.DataFrame, warehouse_ids: list[str] = None) -> go.Figure:
    """Plot historical actuals and future forecast for selected warehouses."""

def plot_warehouse_attributes_timeline(df: pd.DataFrame, warehouse_ids: list[str] = None) -> go.Figure:
    """Step chart showing capacity/docks/staffing changes over time per warehouse."""
```

All functions return `plotly.graph_objects.Figure` objects.

---

### Artifact 5: Snowflake Notebook (`notebooks/end-2-end-warehouse-capacity-planning.ipynb`)

This is the main artifact. Create a single Snowflake Notebook that demonstrates the full pipeline. The notebook must be designed to run inside Snowsight. Use `from snowflake.snowpark.context import get_active_session` to get the session.

The notebook should be organized into clearly numbered markdown section headers. Below is the exact cell-by-cell specification.

---

#### Section 1: Imports

**Markdown cell:** `# Imports`

**Code cell: Package install**
```python
%pip install --q -e ../ ipywidgets mlforecast lightgbm
```

**Code cell: Imports and session**
```python
import pandas as pd
import numpy as np
from datetime import date

import lightgbm as lgb
from mlforecast import MLForecast
from mlforecast.lag_transforms import RollingMean, RollingStd, ExpandingMean
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session

from snowflake.ml.experiment import ExperimentTracking
from snowflake.ml.feature_store import FeatureStore, CreationMode, Entity, FeatureView
from snowflake.ml.registry import Registry
from snowflake.ml.model.custom_model import CustomModel, ModelContext
from snowflake.ml.model import model_signature
from snowflake.ml.monitoring.entities.model_monitor_config import ModelMonitorConfig, ModelMonitorSourceConfig

import demo_functions

session = get_active_session()

database = 'AI_DEMOS'
schema = 'WAREHOUSE_OPTIMIZATION'
warehouse = 'AI_WH'

session.use_database(database)

demo_functions.setup(session, schema)
```

The `demo_functions.setup()` call creates the schema and generates all synthetic data.

---

#### Section 2: Setup Feature Store and Model Registry

**Markdown cell:** `# 1 - Setup Feature Store and Model Registry`

**Code cell:**
```python
my_feature_store = FeatureStore(
    session=session,
    database=database,
    name=f"{schema}_FEATURE_STORE",
    default_warehouse=warehouse,
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
)

my_model_registry = Registry(
    session=session,
    database_name=database,
    schema_name=f'{schema}_MODEL_REGISTRY',
    options={'enable_monitoring': True}
)
```

**Note:** This creates two additional schemas: `WAREHOUSE_OPTIMIZATION_FEATURE_STORE` and `WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY`. The `setup()` function should also `CREATE OR REPLACE` these schemas.

---

#### Section 3: Explore Data

**Markdown cell:** `# 2 - Explore Data`

**Code cell: View raw data**
```python
warehouse_dim = session.table(f'{database}.{schema}.WAREHOUSES')
print('Warehouses (static):')
display(warehouse_dim.limit(5))

warehouse_attrs = session.table(f'{database}.{schema}.WAREHOUSE_ATTRIBUTES')
print('Warehouse Attributes (time-varying):')
display(warehouse_attrs.limit(10))

warehouse_ops = session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
print('Warehouse Operations:')
display(warehouse_ops.limit(5))
```

**Code cell: Daily aggregation / join for visualization**
```python
viz_df = (
    warehouse_ops
        .join(warehouse_dim, on='WAREHOUSE_ID', how='left')
        .order_by('WAREHOUSE_ID', 'DATE')
        .to_pandas()
)
viz_df.head()
```

**Code cell: Utilization time-series visualization (Plotly)**
```python
demo_functions.plot_utilization_timeseries(viz_df, warehouse_ids=['WH_001','WH_002','WH_003','WH_004','WH_005'])
```

**Code cell: Seasonal patterns**
```python
demo_functions.plot_seasonal_patterns(viz_df)
```

**Code cell: Correlation heatmap**
```python
demo_functions.plot_correlation_heatmap(viz_df)
```

**Code cell: Warehouse attribute changes over time**
```python
attrs_df = warehouse_attrs.to_pandas()
demo_functions.plot_warehouse_attributes_timeline(attrs_df, warehouse_ids=['WH_001','WH_002','WH_003'])
```

**Code cell: Utilization distribution by warehouse type**
```python
demo_functions.plot_utilization_distribution(viz_df)
```

---

#### Section 4: Register Warehouse Features in Feature Store

**Markdown cell:** `# 3 - Register Warehouse Features in Feature Store`

The Feature Store manages **non-time-series features** via two Feature Views:

1. **Static Feature View** (no `timestamp_col`): Immutable warehouse attributes — region and type. One row per warehouse. These serve as **static exogenous features** for mlforecast (replicated per time step).
2. **Dynamic Feature View** (with `timestamp_col='EFFECTIVE_DATE'`): Time-varying physical attributes — capacity, loading docks, staffing level. Multiple rows per warehouse (SCD-style). The Feature Store automatically performs **point-in-time correct lookups** — for each date in the spine, it retrieves the attribute values that were effective on that date.

**Code cell: Prepare static feature DataFrame (region, type)**
```python
static_features_df = session.table(f'{database}.{schema}.WAREHOUSES').select(
    'WAREHOUSE_ID',
    'REGION',
    'WAREHOUSE_TYPE',
)
```

**Code cell: One-hot encode categorical features**

mlforecast requires numeric features. Encode `REGION` and `WAREHOUSE_TYPE` as numeric columns.

```python
static_features_df = (
    static_features_df
    .with_column('IS_EMEA', F.iff(F.col('REGION') == 'EMEA', 1, 0))
    .with_column('IS_APAC', F.iff(F.col('REGION') == 'APAC', 1, 0))
    .with_column('IS_AMERICAS', F.iff(F.col('REGION') == 'Americas', 1, 0))
    .with_column('IS_COLD_STORAGE', F.iff(F.col('WAREHOUSE_TYPE') == 'Cold Storage', 1, 0))
    .with_column('IS_HAZMAT', F.iff(F.col('WAREHOUSE_TYPE') == 'Hazmat', 1, 0))
    .with_column('IS_AMBIENT', F.iff(F.col('WAREHOUSE_TYPE') == 'Ambient', 1, 0))
    .drop('REGION', 'WAREHOUSE_TYPE')
)

print('Static features (one row per warehouse):')
display(static_features_df.limit(5))
```

**Code cell: Prepare dynamic feature DataFrame (capacity, docks, staffing)**
```python
dynamic_features_df = session.table(f'{database}.{schema}.WAREHOUSE_ATTRIBUTES').select(
    'WAREHOUSE_ID',
    'EFFECTIVE_DATE',
    'TOTAL_CAPACITY_SQM',
    'NUM_LOADING_DOCKS',
    'STAFFING_LEVEL',
)

print('Dynamic features (multiple rows per warehouse, SCD-style):')
display(dynamic_features_df.order_by('WAREHOUSE_ID', 'EFFECTIVE_DATE').limit(10))
```

**Code cell: Register entity and both Feature Views**
```python
warehouse_entity = Entity(
    name="WAREHOUSE",
    join_keys=["WAREHOUSE_ID"],
    desc="Unique Warehouse ID"
)

my_feature_store.register_entity(warehouse_entity)

warehouse_static_fv = FeatureView(
    name='WAREHOUSE_STATIC_FEATURES',
    entities=[warehouse_entity],
    feature_df=static_features_df,
    refresh_freq='1 day',
    desc='Static warehouse attributes: region, type (one-hot encoded). Never change over time.'
)

warehouse_static_fv = my_feature_store.register_feature_view(
    feature_view=warehouse_static_fv,
    version='1',
    overwrite=True
)

warehouse_dynamic_fv = FeatureView(
    name='WAREHOUSE_DYNAMIC_FEATURES',
    entities=[warehouse_entity],
    feature_df=dynamic_features_df,
    timestamp_col='EFFECTIVE_DATE',
    refresh_freq='1 day',
    desc='Time-varying warehouse attributes: capacity, loading docks, staffing level. Uses timestamp_col for point-in-time correct lookups.'
)

warehouse_dynamic_fv = my_feature_store.register_feature_view(
    feature_view=warehouse_dynamic_fv,
    version='1',
    overwrite=True
)

print(f'Registered: {warehouse_static_fv.name} (static, no timestamp_col)')
print(f'Registered: {warehouse_dynamic_fv.name} (dynamic, timestamp_col=EFFECTIVE_DATE)')
```

**Notes:**
- **Static FV**: No `timestamp_col` — one row per warehouse, attributes never change. The Feature Store replicates these for every date in the spine.
- **Dynamic FV**: `timestamp_col='EFFECTIVE_DATE'` enables **point-in-time correct lookups**. When generating a dataset with `spine_timestamp_col='DATE'`, the Feature Store finds the most recent `EFFECTIVE_DATE <= DATE` for each warehouse, returning the attribute values that were in effect on that date. This prevents data leakage (e.g., using a future capacity expansion when training on historical data).
- Both Feature Views are backed by dynamic tables and refresh daily.

---

#### Section 5: Generate Training Dataset (Immutable Snapshot)

**Markdown cell:** `# 4 - Generate Training Dataset`

Use Snowflake's `generate_dataset()` to create an **immutable snapshot** of the training data. This ensures:
- **Reproducibility**: The exact data used for training is preserved and versioned.
- **Lineage tracking**: The Model Registry can trace which Dataset (and which Feature Views) were used to train each model version.

**Code cell: Create spine DataFrame**
```python
spine_df = (
    session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
    .filter(F.col('DATE') <= '2024-06-30')
    .select('WAREHOUSE_ID', 'DATE', 'UTILIZATION_PCT')
    .distinct()
)

print('Spine DataFrame:')
display(spine_df.limit(5))
```

**Code cell: Generate immutable Dataset via Feature Store**
```python
training_dataset = my_feature_store.generate_dataset(
    name=f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_DATASET_V1',
    spine_df=spine_df,
    features=[warehouse_static_fv, warehouse_dynamic_fv],
    spine_timestamp_col='DATE',
    spine_label_cols=['UTILIZATION_PCT'],
    desc='Immutable training dataset for warehouse capacity forecasting V1.'
)

training_dataset_sf = training_dataset.read.to_snowpark_dataframe()
print(f'Dataset created: {training_dataset_sf.count()} rows')
display(training_dataset_sf.limit(5).to_pandas())
```

**Notes:**
- `features=[warehouse_static_fv, warehouse_dynamic_fv]` joins **both** Feature Views to the spine.
- For the **static FV**: each spine row gets the same static features per warehouse (region, type flags).
- For the **dynamic FV**: `spine_timestamp_col='DATE'` + `timestamp_col='EFFECTIVE_DATE'` means the Feature Store performs a **point-in-time correct lookup** — it finds the most recent `EFFECTIVE_DATE <= DATE` per warehouse, returning the capacity/docks/staffing that were in effect on that date.
- `spine_label_cols=['UTILIZATION_PCT']` marks the target column.
- The Dataset is stored in the `_MODEL_REGISTRY` schema alongside models.
- This Dataset will be referenced when logging the model to establish lineage: Feature Views → Dataset → Model.

---

#### Section 6: Prepare Data for mlforecast

**Markdown cell:** `# 5 - Prepare Data for mlforecast`

Read the immutable Dataset back and convert to the format mlforecast expects: `unique_id`, `ds`, `y` plus feature columns. The Dataset contains features from **both** Feature Views:
- **Static features** (from static FV): `IS_EMEA`, `IS_APAC`, `IS_AMERICAS`, `IS_COLD_STORAGE`, `IS_HAZMAT`, `IS_AMBIENT` — these are constant per warehouse and will be passed as `static_features` to mlforecast.
- **Dynamic features** (from dynamic FV): `TOTAL_CAPACITY_SQM`, `NUM_LOADING_DOCKS`, `STAFFING_LEVEL` — these vary over time (point-in-time joined by the Feature Store) and are treated as regular columns in the training DataFrame.

**Code cell: Convert Dataset to mlforecast format**
```python
ops_df = training_dataset_sf.order_by('WAREHOUSE_ID', 'DATE').to_pandas()

static_feature_cols = ['IS_EMEA', 'IS_APAC', 'IS_AMERICAS', 'IS_COLD_STORAGE', 'IS_HAZMAT', 'IS_AMBIENT']
dynamic_feature_cols = ['TOTAL_CAPACITY_SQM', 'NUM_LOADING_DOCKS', 'STAFFING_LEVEL']
all_feature_cols = static_feature_cols + dynamic_feature_cols

ops_df = ops_df.rename(columns={
    'WAREHOUSE_ID': 'unique_id',
    'DATE': 'ds',
    'UTILIZATION_PCT': 'y'
})

ops_df['ds'] = pd.to_datetime(ops_df['ds'])

if 'EFFECTIVE_DATE' in ops_df.columns:
    ops_df = ops_df.drop(columns=['EFFECTIVE_DATE'])

print(f'Training data shape: {ops_df.shape}')
print(f'Date range: {ops_df["ds"].min()} to {ops_df["ds"].max()}')
print(f'Number of series: {ops_df["unique_id"].nunique()}')
print(f'Static feature columns: {static_feature_cols}')
print(f'Dynamic feature columns: {dynamic_feature_cols}')
display(ops_df.head())
```

**Notes:**
- The data comes from the immutable Dataset (not directly from the raw table), ensuring training reproducibility.
- mlforecast requires columns named `unique_id` (series identifier), `ds` (timestamp), and `y` (target).
- **Static features** (region/type flags) are constant per warehouse — mlforecast replicates them at each forecast step.
- **Dynamic features** (capacity/docks/staffing) change at specific dates — they are already correctly point-in-time joined by `generate_dataset()`. For mlforecast, they are treated as regular columns that happen to be known at each time step.
- `EFFECTIVE_DATE` is dropped since it's the timestamp_col from the dynamic FV (used only for the join, not as a model feature).

---

#### Section 7: Train mlforecast Model

**Markdown cell:** `# 6 - Train mlforecast Model`

mlforecast handles ALL time-series feature engineering automatically:
- **Lag features**: Past values of the target (e.g., lag 1, 7, 14, 28)
- **Lag transforms**: Rolling means, rolling standard deviations, expanding means applied to lags
- **Date features**: Day of week, month extracted from timestamps

These features are generated during `fit()` and automatically reproduced during `predict()` using recursive forecasting — no manual feature engineering needed for time-series patterns.

**Code cell: Configure and fit mlforecast**
```python
fcst = MLForecast(
    models=lgb.LGBMRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        random_state=42,
        verbosity=-1,
        n_jobs=-1,
    ),
    freq='D',
    lags=[1, 7, 14, 28],
    lag_transforms={
        1: [ExpandingMean()],
        7: [RollingMean(window_size=7), RollingStd(window_size=7)],
        14: [RollingMean(window_size=14), RollingStd(window_size=14)],
        28: [RollingMean(window_size=28)],
    },
    date_features=['dayofweek', 'month', 'quarter', 'week'],
    num_threads=-1,
)

fcst.fit(
    ops_df,
    id_col='unique_id',
    time_col='ds',
    target_col='y',
    static_features=static_feature_cols,
)

print('Model fitted successfully.')
print(f'Features used: {fcst.ts.features_order_}')
```

**Notes on mlforecast configuration:**
- `lags=[1, 7, 14, 28]`: Raw lag features — yesterday, last week, 2 weeks ago, 4 weeks ago.
- `lag_transforms`: Transformations applied to specific lags:
  - Lag 1: Expanding mean (cumulative average of the target)
  - Lag 7: 7-day rolling mean and std (weekly patterns)
  - Lag 14: 14-day rolling mean and std (biweekly patterns)
  - Lag 28: 28-day rolling mean (monthly pattern)
- `date_features`: Calendar features extracted from timestamps.
- `static_features=static_feature_cols`: Only the truly **static** columns (region/type flags) — these are replicated at each forecast step. The **dynamic** columns (`TOTAL_CAPACITY_SQM`, `NUM_LOADING_DOCKS`, `STAFFING_LEVEL`) are NOT listed as static_features because they change over time — they are treated as regular exogenous features that must be provided via `X_df` during `predict()`.
- mlforecast uses recursive multi-step forecasting: for h-step ahead predictions, it predicts step 1, uses that prediction to compute features for step 2, etc.

---

#### Section 8: Evaluate Model with Cross-Validation

**Markdown cell:** `# 7 - Evaluate Model`

**Code cell: Cross-validation**
```python
cv_results = fcst.cross_validation(
    df=ops_df,
    h=28,
    n_windows=3,
    step_size=28,
    id_col='unique_id',
    time_col='ds',
    target_col='y',
    static_features=static_feature_cols,
)

cv_results.head()
```

**Code cell: Compute metrics**
```python
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score

rmse = root_mean_squared_error(cv_results['y'], cv_results['LGBMRegressor'])
mae = mean_absolute_error(cv_results['y'], cv_results['LGBMRegressor'])
r2 = r2_score(cv_results['y'], cv_results['LGBMRegressor'])

metrics = {'rmse': float(rmse), 'mae': float(mae), 'r2': float(r2)}
print(f"Cross-Validation Results:")
print(f"  RMSE: {rmse:.4f}")
print(f"  MAE:  {mae:.4f}")
print(f"  R2:   {r2:.4f}")
```

**Code cell: Plot actual vs predicted**
```python
demo_functions.plot_actual_vs_predicted(
    actual=cv_results['y'],
    predicted=cv_results['LGBMRegressor'],
    dates=cv_results['ds']
)
```

---

#### Section 9: Experiment Tracking

**Markdown cell:** `# 8 - Experiment Tracking`

**Code cell: Log experiment**
```python
exp = ExperimentTracking(session=session)
exp.set_experiment('WAREHOUSE_CAPACITY_PLANNING')

params = {
    'model_type': 'LGBMRegressor',
    'n_estimators': 500,
    'max_depth': 6,
    'learning_rate': 0.05,
    'lags': '1,7,14,28',
    'forecast_horizon': 28,
    'cv_windows': 3,
}

with exp.start_run('lgbm_mlforecast_run_1'):
    exp.log_params(params)
    exp.log_metrics(metrics)
```

---

#### Section 10: Feature Importance

**Markdown cell:** `# 9 - Feature Importance`

**Code cell:**
```python
lgbm_model = fcst.models_['LGBMRegressor']
feature_names = fcst.ts.features_order_
importances = lgbm_model.feature_importances_

importance_df = pd.DataFrame({
    'feature': feature_names,
    'importance': importances
}).sort_values(by='importance', ascending=True)

demo_functions.plot_feature_importance(
    importance_df['feature'].tolist(),
    importance_df['importance'].tolist()
)
```

---

#### Section 11: Wrap mlforecast in CustomModel for Model Registry

**Markdown cell:** `# 10 - Register Model in Model Registry`

Since mlforecast is not a natively supported model type in Snowflake's Model Registry, we wrap it in a `CustomModel`. The custom model:
1. Receives the fitted `MLForecast` pipeline (with trained LightGBM model + time-series state)
2. Accepts a pandas DataFrame with `unique_id`, `ds`, `y`, static features (region/type), and dynamic features (capacity/docks/staffing) as input
3. For dynamic features during forecasting, uses the last known values (carried forward for the forecast horizon via `X_df`)
4. Returns a DataFrame with 28-day ahead forecasts

**Code cell: Define CustomModel wrapper**
```python
STATIC_FEATURE_COLS = ['IS_EMEA', 'IS_APAC', 'IS_AMERICAS', 'IS_COLD_STORAGE', 'IS_HAZMAT', 'IS_AMBIENT']
DYNAMIC_FEATURE_COLS = ['TOTAL_CAPACITY_SQM', 'NUM_LOADING_DOCKS', 'STAFFING_LEVEL']

class WarehouseCapacityModel(CustomModel):
    def __init__(self, context: ModelContext) -> None:
        super().__init__(context)
        import pickle
        with open(self.context['mlforecast_model'], 'rb') as f:
            self.fcst = pickle.load(f)

    @CustomModel.inference_api
    def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
        import pandas as pd
        input_df = input_df.copy()
        input_df['ds'] = pd.to_datetime(input_df['ds'])

        static_cols = [c for c in STATIC_FEATURE_COLS if c in input_df.columns]
        dynamic_cols = [c for c in DYNAMIC_FEATURE_COLS if c in input_df.columns]

        h = 28
        future_dates = pd.date_range(
            start=input_df['ds'].max() + pd.Timedelta(days=1),
            periods=h,
            freq='D'
        )
        
        X_df_rows = []
        for uid in input_df['unique_id'].unique():
            uid_data = input_df[input_df['unique_id'] == uid]
            last_dynamic = uid_data[dynamic_cols].iloc[-1]
            for d in future_dates:
                row = {'unique_id': uid, 'ds': d}
                row.update(last_dynamic.to_dict())
                X_df_rows.append(row)
        X_df = pd.DataFrame(X_df_rows)

        preds = self.fcst.predict(
            h=h,
            new_df=input_df,
            X_df=X_df,
            static_features=static_cols,
        )
        return preds
```

**Code cell: Save mlforecast pipeline and create custom model**
```python
import pickle
import tempfile
import os

model_path = os.path.join(tempfile.mkdtemp(), 'mlforecast_model.pkl')
with open(model_path, 'wb') as f:
    pickle.dump(fcst, f)

model_context = ModelContext(mlforecast_model=model_path)
custom_model = WarehouseCapacityModel(model_context)

sample_input = ops_df.tail(100)[['unique_id', 'ds', 'y'] + all_feature_cols].reset_index(drop=True)
test_output = custom_model.predict(sample_input)
print('Custom model test output:')
display(test_output.head())
```

**Code cell: Register in Model Registry**
```python
predict_signature = model_signature.infer_signature(
    input_data=sample_input,
    output_data=test_output
)

registered_model = my_model_registry.log_model(
    custom_model,
    model_name="WAREHOUSE_CAPACITY_MODEL",
    version_name='V1',
    metrics=metrics,
    comment="mlforecast LightGBM pipeline for 28-day warehouse utilization forecasting",
    conda_dependencies=['mlforecast', 'lightgbm', 'pandas', 'numpy'],
    signatures={'predict': predict_signature},
    options={"relax_version": True},
    target_platforms=['WAREHOUSE', 'SNOWPARK_CONTAINER_SERVICES'],
    sample_input_data=training_dataset_sf.select(sample_input.columns).limit(100),
)

print('Model registered successfully.')
```

**Notes:**
- The `MLForecast` object is serialized via pickle and passed through `ModelContext` as a file path — Snowflake handles the rest.
- `conda_dependencies` includes `mlforecast` and `lightgbm` since these are needed at inference time.
- The CustomModel's `predict()` method builds an `X_df` for the forecast horizon, carrying forward the last known dynamic feature values (capacity/docks/staffing). In a real scenario, if a planned capacity expansion is known, the `X_df` could encode future values.
- `static_features` tells mlforecast which columns to replicate (region/type flags).
- `target_platforms` includes both `WAREHOUSE` (SQL inference) and `SNOWPARK_CONTAINER_SERVICES` (SPCS REST endpoint).
- `sample_input_data` references the Snowpark DataFrame from the immutable Dataset, establishing **lineage**: Feature Views → Dataset → Model.

---

#### Section 12: Set Model to Production

**Markdown cell:** `# 11 - Set Model to Production`

**Code cell:**
```python
registered_model.set_alias('PRODUCTION')

production_model = my_model_registry.get_model('WAREHOUSE_CAPACITY_MODEL').version('PRODUCTION')
```

**Code cell: Query lineage**
```python
featureviews = production_model.lineage(direction='upstream')[0].lineage(domain_filter=['feature_view'], direction='upstream')
for featureview in featureviews:
    print(f'Feature View Name: {featureview.name}')
    print('Feature Names:')
    for feature in featureview.feature_names:
        print(f'  {feature}')
```

---

#### Section 13: Test Model

**Markdown cell:** `# 12 - Test Model`

**Code cell: Generate predictions using the registered model**
```python
input_snowpark_df = session.create_dataframe(sample_input)

baseline_predictions = production_model.run(input_snowpark_df, function_name='predict').cache_result()
print('Baseline predictions:')
display(baseline_predictions.limit(10))
```

---

#### Section 14: Model Monitoring

**Markdown cell:** `# 13 - Model Monitoring`

**Code cell: Prepare baseline and source tables**

The model monitor needs actual values alongside predictions. We create a flattened predictions table with a `TIMESTAMP` column, actual `UTILIZATION_PCT`, and predicted values.

```python
baseline_pd = baseline_predictions.to_pandas()

actuals_pd = ops_df[['unique_id', 'ds', 'y']].rename(columns={
    'unique_id': 'UNIQUE_ID',
    'ds': 'DS',
    'y': 'UTILIZATION_PCT'
})

baseline_pd = baseline_pd.rename(columns=str.upper)

monitor_df = baseline_pd.merge(
    actuals_pd, 
    left_on=['UNIQUE_ID', 'DS'], 
    right_on=['UNIQUE_ID', 'DS'], 
    how='left'
)

monitor_df['TIMESTAMP'] = pd.to_datetime(monitor_df['DS'])

monitor_snowpark = session.create_dataframe(monitor_df)

monitor_snowpark.write.save_as_table(
    f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_BASELINE_V1',
    mode='overwrite'
)
monitor_snowpark.write.save_as_table(
    f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_SOURCE_V1',
    mode='overwrite'
)
```

**Important note:** The exact column names in `baseline_pd` depend on what the CustomModel's `predict()` returns and what `production_model.run()` produces. The coding agent must inspect the actual columns in `baseline_predictions` and adjust the merge/rename logic accordingly to ensure these columns exist:
- A timestamp column (e.g., `TIMESTAMP` or `DS`)
- An ID column (e.g., `UNIQUE_ID`)
- A prediction column (e.g., `LGBMREGRESSOR` — the model output column name)
- An actual column (e.g., `UTILIZATION_PCT`)

**Code cell: Create model monitor**
```python
source_config = ModelMonitorSourceConfig(
    baseline=f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_BASELINE_V1',
    source=f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_SOURCE_V1',
    timestamp_column='TIMESTAMP',
    id_columns=['UNIQUE_ID'],
    prediction_score_columns=['LGBMREGRESSOR'],
    actual_score_columns=['UTILIZATION_PCT']
)

monitor_config = ModelMonitorConfig(
    model_version=production_model,
    model_function_name='predict',
    background_compute_warehouse_name=warehouse,
    refresh_interval='1 minute',
    aggregation_window='1 day'
)

model_monitor = my_model_registry.add_monitor(
    name=f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_MM_V1',
    source_config=source_config,
    model_monitor_config=monitor_config
)
```

**Notes:**
- `prediction_score_columns` value (`LGBMREGRESSOR`) matches the output column from mlforecast's predict which names predictions after the model class. Verify the actual column name.
- Uses the Python API (`ModelMonitorSourceConfig`, `ModelMonitorConfig`, `registry.add_monitor()`) — same pattern as the predictive maintenance demo.

---

#### Section 15: Simulate Future Data and Monitor

**Markdown cell:** `# 14 - Simulate Future Data and Model Predictions`

**Code cell: Generate additional data**
```python
demo_functions.generate_warehouse_data(
    session, schema,
    start_date='2024-07-01',
    end_date=date.today().isoformat(),
    mode='append'
)
```

**Code cell: Build new input for forecasting**
```python
future_spine = (
    session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
    .filter(F.col('DATE') > '2024-06-01')
    .select('WAREHOUSE_ID', 'DATE', 'UTILIZATION_PCT')
    .distinct()
)

future_with_features = my_feature_store.retrieve_feature_values(
    spine_df=future_spine,
    features=[warehouse_static_fv, warehouse_dynamic_fv],
    spine_timestamp_col='DATE',
).to_pandas()

future_ops_df = future_with_features.rename(columns={
    'WAREHOUSE_ID': 'unique_id',
    'DATE': 'ds',
    'UTILIZATION_PCT': 'y'
})
future_ops_df['ds'] = pd.to_datetime(future_ops_df['ds'])

if 'EFFECTIVE_DATE' in future_ops_df.columns:
    future_ops_df = future_ops_df.drop(columns=['EFFECTIVE_DATE'])

future_ops_df = future_ops_df.sort_values(['unique_id', 'ds']).reset_index(drop=True)

future_input_snowpark = session.create_dataframe(future_ops_df[['unique_id', 'ds', 'y'] + all_feature_cols])
```

**Code cell: Generate forecasts and append to monitor source**
```python
future_predictions = production_model.run(future_input_snowpark, function_name='predict').cache_result()

future_pred_pd = future_predictions.to_pandas()
future_pred_pd = future_pred_pd.rename(columns=str.upper)

future_actuals = (
    session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
    .select(
        F.col('WAREHOUSE_ID').alias('UNIQUE_ID'),
        F.col('DATE').alias('DS'),
        F.col('UTILIZATION_PCT')
    )
    .to_pandas()
)

future_monitor_df = future_pred_pd.merge(
    future_actuals,
    on=['UNIQUE_ID', 'DS'],
    how='left'
)
future_monitor_df['TIMESTAMP'] = pd.to_datetime(future_monitor_df['DS'])

future_monitor_snowpark = session.create_dataframe(future_monitor_df)
future_monitor_snowpark.write.save_as_table(
    f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_SOURCE_V1',
    mode='append',
    column_order='name'
)
```

**Code cell: Visualize forecast**
```python
demo_functions.plot_forecast(
    historical_df=ops_df.rename(columns={'unique_id': 'WAREHOUSE_ID', 'ds': 'DATE', 'y': 'UTILIZATION_PCT'}),
    forecast_df=future_pred_pd.rename(columns={'UNIQUE_ID': 'WAREHOUSE_ID', 'DS': 'DATE', 'LGBMREGRESSOR': 'FORECAST'}),
    warehouse_ids=['WH_001', 'WH_002', 'WH_003']
)
```

---

#### Section 16: Model Retraining on Fresher Data

**Markdown cell:** `# 15 - Retrain Model on Fresher Data`

After observing performance degradation from the model monitor (caused by the distributional shifts in the future data), we retrain the model on the full dataset including the new data. This demonstrates the retraining workflow to combat model drift.

**Code cell: Create V2 Dataset (immutable snapshot of full data)**
```python
spine_df_v2 = (
    session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
    .select('WAREHOUSE_ID', 'DATE', 'UTILIZATION_PCT')
    .distinct()
)

training_dataset_v2 = my_feature_store.generate_dataset(
    name=f'{database}.{schema}_MODEL_REGISTRY.WAREHOUSE_CAPACITY_DATASET_V2',
    spine_df=spine_df_v2,
    features=[warehouse_static_fv, warehouse_dynamic_fv],
    spine_timestamp_col='DATE',
    spine_label_cols=['UTILIZATION_PCT'],
    desc='Immutable training dataset V2 — includes post-drift data for retraining.'
)

training_dataset_v2_sf = training_dataset_v2.read.to_snowpark_dataframe()
print(f'V2 Dataset created: {training_dataset_v2_sf.count()} rows')
```

**Code cell: Build updated training data from V2 Dataset**
```python
all_ops_df = training_dataset_v2_sf.order_by('WAREHOUSE_ID', 'DATE').to_pandas()

all_ops_df = all_ops_df.rename(columns={
    'WAREHOUSE_ID': 'unique_id',
    'DATE': 'ds',
    'UTILIZATION_PCT': 'y'
})
all_ops_df['ds'] = pd.to_datetime(all_ops_df['ds'])

if 'EFFECTIVE_DATE' in all_ops_df.columns:
    all_ops_df = all_ops_df.drop(columns=['EFFECTIVE_DATE'])

print(f'Retrain data shape: {all_ops_df.shape}')
print(f'Date range: {all_ops_df["ds"].min()} to {all_ops_df["ds"].max()}')
```

**Code cell: Retrain mlforecast on full dataset**
```python
fcst_v2 = MLForecast(
    models=lgb.LGBMRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        random_state=42,
        verbosity=-1,
        n_jobs=-1,
    ),
    freq='D',
    lags=[1, 7, 14, 28],
    lag_transforms={
        1: [ExpandingMean()],
        7: [RollingMean(window_size=7), RollingStd(window_size=7)],
        14: [RollingMean(window_size=14), RollingStd(window_size=14)],
        28: [RollingMean(window_size=28)],
    },
    date_features=['dayofweek', 'month', 'quarter', 'week'],
    num_threads=-1,
)

fcst_v2.fit(
    all_ops_df,
    id_col='unique_id',
    time_col='ds',
    target_col='y',
    static_features=static_feature_cols,
)

print('V2 model fitted on full dataset.')
```

**Code cell: Evaluate V2 model**
```python
cv_results_v2 = fcst_v2.cross_validation(
    df=all_ops_df,
    h=28,
    n_windows=3,
    step_size=28,
    id_col='unique_id',
    time_col='ds',
    target_col='y',
    static_features=static_feature_cols,
)

rmse_v2 = root_mean_squared_error(cv_results_v2['y'], cv_results_v2['LGBMRegressor'])
mae_v2 = mean_absolute_error(cv_results_v2['y'], cv_results_v2['LGBMRegressor'])
r2_v2 = r2_score(cv_results_v2['y'], cv_results_v2['LGBMRegressor'])

metrics_v2 = {'rmse': float(rmse_v2), 'mae': float(mae_v2), 'r2': float(r2_v2)}
print(f'V1 Metrics: RMSE={metrics["rmse"]:.4f}, MAE={metrics["mae"]:.4f}, R2={metrics["r2"]:.4f}')
print(f'V2 Metrics: RMSE={rmse_v2:.4f}, MAE={mae_v2:.4f}, R2={r2_v2:.4f}')
print(f'\nImprovement: RMSE {((metrics["rmse"] - rmse_v2) / metrics["rmse"] * 100):.1f}%')
```

**Code cell: Log experiment for V2**
```python
with exp.start_run('lgbm_mlforecast_run_2_retrained'):
    exp.log_params({**params, 'retrained': True, 'data_end_date': date.today().isoformat()})
    exp.log_metrics(metrics_v2)
```

**Code cell: Register V2 and update PRODUCTION alias**
```python
model_path_v2 = os.path.join(tempfile.mkdtemp(), 'mlforecast_model_v2.pkl')
with open(model_path_v2, 'wb') as f:
    pickle.dump(fcst_v2, f)

model_context_v2 = ModelContext(mlforecast_model=model_path_v2)
custom_model_v2 = WarehouseCapacityModel(model_context_v2)

sample_input_v2 = all_ops_df.tail(100)[['unique_id', 'ds', 'y'] + all_feature_cols].reset_index(drop=True)
test_output_v2 = custom_model_v2.predict(sample_input_v2)

predict_signature_v2 = model_signature.infer_signature(
    input_data=sample_input_v2,
    output_data=test_output_v2
)

registered_model_v2 = my_model_registry.log_model(
    custom_model_v2,
    model_name="WAREHOUSE_CAPACITY_MODEL",
    version_name='V2',
    metrics=metrics_v2,
    comment="Retrained mlforecast LightGBM on full dataset including post-drift data",
    conda_dependencies=['mlforecast', 'lightgbm', 'pandas', 'numpy'],
    signatures={'predict': predict_signature_v2},
    options={"relax_version": True},
    target_platforms=['WAREHOUSE', 'SNOWPARK_CONTAINER_SERVICES'],
    sample_input_data=training_dataset_v2_sf.select(sample_input_v2.columns).limit(100),
)

registered_model.unset_alias('PRODUCTION')
registered_model_v2.set_alias('PRODUCTION')

production_model = my_model_registry.get_model('WAREHOUSE_CAPACITY_MODEL').version('PRODUCTION')
print(f'PRODUCTION alias now points to: {production_model.version_name}')
```

**Notes:**
- The retraining uses the exact same mlforecast configuration but trains on the full dataset (including the drifted future data).
- V2 metrics should be better than V1 on recent data because the model has learned the new distribution.
- The `PRODUCTION` alias is moved from V1 to V2 — any downstream consumers automatically use the updated model.
- Experiment tracking logs both runs for side-by-side comparison.

---

#### Section 17: Deploy Model as Inference Service in SPCS

**Markdown cell:** `# 16 - Deploy Model as Inference Service in SPCS`

**Code cell: Create service (using the retrained V2 model)**
```python
registered_model_v2.create_service(
    service_name="warehouse_capacity_prediction_service",
    service_compute_pool="SYSTEM_COMPUTE_POOL_CPU",
    ingress_enabled=True,
    gpu_requests=None
)
```

**Important notes:**
- Service creation takes **5-15 minutes**. The cell will block until complete.
- The compute pool `SYSTEM_COMPUTE_POOL_CPU` already exists.
- Do NOT use `block=False`; let the cell block naturally.

---

#### Section 18: Online Feature Retrieval and Realtime Scoring

**Markdown cell:** `# 17 - Online Feature Retrieval and Realtime Scoring`

This section demonstrates the full real-time inference workflow: retrieving features from the **online Feature Store** at request time, combining them with recent time-series history, and calling the **SPCS model endpoint** for a low-latency forecast. This is the pattern a production application would follow.

**Code cell: Retrieve online features from Feature Store**

In a real-time scenario, a request arrives with a `WAREHOUSE_ID`. The application retrieves the latest features from **both** Feature Views — static attributes (region, type) and dynamic attributes (current capacity, docks, staffing) — and fetches recent operational history.

```python
target_warehouse_id = 'WH_003'

online_spine = session.create_dataframe(
    [{'WAREHOUSE_ID': target_warehouse_id}]
)

online_static_features = my_feature_store.retrieve_feature_values(
    spine_df=online_spine,
    features=[warehouse_static_fv],
).to_pandas()

online_dynamic_features = my_feature_store.retrieve_feature_values(
    spine_df=online_spine,
    features=[warehouse_dynamic_fv],
).to_pandas()

if 'EFFECTIVE_DATE' in online_dynamic_features.columns:
    online_dynamic_features = online_dynamic_features.drop(columns=['EFFECTIVE_DATE'])

online_features = online_static_features.merge(online_dynamic_features, on='WAREHOUSE_ID', how='left')

print(f'Online features for {target_warehouse_id}:')
display(online_features)
```

**Code cell: Build inference input (recent history + online features)**
```python
recent_history = (
    session.table(f'{database}.{schema}.WAREHOUSE_OPERATIONS')
    .filter(F.col('WAREHOUSE_ID') == target_warehouse_id)
    .select('WAREHOUSE_ID', 'DATE', 'UTILIZATION_PCT')
    .order_by(F.col('DATE').desc())
    .limit(60)
    .to_pandas()
)

recent_history = recent_history.sort_values('DATE').reset_index(drop=True)
recent_history = recent_history.merge(online_features, on='WAREHOUSE_ID', how='left')
recent_history = recent_history.rename(columns={
    'WAREHOUSE_ID': 'unique_id',
    'DATE': 'ds',
    'UTILIZATION_PCT': 'y'
})
recent_history['ds'] = pd.to_datetime(recent_history['ds'])

print(f'Inference input: {recent_history.shape[0]} days of history for {target_warehouse_id}')
print(f'Date range: {recent_history["ds"].min()} to {recent_history["ds"].max()}')
display(recent_history.tail())
```

**Code cell: Call SPCS endpoint via Python SDK**
```python
inference_snowpark = session.create_dataframe(
    recent_history[['unique_id', 'ds', 'y'] + all_feature_cols]
)

realtime_forecast = registered_model_v2.run(
    inference_snowpark,
    function_name="predict",
    service_name="warehouse_capacity_prediction_service"
)

print(f'28-day forecast for {target_warehouse_id}:')
display(realtime_forecast)
```

**Code cell: Call SPCS endpoint via REST API**
```python
import requests
import json

endpoint = session.sql("SHOW ENDPOINTS IN SERVICE warehouse_capacity_prediction_service").collect()

pat_token = session.sql("ALTER USER ADD PROGRAMMATIC ACCESS TOKEN demo_token;").collect()
pat_token = pat_token[0]['token_secret']

URL = f"https://{endpoint[0]['ingress_url']}/predict"
headers = {"Authorization": f'Snowflake Token="{pat_token}"'}

rest_input = recent_history[['unique_id', 'ds', 'y'] + all_feature_cols].copy()
rest_input['ds'] = rest_input['ds'].astype(str)
payload_data = {'data': [[i] + row.tolist() for i, row in rest_input.iterrows()]}

print(f'Sending {len(payload_data["data"])} rows of history for {target_warehouse_id}...')
r = requests.post(URL, json=payload_data, headers=headers)

print('\n28-day Forecast Response:')
print(json.dumps(r.json(), indent=2))

_ = session.sql("ALTER USER REMOVE PROGRAMMATIC ACCESS TOKEN demo_token;").collect()
```

**Notes:**
- This demonstrates the **online Feature Store** pattern: `retrieve_feature_values()` is called at inference time to get the latest features from **both** Feature Views:
  - **Static FV**: Region and type flags (constant per warehouse).
  - **Dynamic FV**: Current capacity, docks, and staffing (latest effective values — the Feature Store returns the most recent `EFFECTIVE_DATE` row).
- The recent time-series history (last 60 days, more than max lag of 28) is fetched from the operations table — mlforecast needs this history to compute lag/rolling features during `predict()`.
- The CustomModel's `predict()` method builds `X_df` internally, carrying forward the dynamic feature values for the 28-day forecast horizon.
- The SPCS endpoint provides low-latency inference (sub-second after initial cold start).
- In a production app, this flow would be wrapped in a REST API handler or Streamlit app.

---

## Cleanup

```sql
USE ROLE AI_DEVELOPER;
USE DATABASE AI_DEMOS;

DROP SERVICE IF EXISTS WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY.WAREHOUSE_CAPACITY_PREDICTION_SERVICE;
DROP MODEL MONITOR IF EXISTS WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL_MM_V1;
DROP MODEL IF EXISTS WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY.WAREHOUSE_CAPACITY_MODEL;
DROP SCHEMA IF EXISTS WAREHOUSE_OPTIMIZATION CASCADE;
DROP SCHEMA IF EXISTS WAREHOUSE_OPTIMIZATION_FEATURE_STORE CASCADE;
DROP SCHEMA IF EXISTS WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY CASCADE;
```

---

## Appendix: Implementation Notes for Coding Agent

### Architecture: Dual Feature Engineering Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA LAYER                                  │
│  WAREHOUSE_OPERATIONS (daily time-series)                        │
│  WAREHOUSES (static dimension table)                             │
│  WAREHOUSE_ATTRIBUTES (time-varying SCD table)                   │
└─────────────┬──────────────────────────────────┬────────────────┘
              │                                  │
              ▼                                  ▼
┌─────────────────────────┐    ┌─────────────────────────────────┐
│   mlforecast (Nixtla)   │    │   Snowflake Feature Store       │
│                         │    │                                 │
│ • Lag features          │    │ Static FV (no timestamp_col):   │
│   (1, 7, 14, 28)       │    │   • IS_EMEA / IS_APAC / ...     │
│ • Rolling mean/std      │    │   • IS_COLD_STORAGE / IS_HAZMAT │
│   (7, 14, 28 windows)  │    │   • IS_AMBIENT                  │
│ • Expanding mean        │    │                                 │
│ • Date features         │    │ Dynamic FV (timestamp_col=      │
│   (dayofweek, month,    │    │   EFFECTIVE_DATE):              │
│    quarter, week)       │    │   • TOTAL_CAPACITY_SQM          │
│                         │    │   • NUM_LOADING_DOCKS           │
│ Auto-generated during   │    │   • STAFFING_LEVEL              │
│ fit() AND predict()     │    │                                 │
│                         │    │ Point-in-time correct lookups   │
│ Depend on past target   │    │ via generate_dataset() and      │
│ values → recursive      │    │ retrieve_feature_values()       │
└──────────┬──────────────┘    └──────────────┬──────────────────┘
           │                                  │
           │  static FV → static_features     │
           │  dynamic FV → X_df in predict()  │
           ◄──────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    mlforecast.fit() / .predict()                  │
│  Static features (region/type) replicated per time step          │
│  Dynamic features (capacity/docks/staffing) via X_df             │
│  Time-series features computed recursively for multi-step        │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Snowflake Model Registry                       │
│  CustomModel wrapping fitted MLForecast pipeline                 │
│  target_platforms: WAREHOUSE + SNOWPARK_CONTAINER_SERVICES       │
└─────────────────────────────────────────────────────────────────┘
```

### mlforecast Key Concepts

1. **MLForecast class**: Full pipeline encapsulating feature generation + model training.
   - `lags`: List of integers specifying which past target values to use as features.
   - `lag_transforms`: Dict mapping lag index → list of transformations (e.g., `RollingMean`, `RollingStd`, `ExpandingMean`).
   - `date_features`: List of date attributes extracted from timestamps (e.g., `'dayofweek'`, `'month'`).
   - `static_features`: Columns that don't change over time (passed to `fit()` and `predict()`).
   - `freq='D'`: Daily frequency.

2. **fit()**: Trains models on the data. Automatically creates all lag/rolling/date features.
   - `id_col='unique_id'`, `time_col='ds'`, `target_col='y'` — required column naming convention.
   - `static_features=<list>` — which columns are static (replicated during predict).

3. **predict(h=28)**: Generates h-step ahead forecasts using recursive prediction.
   - Time-series features are auto-computed at each step.
   - Static features are replicated.
   - `X_df`: Optional DataFrame with future values of dynamic exogenous features (e.g., planned capacity/docks/staffing for each future date). Required when the model was trained with non-static exogenous features.
   - Returns DataFrame with columns: `unique_id`, `ds`, `<ModelName>` (e.g., `LGBMRegressor`).

4. **cross_validation()**: Time-series cross-validation with multiple windows.
   - `n_windows`: Number of train/test splits.
   - `h`: Forecast horizon per window.
   - `step_size`: Gap between windows.

5. **Serialization**: `MLForecast` objects are picklable. Use `pickle.dump(fcst, f)` to serialize.

### CustomModel Pattern for mlforecast

Since Snowflake's Model Registry doesn't natively support `MLForecast`, we use `CustomModel`:

```python
from snowflake.ml.model.custom_model import CustomModel, ModelContext

class WarehouseCapacityModel(CustomModel):
    def __init__(self, context: ModelContext) -> None:
        super().__init__(context)
        import pickle
        with open(self.context['mlforecast_model'], 'rb') as f:
            self.fcst = pickle.load(f)

    @CustomModel.inference_api
    def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
        # input_df has: unique_id, ds, y, + static features + dynamic features
        # Build X_df for forecast horizon with last-known dynamic feature values
        ...
        preds = self.fcst.predict(h=28, new_df=input_df, X_df=X_df, static_features=static_cols)
        return preds
```

- `ModelContext(mlforecast_model='/path/to/model.pkl')` — pass pickle file path.
- The `predict()` method builds `X_df` (future dynamic feature values) and passes `static_features` (region/type flags).
- `conda_dependencies=['mlforecast', 'lightgbm', 'pandas', 'numpy']` — required at inference time.

### Package Dependencies

The notebook installs the local `demo_functions` package via `%pip install -e ../`. Additional packages required in the Snowflake Notebook environment:

- `snowflake-ml-python >= 1.7.1`
- `snowflake-snowpark-python >= 1.39.1`
- `mlforecast`
- `lightgbm`
- `scikit-learn`
- `pandas`
- `numpy`
- `plotly`
- `ipywidgets`

### Data Generation Strategy

The `setup()` function generates data only up to the initial training boundary (e.g., through `2024-06-30`). Section 14 of the notebook then calls `generate_warehouse_data()` with `mode='append'` to add "future" data (from `2024-07-01` to today), simulating new data arriving after the model was trained.

**The future data must include deliberate distributional shifts** to make the model monitor demo compelling:
- 3-5 warehouses get a permanent +15-20% utilization increase (large client onboarding)
- Trend growth rate doubles for all warehouses
- Seasonal peak shifts from Q4→Q3 for some warehouses
- Random noise variance doubles

This guarantees the model monitor will detect both feature drift (PSI) and performance degradation (RMSE/MAE), creating a clear visual signal in the monitoring dashboard.

### Schema Layout

| Schema | Purpose |
|--------|---------|
| `WAREHOUSE_OPTIMIZATION` | Raw data tables (`WAREHOUSES`, `WAREHOUSE_ATTRIBUTES`, `WAREHOUSE_OPERATIONS`) |
| `WAREHOUSE_OPTIMIZATION_FEATURE_STORE` | Feature Store objects (entity, static FV, dynamic FV with `timestamp_col` — backed by dynamic tables) |
| `WAREHOUSE_OPTIMIZATION_MODEL_REGISTRY` | Model Registry, datasets, baseline/source tables, model monitor |

### Error Handling

- If `mlforecast` import fails, ensure `%pip install mlforecast lightgbm` executed successfully.
- If `root_mean_squared_error` import fails, you are on `scikit-learn < 1.6`. Use `mean_squared_error(y_test, y_pred, squared=False)` instead.
- If `FeatureStore` constructor errors with `CreationMode`, ensure Enterprise Edition is active.
- The prediction column from `production_model.run()` is named after the model class (e.g., `LGBMREGRESSOR`). Inspect `baseline_predictions.columns` and adjust `ModelMonitorSourceConfig.prediction_score_columns` accordingly.
- If `fcst.predict()` fails with `new_df`, ensure the DataFrame has at least `max(lags)` rows of history per `unique_id`.

### API Method Reference

| API | Method | Key Parameters |
|-----|--------|---------------|
| `FeatureStore` | `__init__(session, database, name, default_warehouse, creation_mode)` | `name` = schema name, `creation_mode=CreationMode.CREATE_IF_NOT_EXIST` |
| `FeatureStore` | `register_entity(entity)` | Entity with `name` and `join_keys` |
| `FeatureStore` | `register_feature_view(feature_view, version, overwrite)` | `overwrite=True` to replace existing |
| `FeatureStore` | `generate_dataset(name, spine_df, features, ...)` | Materializes immutable dataset for lineage tracking |
| `FeatureStore` | `retrieve_feature_values(spine_df, features, ...)` | For inference-time (online) feature retrieval |
| `FeatureStore` | `refresh_feature_view(fv)` | Force manual refresh of feature view |
| `FeatureView` | `__init__(name, entities, feature_df, timestamp_col, ...)` | `timestamp_col` enables point-in-time correct lookups for time-varying features |
| `ExperimentTracking` | `__init__(session)` | Uses session's current db/schema |
| `ExperimentTracking` | `start_run(name)` | Use as context manager |
| `Registry` | `__init__(session, database_name, schema_name, options)` | `options={'enable_monitoring': True}` |
| `Registry` | `log_model(model, model_name, version_name, ...)` | `target_platforms=['WAREHOUSE', 'SNOWPARK_CONTAINER_SERVICES']` |
| `Registry` | `add_monitor(name, source_config, model_monitor_config)` | Python API for model monitor creation |
| `ModelVersion` | `set_alias(alias)` / `unset_alias(alias)` | Set/unset production alias |
| `ModelVersion` | `create_service(service_name, service_compute_pool, ...)` | `ingress_enabled=True` for HTTP access |
| `ModelVersion` | `run(data, function_name, service_name)` | `service_name` routes to SPCS |
| `ModelVersion` | `lineage(direction)` | Trace upstream feature views |
| `CustomModel` | `__init__(context: ModelContext)` | Load serialized artifacts from context |
| `CustomModel` | `@inference_api predict(input_df)` | Define the inference method |
| `ModelContext` | `__init__(**kwargs)` | Pass file paths or supported model objects |

### Workflow Sequence

```
demo_functions.setup()
        |
        v
[Schema + Data Generation] --> WAREHOUSES + WAREHOUSE_ATTRIBUTES + WAREHOUSE_OPERATIONS tables (up to training boundary)
        |
        v
[Feature Store + Model Registry init]
        |
        v
[EDA] --> Plotly visualizations (incl. attribute change timeline)
        |
        v
[Feature Store] --> Entity + Static FV (region/type, no timestamp_col) + Dynamic FV (capacity/docks/staffing, timestamp_col=EFFECTIVE_DATE)
        |
        v
[Generate Dataset] --> Immutable Snowflake Dataset (both FVs point-in-time joined to spine) for lineage
        |
        v
[Prepare Training Data] --> Read Dataset → convert to mlforecast format (unique_id, ds, y + static + dynamic features)
        |
        v
[mlforecast.fit()] --> Time-series features auto-generated (lags, rolling, date) + static features + dynamic features as exogenous
        |
        v
[Cross-Validation] --> mlforecast.cross_validation() → RMSE, MAE, R2
        |
        v
[Experiment Tracking] --> Log params + metrics
        |
        v
[CustomModel Wrapper] --> Pickle mlforecast → ModelContext → WarehouseCapacityModel (builds X_df for dynamic features)
        |
        v
[Model Registry] --> log_model(CustomModel, sample_input_data=Dataset) → Production alias → Lineage: Feature Views → Dataset → Model
        |
        v
[Test Model] --> production_model.run() → Baseline predictions
        |
        v
[Model Monitor] --> Baseline + Source tables → Monitor via Python API
        |
        v
[Simulate Future Data] --> Append drifted data → retrieve_feature_values(both FVs) → Score → Append to monitor source → Observe drift
        |
        v
[Retrain Model] --> Generate V2 Dataset (both FVs) → mlforecast V2 on full dataset → CV metrics → Compare V1 vs V2
        |
        v
[Register V2] --> log_model(V2, sample_input_data=Dataset_V2) → Move PRODUCTION alias V1→V2 → Lineage preserved
        |
        v
[SPCS Deployment] --> Real-time inference service (V2 model)
        |
        v
[Online Feature Retrieval + Realtime Scoring] --> retrieve_feature_values(both FVs) → Recent history → SPCS endpoint → 28-day forecast
```
