# Warehouse Capacity Planning with Snowflake ML

A logistics company operates 25 warehouses across EMEA, APAC, and the Americas. Each facility handles different goods — ambient, cold storage, hazmat — and serves a shifting roster of clients. Predicting space utilization 90 days ahead lets operations teams redistribute inventory before a warehouse fills up, negotiate new leases before costs spike, and avoid the lost revenue of turning clients away.

This demo builds that forecasting system end-to-end in a single Snowflake Notebook using **mlforecast** (Nixtla) with LightGBM, and walks through the full ML lifecycle: feature engineering, training, registration, monitoring, retraining after drift, containerized deployment, and real-time scoring.

---

## What the Notebook Covers

### 1 &mdash; Feature Store: Three Feature Views

Warehouse utilization depends on features that change at very different cadences. The Snowflake Feature Store manages all of them in one place:

| Feature View | Examples | Update Cadence |
|---|---|---|
| **Static** | Region, warehouse type (one-hot encoded) | Never changes |
| **Dynamic** | Total capacity (sqm), loading docks, staffing level | Changes on specific effective dates (SCD) |
| **Operational** | Inbound/outbound shipments, temperature, active clients | Daily |

The Dynamic Feature View uses `timestamp_col='EFFECTIVE_DATE'`, so the Feature Store automatically performs **point-in-time correct lookups** &mdash; when building a training dataset for January 15, it retrieves the capacity that was in effect on that date, not a future expansion. This prevents data leakage without any manual join logic.

### 2 &mdash; Immutable Training Datasets

`generate_dataset()` creates a versioned, immutable snapshot that joins all three Feature Views to the daily operations spine. This snapshot is stored in the Model Registry schema and provides full **lineage** &mdash; from raw tables through Feature Views to the exact rows the model trained on &mdash; making every experiment reproducible.

### 3 &mdash; Time-Series Forecasting with mlforecast

[mlforecast](https://github.com/Nixtla/mlforecast) handles the time-series feature engineering (lags, rolling statistics, date features) and recursive multi-step prediction. The Feature Store features flow in as exogenous regressors:

- **Static features** (region/type) &rarr; passed as `static_features`, replicated at every forecast step.
- **Dynamic + operational features** (capacity, shipments, temperature, ...) &rarr; passed via `X_df`, carrying forward the last known values into the 90-day forecast horizon.

Training and cross-validation are logged through **Experiment Tracking**, so V1 and V2 runs can be compared side-by-side.

### 4 &mdash; Model Registry with CustomModel

The Snowflake Model Registry's `CustomModel` interface lets you register **any** model type. Here, it wraps the fitted mlforecast pipeline in a class that:

- Accepts a combined DataFrame of historical rows (with actuals) and future stubs (with nulls).
- Splits history from future, builds `X_df`, and calls `fcst.predict()`.
- Returns forecast dates and predicted utilization.

The model is registered with `target_platforms=['WAREHOUSE', 'SNOWPARK_CONTAINER_SERVICES']` and uses `@partitioned_api` with `TABLE_FUNCTION` so each warehouse is scored independently in parallel.

### 5 &mdash; Production Alias and Lineage

Setting a `PRODUCTION` alias on a model version lets downstream consumers always reference the latest blessed model without changing code. The notebook queries **upstream lineage** to verify which Feature Views fed the production model.

### 6 &mdash; Model Monitoring and Feature Drift

After training V1, the demo simulates 90 days of new operational data with **deliberate distributional shifts**: inbound shipments increase, outbound drops, temperatures rise, and client counts grow across all warehouses. A 30-day ramp smoothly transitions from normal to drifted patterns.

A **Model Monitor** tracks prediction accuracy and feature drift (PSI) against a baseline. Because the V1 model was trained on pre-drift data, its forecasts degrade visibly on the drifted period.

### 7 &mdash; Retraining and Version Promotion

The notebook retrains on fresher data (including post-drift observations) to produce V2. Cross-validation metrics confirm V2 handles the new distribution. The `PRODUCTION` alias is moved from V1 to V2 &mdash; any service or query referencing `PRODUCTION` automatically picks up the retrained model.

A second Model Monitor is attached to V2, showing restored accuracy on the drifted data.

### 8 &mdash; SPCS Deployment and Real-Time Scoring

The retrained model is deployed as a containerized inference service on **Snowpark Container Services**. The final section demonstrates the full real-time workflow:

1. **Online feature retrieval** &mdash; the Feature Store returns the latest static, dynamic, and operational features for a target warehouse.
2. **History assembly** &mdash; recent daily observations are fetched to seed mlforecast's lag computations.
3. **SPCS inference** &mdash; the model service returns a 90-day utilization forecast, callable both through the Python SDK and a REST API with a programmatic access token.

---

## Snowflake ML Features Used

| Feature | Role in This Demo |
|---|---|
| **Feature Store** | Three Feature Views (static, dynamic with `timestamp_col`, operational) with point-in-time correct joins and online retrieval at inference time |
| **Immutable Datasets** | Versioned training snapshots with full lineage back to Feature Views |
| **Experiment Tracking** | Side-by-side comparison of V1 and V2 training runs |
| **Model Registry** | CustomModel registration, version management, production alias, and upstream lineage queries |
| **Model Monitoring** | Automated drift detection and accuracy tracking for both V1 and V2 |
| **Snowpark Container Services** | Low-latency REST endpoint for real-time 90-day forecasts |

---