# 📡 Network Operations & Predictive Intelligence System

An end-to-end telecom data engineering, machine learning, API, dashboard, and AI-assisted operations project built using the **Telecom Italia Milan Mobile Phone Activity dataset**.

The project transforms raw telecom activity data into a governed analytics platform that supports network activity monitoring, hotspot detection, anomaly analysis, predictive attention scoring, interactive visualization, and AI-assisted operational investigation.

---

## 🎯 Project Objective

Network operations teams need a reliable way to understand how communication activity changes across geographic areas and over time.

Raw telecom activity files alone are difficult to use operationally. They must first be validated, cleaned, aggregated, enriched, stored, exposed through APIs, and converted into useful operational signals.

This project builds that complete workflow:

**Raw Telecom Data → Data Engineering → Analytics Warehouse → Machine Learning → APIs → NOC Dashboard → AI-Assisted Insights**

The system focuses on **network activity intelligence**.

> **Important:** Activity volume is not the same as network congestion.  
> The available dataset does not contain capacity, utilization, latency, throughput, or packet-loss measurements. Therefore, model outputs are treated as **operational attention signals**, not confirmed network faults.

---

# 🏗️ System Architecture

```text
                 TELECOM ACTIVITY DATA
                         │
                         ▼
              ┌─────────────────────┐
              │    Landing Zone     │
              │   Daily CSV Files   │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │ Ingestion Validation│
              │ Schema / Quality    │
              │ Duplicate Checks    │
              └───────┬───────┬─────┘
                      │       │
                  Valid     Invalid
                      │       │
                      ▼       ▼
                ┌────────┐ ┌──────────┐
                │  Raw   │ │ Rejected │
                └───┬────┘ └──────────┘
                    │
                    ▼
          ┌───────────────────────┐
          │       PySpark ETL     │
          │                       │
          │ Clean                 │
          │ Standardize           │
          │ Aggregate             │
          │ Geo-Enrich            │
          └───────────┬───────────┘
                      │
             ┌────────┴─────────┐
             ▼                  ▼
      Processed Parquet   Analytics Parquet
             │                  │
             └────────┬─────────┘
                      ▼
          ┌───────────────────────┐
          │   Analytics Warehouse │
          │                       │
          │ dim_grid              │
          │ dim_time              │
          │ fact_network_activity │
          └───────────┬───────────┘
                      │
          ┌───────────┴────────────┐
          │                        │
          ▼                        ▼
   Feature Engineering       Anomaly Baseline
          │                        │
          ▼                        │
    ML Risk Model ◄────────────────┘
          │
          ▼
   Network Risk Scores
          │
          ▼
      ┌───────────┐
      │  FastAPI  │
      └─────┬─────┘
            │
      ┌─────┴───────────────┐
      │                     │
      ▼                     ▼
 React NOC Dashboard   AI Insight Layer
```

---

# 🔄 Production Pipeline

Apache Airflow orchestrates the complete processing workflow.

```text
ingest_validate_route
        │
        ▼
spark_process
        │
        ▼
load_warehouse
        │
        ▼
generate_features
        │
        ▼
score_risk
        │
        ▼
quality_check
        │
        ▼
notify
```

This makes the pipeline repeatable, auditable, failure-aware, and safe to rerun.

---

# 🧰 Technology Stack

| Layer | Technology |
|---|---|
| Programming | Python |
| Data Analysis | Pandas |
| Distributed Processing | PySpark |
| Orchestration | Apache Airflow |
| Data Storage | CSV / Parquet |
| Warehouse | SQLite |
| Backend API | FastAPI |
| API Validation | Pydantic |
| Frontend | React |
| Routing | React Router |
| Machine Learning | Scikit-learn |
| Model Persistence | Joblib |
| Mapping | Leaflet / OpenStreetMap |
| AI Layer | Claude API |
| Version Control | Git / GitHub |
| Environment | Windows 11 + WSL2 |

---

# 📊 Dataset

The project uses the **Telecom Italia Milan Mobile Phone Activity dataset**.

The activity data contains:

| Raw Column | Project Column | Description |
|---|---|---|
| `datetime` | `timestamp` | Hourly interval |
| `CellID` | `grid_id` | Milan geographic grid |
| `countrycode` | `country_code` | Country-code dimension |
| `smsin` | `sms_in` | Incoming SMS activity |
| `smsout` | `sms_out` | Outgoing SMS activity |
| `callin` | `call_in` | Incoming call activity |
| `callout` | `call_out` | Outgoing call activity |
| `internet` | `internet_activity` | Internet activity |

The project also uses:

```text
milano-grid.geojson
```

for geographic enrichment of the 10,000 Milan grid cells.

---

# ⚠️ Dataset Semantics

The activity fields are proportional **activity measurements**.

They should not be interpreted as:

```text
sms_in            ≠ exact SMS count
call_in           ≠ exact call count
internet_activity ≠ megabytes
grid_id           ≠ tower ID
```

A grid represents a geographic area of Milan.

---

# 🧱 Project Phases

The project was developed progressively across seven phases.

---

# Phase 1 — Python / Pandas

## Data Understanding & Reusable Utilities

Phase 1 establishes the business and data logic using Pandas before scaling the processing with Spark.

### NP1 — Dataset Profiling

The first step investigates:

- dataset shape
- data types
- timestamp cadence
- null values
- duplicate rows
- negative activity values
- dataset grain

The raw dataset grain is:

```text
timestamp + grid_id + country_code
```

Multiple records can therefore exist for the same grid and hour.

---

### NP2 — UsageProcessor

Reusable processing logic was implemented for:

```text
load_data()
clean_data()
derive_time_features()
aggregate_to_grid_time()
derive_activity_features()
compute_kpis()
export_summary()
```

The analytics grain becomes:

```text
timestamp + grid_id
```

The `country_code` dimension is aggregated away for the operational analytics path.

---

### NP3 — Rule-Based Activity Alerts

A baseline-based alert mechanism was implemented.

The current grid/hour is compared against historical activity while ensuring that the current observation does not contaminate its own baseline.

This creates the project's first operational attention signal.

---

# Phase 2 — PySpark

## Distributed Data Processing

Phase 2 scales the validated Pandas logic across all supplied Milan activity files.

---

### SP1 — Distributed Ingestion

PySpark loads all Milan daily files using an explicit schema.

The ingestion pattern is intentionally strict:

```text
sms-call-internet-mi-*.csv
```

This prevents accidental ingestion of other geographic datasets.

---

### SP2 — Cleaning & Standardization

Spark applies the same data-quality rules established during Phase 1.

Examples include:

```text
Missing grid_id      → Reject
Missing timestamp    → Reject
Negative activity    → Reject
Null activity value  → Handle as zero
```

Pandas and Spark results were compared to ensure the distributed implementation preserved the original logic.

---

### SP3 — Network Activity Aggregation

Raw records are aggregated from:

```text
timestamp + grid_id + country_code
```

to:

```text
timestamp + grid_id
```

This produces one operational analytics row for each grid and hour.

---

### SP4 — Geospatial Enrichment

The Milan grid GeoJSON is joined using:

```text
properties.cellId → grid_id
```

rather than the GeoJSON top-level ID.

Validation includes:

- row-count preservation
- unmatched-grid checks
- geographic centroid checks

---

### SP5 — Performance Optimization

Spark performance experiments included:

- execution-plan inspection
- caching
- partition tuning
- column pruning

Optimizations were retained only when measurements demonstrated an improvement.

---

### SP6 — Analytics Outputs

Processed data is stored primarily as Parquet.

Example outputs:

```text
data/processed/activity/
data/analytics/hourly_grid_summary/
```

The raw CSV data was approximately:

```text
631.6 MB
```

while the processed Parquet representation was approximately:

```text
165.1 MB
```

---

### SP7 — Reusable Spark ETL

The processing logic was consolidated into a reusable pipeline containing:

```text
read_raw()
clean()
aggregate()
enrich()
write_outputs()
main()
```

The job supports command-line configuration and fail-fast behavior.

---

# Phase 3 — Data Engineering

## Productionizing the Pipeline

Phase 3 converts the Spark processing workflow into a governed and orchestrated data platform.

---

## Data Zones

```text
data/
│
├── landing/
├── raw/
├── rejected/
├── reference/
├── processed/
├── analytics/
└── logs/
```

### Landing

Temporary arrival location for new files.

### Raw

Validated source files.

Raw data is treated as immutable.

### Rejected

Files failing validation are quarantined here with a documented reason.

### Reference

Contains static reference data such as:

```text
milano-grid.geojson
```

### Processed

Cleaned and transformed Parquet data.

### Analytics

Curated outputs used by downstream systems.

---

# Apache Airflow Orchestration

Airflow manages the production workflow.

The final DAG is:

```text
ingest_validate_route
        ↓
spark_process
        ↓
load_warehouse
        ↓
generate_features
        ↓
score_risk
        ↓
quality_check
        ↓
notify
```

The pipeline records operational information including:

```text
run_id
run_timestamp
task_status
rows_in
rows_rejected
nulls_handled
rows_published
AS_OF
```

---

# Reliability Controls

The pipeline handles different failures differently.

| Fault | Action |
|---|---|
| Missing daily file | Continue |
| Duplicate ingestion | Skip |
| Malformed timestamp | Reject |
| Negative activity | Reject |
| Missing required column | Reject |
| Partial corruption | Warn + Continue |
| Spark processing failure | Fail pipeline |

Safe reruns are designed to avoid creating duplicate warehouse records.

---

# 🗄️ Analytics Warehouse

The processed network activity is loaded into a star-schema warehouse.

```text
               dim_grid
                   │
                   │
                   ▼
          fact_network_activity
                   ▲
                   │
                   │
               dim_time
```

### `dim_grid`

Contains geographic information.

```text
grid_id
centroid_lon
centroid_lat
geometry_ref
```

### `dim_time`

Contains time dimensions.

```text
time_id
timestamp
date
hour
day_of_week
```

### `fact_network_activity`

Contains activity measures.

```text
fact_id
grid_id
time_id
sms_in
sms_out
call_in
call_out
internet_activity
total_activity
```

Geometry is intentionally not duplicated inside the fact table.

---

# Phase 4 — FastAPI

## Network Intelligence Service Layer

FastAPI exposes warehouse and model intelligence through stable REST contracts.

The API acts as the boundary between the data platform and downstream consumers.

---

## Main API Endpoints

### Network Summary

```http
GET /network/summary
```

Provides:

```text
total_activity
active_grids
peak_hour
top_grid
as_of
```

---

### Grid Activity

```http
GET /network/grid/{grid_id}
```

Returns the recent hourly activity history for a selected grid.

---

### Hotspots

```http
GET /network/hotspots
```

Ranks grids with elevated activity.

---

### Alerts

```http
GET /network/alerts
```

Returns rule-based activity alerts.

---

### Grid Features

```http
GET /network/grid/{grid_id}/features
```

Returns stored machine-learning features.

---

### Predictive Risk / Attention Score

```http
POST /network/predict-risk
```

Returns model-generated operational attention information.

Example fields:

```json
{
  "grid_id": 4821,
  "risk_score": 0.559,
  "risk_level": "medium",
  "model_version": "logreg-balanced-v1"
}
```

---

### Pipeline Status

```http
GET /pipeline/status
```

Provides the health of the latest data pipeline execution.

---

### Grid Location

```http
GET /network/grid/{grid_id}/location
```

Returns geographic information used by the dashboard map.

---

# ⏱️ AS_OF Convention

Because the project uses historical data, the application must not use the computer's current clock as the network reporting time.

Instead:

```text
AS_OF = maximum timestamp available in the analytics data
```

All layers use the same reporting-time convention.

```text
Warehouse
   ↓
FastAPI
   ↓
React
   ↓
ML / AI
```

This keeps every component synchronized.

---

# Phase 5 — React NOC Dashboard

The React application provides an operator-facing Network Operations Center interface.

Main pages include:

```text
Network Overview
Grid Explorer
Hotspots & Alerts
Predictive Risk
```

---

## Network Overview

Displays high-level operational metrics such as:

- total activity
- active grids
- peak hour
- top grid
- reporting timestamp

---

## Grid Explorer

Operators can search for a grid and inspect its recent activity.

Displayed fields include:

```text
timestamp
sms activity
call activity
internet activity
total activity
```

---

## Hotspots & Alerts

Displays:

- high-activity grids
- rule-based alerts
- Milan geographic map
- flagged grid polygons

Only operationally relevant grids are rendered rather than drawing all 10,000 grid polygons unnecessarily.

---

## Predictive Risk

Displays model-generated attention information.

The interface intentionally uses terms such as:

```text
Attention Score
Attention Level
```

rather than claiming:

```text
Confirmed Network Failure
```

The model output and narrative explanation are visually separated.

---

# Phase 6 — Machine Learning

## Lightweight Network Activity Intelligence

The machine-learning layer adds predictive activity intelligence.

The prediction problem is:

> Using information available through hour **t**, estimate whether a grid is likely to enter a high-activity condition at **t+1**.

---

# Leakage Prevention

The most important ML rule is:

```text
Features at time t
        │
        ▼
      MODEL
        │
        ▼
Prediction for t+1
```

Data from `t+1` must never appear inside features calculated for `t`.

---

# ML Features

Six primary features are generated.

| Feature | Meaning |
|---|---|
| `avg_activity` | Recent average activity |
| `activity_growth` | Change in activity level |
| `active_hours` | Number of active hours |
| `peak_ratio` | Peak relative to average |
| `variability` | Activity instability |
| `internet_share` | Internet proportion of activity |

---

# Model

The project uses a Logistic Regression classifier with class balancing.

The model artifact is versioned and persisted using Joblib.

Example version:

```text
logreg-balanced-v1
```

The model is deliberately lightweight because the project emphasizes:

- correct problem framing
- leakage prevention
- feature engineering
- evaluation
- explainability
- operationalization

rather than algorithm complexity.

---

# Batch Scoring

The production ML flow is:

```text
Warehouse
    │
    ▼
Feature Generation
    │
    ▼
grid_features
    │
    ▼
Risk Model
    │
    ▼
network_risk_scores
    │
    ▼
FastAPI
    │
    ▼
React Dashboard
```

Risk fields can also enrich hotspot and alert responses.

---

# Phase 7 — AI-Assisted Network Operations

The AI layer sits above the existing data, API, and ML layers.

Its responsibility is to **interpret evidence already produced by the platform**.

It does not replace Spark, SQL, ML, or the API.

```text
Data Engineering → computes facts
SQL/Warehouse    → stores facts
ML               → generates scores
FastAPI          → exposes evidence
Claude           → interprets evidence
Operator         → makes decisions
```

The AI layer is designed to distinguish:

1. **Observed activity**
2. **Predicted attention/risk signal**
3. **Confirmed network fault**

A confirmed fault is not claimed unless appropriate operational evidence exists.

---

# 📂 Repository Structure

```text
Network-operator/
│
├── data/
│   ├── landing/
│   ├── raw/
│   ├── rejected/
│   ├── reference/
│   ├── processed/
│   └── analytics/
│
├── phase1/
│   └── Pandas profiling and reusable processing
│
├── phase2/
│   └── PySpark distributed ETL
│
├── phase3/
│   ├── ingestion
│   ├── Airflow DAGs
│   └── warehouse/
│
├── phase4/
│   └── api/
│       └── app/
│           ├── main.py
│           ├── service.py
│           ├── schemas.py
│           └── db.py
│
├── phase5/
│   └── noc-dashboard/
│
├── phase6/
│   ├── feature engineering
│   ├── model training
│   ├── anomaly baseline
│   └── batch scoring
│
├── phase7/
│   └── AI-assisted network insights
│
├── requirements.txt
└── README.md
```

> The exact filenames inside individual phase folders may vary as the project evolves.

---

# 🚀 Running the Project

## 1. Clone the Repository

```bash
git clone https://github.com/Saisubramoni03/Network-operator.git
cd Network-operator
```

---

## 2. Run the Data Pipeline

The Spark processing layer should be executed before starting downstream services.

Ensure:

```text
Python
Java 17
PySpark
```

are correctly configured.

---

## 3. Start Airflow

The project uses Airflow through WSL.

Activate the Airflow environment and start the required Airflow services.

The main DAG is:

```text
de7_end_to_end_pipeline
```

The final workflow includes:

```text
ingest_validate_route
spark_process
load_warehouse
generate_features
score_risk
quality_check
notify
```

---

## 4. Start FastAPI

```bash
cd phase4/api
venv\Scripts\activate
python -m uvicorn app.main:app --reload
```

Swagger documentation is available at:

```text
http://127.0.0.1:8000/docs
```

---

## 5. Start the React Dashboard

Open another terminal:

```bash
cd phase5/noc-dashboard
npm install
npm start
```

Configure the frontend API base URL using:

```env
REACT_APP_API_BASE_URL=http://127.0.0.1:8000
```

---

# 🧪 Validation Strategy

Generated or automated results are not accepted without independent validation.

Different layers use different validation methods:

| Layer | Validation |
|---|---|
| Pandas | Hand calculations and unit tests |
| Spark | Counts, totals and grain checks |
| Geo enrichment | Coverage + geographic spot checks |
| Airflow | Task states and failure propagation |
| Warehouse | Independent SQL queries |
| FastAPI | API response vs SQL |
| React | Observed UI behavior |
| ML | Chronological evaluation + leakage tests |
| AI | Evidence-grounding tests |

---

# 📈 Key Engineering Results

The project demonstrated:

- processing of **15M+ raw activity records**
- approximately **1.68M grid-hour analytics records**
- geographic coverage across **10,000 Milan grid cells**
- Parquet storage reduction from approximately **631.6 MB to 165.1 MB**
- repeatable Airflow orchestration
- safe rerun / idempotency controls
- warehouse-backed REST APIs
- interactive NOC visualization
- leakage-tested feature engineering
- versioned ML model deployment
- batch network attention scoring
- AI-assisted evidence interpretation

---

# 🧠 Key Engineering Principles

### 1. Validate before trusting

A successful program execution does not guarantee correct data.

### 2. Grain is a contract

Every layer explicitly defines what one row represents.

### 3. Silent failures are dangerous

A geographically incorrect join can still produce 100% join coverage.

### 4. Measure before optimizing

Spark tuning decisions are based on measured execution behavior.

### 5. Pipelines must be safe to rerun

Repeated execution must not corrupt previously valid data.

### 6. Prevent ML leakage

Future information must never enter historical features.

### 7. APIs are contracts

Frontend and ML integrations should survive internal implementation changes.

### 8. AI explains evidence

The AI layer reasons over governed evidence rather than inventing operational facts.

---

# ⚠️ Current Limitations

The source dataset does not contain:

- network capacity
- bandwidth utilization
- throughput
- latency
- packet loss
- RF quality
- equipment alarms
- tower health
- confirmed incident labels

Therefore, this project detects and predicts **activity patterns**, not confirmed network congestion or infrastructure failure.

The machine-learning target is a training proxy and should be interpreted as an operational attention signal.

---

# 🔮 Future Improvements

Potential extensions include:

- real-time Kafka ingestion
- live network KPI integration
- actual network incident labels
- RF and capacity metrics
- PostgreSQL or cloud warehouse migration
- containerization with Docker
- CI/CD pipeline
- production monitoring
- model drift detection
- automated model retraining
- expanded AI tool integration
- MCP-based network intelligence tools
- multi-agent incident investigation

---

# 🏁 Conclusion

This project demonstrates the complete evolution of telecom data from raw files into operational intelligence.

```text
Raw Data
   ↓
Validated Data
   ↓
Distributed Processing
   ↓
Governed Warehouse
   ↓
Machine Learning
   ↓
REST APIs
   ↓
NOC Dashboard
   ↓
AI-Assisted Investigation
```

Rather than treating data engineering, machine learning, APIs, frontend development, and AI as separate exercises, the project integrates them into a single **Network Operations & Predictive Intelligence platform**.
