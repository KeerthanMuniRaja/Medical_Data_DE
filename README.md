# Medical Data Engineering Project

## Project Overview

A medallion architecture (Bronze → Silver → Gold) data engineering pipeline built on Databricks, processing synthetic medical/healthcare data. The pipeline ingests raw CSV data from Azure Blob Storage, cleanses and enriches it through the silver layer, then models it into a star schema with dimension/fact tables and pre-computed KPI tables in the gold layer.

**Catalog:** `medical_data`  
**Source:** `workspace.azure_blob_storage` (external CSV files via Fivetran sync)  
**Storage Format:** Delta Lake  
**Compute:** Databricks Serverless  

---

## Project Structure

```
Medical_Data_DE/
├── 01_bronze/
│   └── raw/
│       └── nb_rawdata_ingestion          # Raw data ingestion from Azure Blob Storage
├── 02_silver/
│   └── core/
│       └── nb_core                       # Data cleansing & feature engineering
├── 03_gold/
│   ├── data_mart/
│   │   └── nb_tablecreation             # Dimension & fact table creation (star schema)
│   ├── kpi's/
│   │   └── nb_kpi                       # KPI table generation
│   └── data_cube/
│       └── nb_data_cube                 # Aggregated data cube for analytics
├── 04_orchestration/
│   └── orchestrating_file.yaml          # Databricks workflow DAG definition
└── README.md
```

---

## Pipeline Architecture

### Layer 1 — Bronze (Raw Ingestion)

**Notebook:** `01_bronze/raw/nb_rawdata_ingestion`  
**Purpose:** Ingest raw tables from `workspace.azure_blob_storage` into `medical_data.raw` as Delta tables with zero transformation.

**Source Tables Ingested:**

| Raw Table | Rows | Description |
| --- | --- | --- |
| `medical_data.raw.data_dictionary` | 65 | Field-level metadata for all source tables |
| `medical_data.raw.encounters` | 27,891 | Patient encounter/visit records |
| `medical_data.raw.patients` | 974 | Patient demographic data |
| `medical_data.raw.payers` | 10 | Insurance payer organizations |
| `medical_data.raw.procedures` | 47,701 | Medical procedures performed |
| `medical_data.raw.organizations` | 1 | Healthcare provider organizations |

### Layer 2 — Silver (Cleansed & Enriched)

**Notebook:** `02_silver/core/nb_core`  
**Purpose:** Data quality cleansing, type casting, null handling, deduplication, and feature engineering.

#### Transformations Applied

**`encounters_s`** (27,891 rows)
- Selected relevant columns, dropped `reasoncode`/`reasondescription`
- Cast `base_encounter_cost`, `total_claim_cost`, `payer_coverage` from string to DOUBLE (with comma removal)
- Filled nulls: numeric costs → 0, string fields → "NA"
- Deduplicated on `id`, filtered null IDs
- **Feature engineering:** Added `month`, `quarter` (from `stop` timestamp), `time_diff` (encounter duration in hours)

**`patients_s`** (974 rows)
- Parsed `birthdate` and `death_date` with multi-format date handling (`yyyy-MM-dd` / `dd-MM-yyyy`)
- Cast `zip_code` to string, `lat`/`lon` to double
- Standardized text: `gender`, `race`, `ethnicity`, `marital_status` → lowercase trimmed
- Cleaned names: removed digits from `first`, `last`, `maiden`
- Cleaned `county`: removed trailing "county" suffix
- Cleaned `birth_place`: removed duplicate leading words
- Filled nulls: `maiden`, `suffix`, `prefix` → "NA"
- Deduplicated on `id`, filtered null IDs

**`payers_s`** (10 rows)
- Selected `id`, `name`, `address`, `city`, `state_headquartered`, `zip`
- Filled nulls → "NA", cleaned `zip` (removed non-digits)
- Standardized: `name`/`city` → lowercase trimmed, `state_headquartered` → uppercase trimmed
- Deduplicated on `id`, filtered null IDs

**`procedures_s`** (43,378 rows)
- Selected relevant columns, dropped `reasoncode`/`reasondescription`
- Cast `base_cost` to DOUBLE (with comma removal)
- Filled nulls: `base_cost` → 0, strings → "NA"
- Deduplicated on `encounter_id` + `code`
- Filtered null `encounter_id` and `patient`
- **Feature engineering:** Added `year` and `half_yearly` (from `stop` timestamp)

### Layer 3 — Gold (Star Schema & Analytics)

#### Dimensional Model

**Notebook:** `03_gold/data_mart/nb_tablecreation`

| Table | Type | Rows | Key Columns |
| --- | --- | --- | --- |
| `dim_patient` | Dimension | 974 | `patient_id`, `gender`, `race`, `ethnicity`, `marital_status`, `city_name`, `state` |
| `dim_payer` | Dimension | 10 | `payer_id`, `name`, `city`, `state_headquartered` |
| `dim_procedure` | Dimension | 43,378 | `procedure_id` (generated), `encounter_id`, `code`, `description` |
| `dim_encounter` | Dimension | 27,891 | `encounter_id`, `encounter_class`, `code`, `description`, `organization` |
| `fact_encounters` | Fact | 56,599 | `encounter_id`, `patient_id`, `payer_id`, `procedure_id`, `start`, `stop`, `base_encounter_cost`, `total_claim_cost`, `payer_coverage`, `month`, `quarter`, `time_diff` |

**Star Schema Relationships:**
- `fact_encounters.patient_id` → `dim_patient.patient_id`
- `fact_encounters.payer_id` → `dim_payer.payer_id`
- `fact_encounters.procedure_id` → `dim_procedure.procedure_id`
- `fact_encounters.encounter_id` → `dim_encounter.encounter_id`

> **Note:** `fact_encounters` is built by joining `encounters_s` with `dim_procedure` on `encounter_id`, resulting in 56,599 rows (one row per encounter-procedure combination).

#### KPI Tables

**Notebook:** `03_gold/kpi's/nb_kpi`

| KPI Table | Rows | Description |
| --- | --- | --- |
| `kpi_encounter` | 24 | Encounter class distribution by quarter (ambulatory, outpatient, wellness, etc.) with percentages |
| `kpi_encounter_duration` | 24 | Encounter duration split (Above/Below 24 hours) by month with percentages |
| `kpi_zero_coverage` | 120 | Zero payer coverage analysis by month and payer, with coverage/non-coverage percentages |
| `kpi_top_procedures` | 10 | Top 10 most expensive procedures by half-year with average base cost |
| `kpi_avg_claim_cost` | 10 | Average claim cost per payer with encounter counts |
| `kpi_readmission` | 134 | Monthly readmission rates (readmission = return within 30 days of previous encounter) |

#### Data Cube

**Notebook:** `03_gold/data_cube/nb_data_cube`

| Table | Rows | Description |
| --- | --- | --- |
| `data_cube` | 4,532 | Pre-aggregated analytics cube joining encounters, procedures, and encounter classes |

**Cube Dimensions:** `year`, `quarter`, `month`, `half_year`, `payer_id`, `encounter_class`  
**Cube Measures:** `total_encounters`, `unique_patients`, `total_encounter_cost`, `total_procedures`, `total_procedure_cost`

---

## Workflow Orchestration

**File:** `04_orchestration/orchestrating_file.yaml`

The pipeline is orchestrated as a Databricks Workflow (DAB YAML) with the following task dependency graph:

```
Data_Ingestion (Bronze)
├──► Silver_encounter ──► dim_encounter ──┐
├──► silver_patient ───► dim_patient ────┤
├──► silver_payer ─────► dim_payer ──────┤
└──► silver_procedure ─► dim_procedure ──┤
                              │           │
                              ▼           │
                        fact_encounter ◄──┘
                              │
                              ▼
                            kpi
```

**Key Dependencies:**
- `fact_encounter` depends on both `dim_procedure` and `Silver_encounter` (needs procedure IDs to build the fact table)
- `kpi` depends on all dimension tables + fact table being complete
- Silver tasks run in parallel after ingestion
- Gold dimension tasks run in parallel after their respective silver tasks

---

## Data Source Details

- **Origin:** Synthea™ synthetic patient data (simulated healthcare records)
- **Ingestion:** Fivetran sync from Azure Blob Storage (CSV files) into `workspace.azure_blob_storage`
- **Patient Population:** 974 synthetic patients
- **Encounter Volume:** 27,891 encounters with 43,378 associated procedures
- **Payer Network:** 10 insurance payers (Medicare, Medicaid, Blue Cross Blue Shield, UnitedHealthcare, Aetna, Cigna, Humana, etc.)

---

## Key Design Decisions

1. **Medallion Architecture:** Three-layer separation ensures raw data preservation, standardized cleansing, and analytics-ready outputs
2. **Star Schema Modeling:** Dimension + fact table design enables flexible analytical queries with minimal joins
3. **Pre-computed KPIs:** Six dedicated KPI tables avoid repeated expensive aggregations at query time
4. **Data Cube:** Multi-dimensional aggregation on time + payer + encounter class for fast slice-and-dice analytics
5. **Delta Lake:** ACID transactions, schema enforcement, and time travel across all layers
6. **Parallel Orchestration:** Silver and gold dimension tasks run in parallel where dependencies allow, optimizing pipeline throughput
