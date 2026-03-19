# 🏥 Hospital Healthcare Operations Data Platform
### Built on Snowflake · Medallion Architecture · Team Data Mavericks

---

## 👥 Team

| Name | Contribution |
|---|---|
| Manubolu Rakesh | Bronze layer, Snowpipe, Storage Integration, Tag Based Masking, Row Access policy |
| Challagulla Venkat | Silver layer, DQ procedures, FK design |
| Seelam Surya Prakash Reddy | Gold layer, Dynamic Tables, KPI views |
| Gosu Lavanya | SCD-2 dimensions, Anomaly & Rules Table |

---

## 📌 What We Built

A fully automated **end-to-end healthcare data platform** on Snowflake for **MediCore Health**, a multi-specialty hospital network.

The platform ingests data from 5 operational domains out of AWS S3, validates and cleans it through three data layers, tracks full change history using SCD-2, and delivers 5 business KPIs ready for BI dashboards — all automated with zero manual intervention.

---

## 🎯 Business Problems Solved

| Business Problem | Our Solution |
|---|---|
| Bed allocation inefficiency | KPI 1: Bed Occupancy & Utilization Score |
| Unknown hospital throughput | KPI 2: Admission Turnaround Time (ATT) |
| Surgery delays and OT overruns | KPI 3: Surgery Efficiency Index |
| Doctor overload and staffing gaps | KPI 4: Doctor Workload Index |
| Billing fraud and revenue leakage | KPI 5: Billing Accuracy Index |
| PII data exposure | Dynamic Masking on email, phone, insurance_id |
| No audit trail | Exception table + SCD-2 history |

---

## 📂 Source Data (AWS S3)

**S3 Bucket:** `s3://hospital-health-care/`  
**File Format:** CSV with optional double-quote enclosure, header row skipped

Each domain has one folder in S3 and delivers two files:

| Domain | S3 Folder | Files | Primary Key |
|---|---|---|---|
| Patients | `/patients/` | patients_master.csv, patients_inc.csv | patient_id |
| Doctors | `/doctors/` | doctors_master.csv, doctors_inc.csv | doctor_id |
| Admissions | `/admissions/` | admissions_master.csv, admissions_inc.csv | admission_id |
| Procedures | `/procedures/` | procedures_master.csv, procedures_inc.csv | procedure_id |
| Billing | `/billing/` | billing_master.csv, billing_inc.csv | bill_id |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    AWS S3 Bucket                         │
│   /patients/  /doctors/  /admissions/  /procedures/      │
│   /billing/                                              │
└──────────────┬───────────────────────────────────────────┘
               │  S3 Event Notification (SQS) triggers pipe
               ▼
┌──────────────────────────────────────────────────────────┐
│  BRONZE LAYER  —  health_care.bronze                     │
│                                                          │
│  raw_patients     + stream raw_patients_st               │
│  raw_doctors      + stream raw_doctors_st                │
│  raw_admissions   + stream raw_admissions_st             │
│  raw_procedures   + stream raw_procedures_st             │
│  raw_billing      + stream raw_billing_st                │
│                                                          │
│  5 Snowpipes (AUTO_INGEST = TRUE)                        │
│  All columns STRING — no casting at landing              │
└──────────────┬───────────────────────────────────────────┘
               │  Task every 5 min → CALL run_full_pipeline()
               │  Streams → Temp tables → DQ → MERGE
               ▼
┌──────────────────────────────────────────────────────────┐
│  SILVER LAYER  —  health_care.silver                     │
│                                                          │
│  silver_patients   (INT PK)                              │
│  silver_doctors    (INT PK)                              │
│  silver_admissions (INT PK, FK → patients, doctors)      │
│  silver_procedures (INT PK, FK → admissions)             │
│  silver_billing    (INT PK, FK → admissions)             │
│  exception_table   (DQ failure log)                      │
│                                                          │
│  5 stored procedures + 1 master pipeline procedure       │
│  Dynamic masking on PII columns (email, phone, ins_id)   │
└──────────────┬───────────────────────────────────────────┘
               │  SCD-2 procedures + Dynamic Tables
               ▼
┌──────────────────────────────────────────────────────────┐
│  GOLD LAYER  —  health_care.gold                         │
│                                                          │
│  dim_patient   (SCD-2: effective dates + current_flag)   │
│  dim_doctor    (SCD-2: effective dates + current_flag)   │
│  fact_admissions   (Dynamic Table, TARGET_LAG 5 min)     │
│  fact_procedures   (Dynamic Table, TARGET_LAG 5 min)     │
│  fact_billing      (Dynamic Table, TARGET_LAG 5 min)     │
│  bed_occ view  │  att view  │  surgey_efficiny view      │
│  kpi_doctor_workload view  │  kpi_data_quality view      │
└──────────────────────────────────────────────────────────┘
```

---

## 🔹 Bronze Layer — Raw Ingestion

**Key components:** Storage Integration · External Stages · Raw Tables · Streams · Snowpipes

### How Snowpipe works in this project
1. A new CSV file lands in an S3 folder (e.g. `/patients/`)
2. S3 sends an SQS event notification to Snowflake automatically
3. The corresponding Snowpipe (`patients_pipe`) triggers and runs `COPY INTO`
4. Data lands in the raw table (`raw_patients`) as STRING columns
5. The stream (`raw_patients_st`) captures the new rows for Silver to consume

### Why all columns are STRING in Bronze
Source data cannot be trusted at the point of landing. Casting at this stage would silently drop rows with bad dates or mistyped numbers. Instead, all casting and validation happens in Silver where failures are explicitly logged.

### Streams — Change Data Capture
Each raw table has a Snowflake Stream. The stream records every INSERT from Snowpipe. Silver procedures read `METADATA$ACTION = 'INSERT'` from the stream to process only new rows, ensuring no row is processed twice.

---

## 🔹 Silver Layer — Validated & Cleaned

**Key components:** Stored Procedures · MERGE with ROW_NUMBER dedup · Exception Table · FK constraints · Dynamic Masking

### Loading Order (strictly enforced)
```
1. load_patients()    ← no FK dependencies
2. load_doctors()     ← no FK dependencies
3. load_admissions()  ← FK: patient_id + attending_doctor_id
4. load_procedures()  ← FK: admission_id
5. load_billing()     ← FK: admission_id
```
Breaking this order causes BUSINESS_ERROR failures because FK lookups return empty.

### What each procedure does (same 4-step pattern)

```
Stream
  ↓  Step 1: CREATE TEMP TABLE — land stream rows (raw strings)
  ↓  Step 2: DATA_ERROR check — log bad types to exception_table
  ↓  Step 3: CREATE valid TEMP TABLE — filter only clean rows
  ↓  Step 4: BUSINESS_ERROR check — FK violations + temporal rules
  ↓  Step 5: MERGE with ROW_NUMBER dedup → silver table
```

### Data Quality Rules

| Domain | DATA_ERROR checks | BUSINESS_ERROR checks |
|---|---|---|
| Patients | patient_id numeric, dob valid date, email has @ | — |
| Doctors | doctor_id numeric, join_date valid date, email has @ | — |
| Admissions | admission_id numeric, timestamps parseable | patient_id ∈ silver_patients, doctor_id ∈ silver_doctors, discharge ≥ admission |
| Procedures | procedure_id numeric | admission_id ∈ silver_admissions, end_time ≥ start_time |
| Billing | bill_id numeric, total_amount numeric | admission_id ∈ silver_admissions, total_amount ≥ 0 |

### Exception Table
```sql
-- see all failures
SELECT * FROM silver.exception_table;

-- failures by domain
SELECT source_table, error_type, COUNT(*) AS failures
FROM silver.exception_table
GROUP BY source_table, error_type
ORDER BY failures DESC;
```

### Foreign Key Structure
```
silver_admissions.patient_id          → silver_patients.patient_id
silver_admissions.attending_doctor_id → silver_doctors.doctor_id
silver_procedures.admission_id        → silver_admissions.admission_id
silver_billing.admission_id           → silver_admissions.admission_id
```

### Dynamic Data Masking (PII)

Tag-based masking policy applied to sensitive columns:

| Table | Masked Columns |
|---|---|
| silver_patients | email, phone, insurance_id |
| silver_doctors | email, phone |

```
ACCOUNTADMIN / SYSADMIN  →  real value shown
All other roles           →  ****MASKED****
```

---

## 🔹 Gold Layer — Analytics Ready

**Key components:** SCD-2 dimension procedures · Dynamic Tables · KPI Views

### SCD-2 Dimensions — dim_patient & dim_doctor

Gold dimensions use **Slowly Changing Dimension Type 2** with the expire + insert pattern:

**On each pipeline run:**
1. All rows with `current_flag = 'Y'` get expired → `effective_end_date = yesterday`, `current_flag = 'N'`
2. Fresh rows from silver are inserted → `effective_start_date = today`, `effective_end_date = NULL`, `current_flag = 'Y'`

**Example — patient who changed city:**

| patient_id | city | current_flag | effective_start | effective_end |
|---|---|---|---|---|
| 101 | Hyderabad | N | 2025-01-01 | 2026-03-17 |
| 101 | Vijayawada | Y | 2026-03-18 | NULL |

```sql
-- current patients only
SELECT * FROM gold.dim_patient WHERE current_flag = 'Y';

-- full history for one patient
SELECT * FROM gold.dim_patient WHERE patient_id = 101 ORDER BY effective_start_date;
```

### Dynamic Fact Tables

`fact_admissions`, `fact_procedures`, `fact_billing` are **Snowflake Dynamic Tables** with `TARGET_LAG = '5 minutes'`. Snowflake automatically refreshes them — no task or stored procedure needed. They always reflect the latest data from silver.

---

## 📊 KPIs — 5 Business Metrics

All implemented as SQL **Views** on top of Gold Dynamic Tables. Always up to date.

| # | KPI | View Name | Formula |
|---|---|---|---|
| 1 | Bed Occupancy & Utilization Score | `bed_occ` | Occupied hours / (distinct beds × 24) |
| 2 | Admission Turnaround Time | `att` | AVG(discharge_time − admission_time) in hours |
| 3 | Surgery Efficiency Index | `surgey_efficiny` | % surgeries starting within ±10 min of schedule |
| 4 | Doctor Workload Index | `kpi_doctor_workload` | AVG daily (admissions + procedures) per doctor |
| 5 | Billing Accuracy Index | `kpi_data_quality` | 1 − (anomaly bills / total bills) |

```sql
-- run all 5 KPIs
SELECT * FROM gold.bed_occ;
SELECT * FROM gold.att;
SELECT * FROM gold.surgey_efficiny;
SELECT * FROM gold.kpi_doctor_workload;
SELECT * FROM gold.kpi_data_quality;
```

---

## ⚙️ Automation

| Component | Trigger | Frequency |
|---|---|---|
| Snowpipe (5 pipes) | S3 file upload → SQS event | Real-time on file arrival |
| Stream (5 streams) | Always-on CDC | Continuous |
| Task `healthcare_pipeline_task` | Schedule | Every 5 minutes |
| Dynamic Tables (3 tables) | Snowflake managed | Every 5 minutes (TARGET_LAG) |

```sql
-- run pipeline manually any time
CALL silver.run_full_pipeline();

-- check task is running
SHOW TASKS IN SCHEMA silver;

-- check pipe health
SELECT SYSTEM$PIPE_STATUS('patients_pipe');
```

---

## 🚀 Setup & Run Guide

### Prerequisites
- Snowflake account with ACCOUNTADMIN role
- AWS account with S3 bucket `hospital-health-care`
- IAM role `health_care_role` in AWS

### Step-by-step

```sql
-- 1. Create database and bronze schema
CREATE DATABASE health_care;
CREATE SCHEMA bronze;

-- 2. Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION bronze.s3_int_orders ...;

-- 3. Get AWS trust policy values
DESC STORAGE INTEGRATION bronze.s3_int_orders;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Paste into AWS IAM role trust policy in AWS Console

-- 4. Create file format, stages, raw tables, streams
-- (run bronze section of medicore_full_commented.sql)

-- 5. Refresh pipes for initial load
ALTER PIPE patients_pipe REFRESH;
ALTER PIPE doctors_pipe REFRESH;
ALTER PIPE admissions_pipe REFRESH;
ALTER PIPE procedures_pipe REFRESH;
ALTER PIPE billing_pipe REFRESH;

-- 6. Create silver schema, tables, procedures
CREATE SCHEMA silver;
-- (run silver section of medicore_full_commented.sql)

-- 7. Run pipeline manually to test
CALL silver.run_full_pipeline();

-- 8. Check results
SELECT * FROM silver.silver_patients;
SELECT * FROM silver.exception_table;

-- 9. Create gold schema, dimensions, dynamic tables, KPI views
CREATE SCHEMA gold;
CALL gold.dim_patient();
CALL gold.dim_doctor();
-- (run gold section of medicore_full_commented.sql)

-- 10. Resume task for automation
ALTER TASK silver.healthcare_pipeline_task RESUME;

-- 11. Apply masking (run masking section of SQL file)
```

---

## 📁 Full Object Reference

```
health_care  (database)
│
├── bronze  (schema)
│   ├── s3_int_orders          — storage integration (IAM role)
│   ├── csv_format             — file format
│   ├── patients_s3_stage      — external stage
│   ├── doctors_s3_stage
│   ├── admissions_s3_stage
│   ├── procedures_s3_stage
│   ├── billing_s3_stage
│   ├── raw_patients           — raw table (all STRING)
│   ├── raw_patients_st        — CDC stream
│   ├── raw_doctors + stream
│   ├── raw_admissions + stream
│   ├── raw_procedures + stream
│   ├── raw_billing + stream
│   ├── patients_pipe          — Snowpipe AUTO_INGEST
│   ├── doctors_pipe
│   ├── admissions_pipe
│   ├── procedures_pipe
│   └── billing_pipe
│
├── silver  (schema)
│   ├── silver_patients        — INT PK, typed, deduplicated
│   ├── silver_doctors         — INT PK, typed, deduplicated
│   ├── silver_admissions      — INT PK, FK → patients + doctors
│   ├── silver_procedures      — INT PK, FK → admissions
│   ├── silver_billing         — INT PK, FK → admissions
│   ├── exception_table        — DQ failure log
│   ├── pii_tag                — PII classification tag
│   ├── pii_mask_policy        — masking policy
│   ├── load_patients()        — procedure
│   ├── load_doctors()
│   ├── load_admissions()
│   ├── load_procedures()
│   ├── load_billing()
│   ├── run_full_pipeline()    — master orchestration
│   └── healthcare_pipeline_task  — Task, runs every 5 min
│
└── gold  (schema)
    ├── dim_patient            — SCD-2 table (current_flag Y/N)
    ├── dim_doctor             — SCD-2 table (current_flag Y/N)
    ├── dim_patient()          — SCD-2 expire+insert procedure
    ├── dim_doctor()           — SCD-2 expire+insert procedure
    ├── fact_admissions        — Dynamic Table (5-min lag)
    ├── fact_procedures        — Dynamic Table (5-min lag)
    ├── fact_billing           — Dynamic Table (5-min lag)
    ├── bed_occ                — KPI 1 view
    ├── att                    — KPI 2 view
    ├── surgey_efficiny        — KPI 3 view
    ├── kpi_doctor_workload    — KPI 4 view
    └── kpi_data_quality       — KPI 5 view
```

---

## 🔗 BI Integration

All Gold layer views and Dynamic Tables connect directly to:
- **Power BI** — via Snowflake connector

No additional transformation needed — Gold layer is query-ready.