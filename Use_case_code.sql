-- ============================================================
--  MEDICORE HEALTH — HOSPITAL HEALTHCARE OPERATIONS PLATFORM
--  Team  : Data Mavericks
--  Members: Manubolu Rakesh | Challagulla Venkat
--           Seelam Surya Prakash Reddy | Gosu Lavanya
--  Architecture : Medallion  (Bronze → Silver → Gold)
--  Platform     : Snowflake + AWS S3
-- ============================================================


-- ============================================================
--  SECTION 1 — DATABASE & BRONZE SCHEMA
-- ============================================================

-- create the single database that holds all 3 medallion schemas
create database health_care;

-- bronze = raw landing zone
-- data arrives here exactly as it comes from S3 — no transformations
create schema bronze;

-- ── STORAGE INTEGRATION ──────────────────────────────────────
-- links Snowflake to the AWS S3 bucket using IAM role (more secure than keys)
-- after creating this, run DESC to get the AWS_IAM_USER_ARN and EXTERNAL_ID
-- those two values must be added to the AWS IAM trust policy for the role
CREATE OR REPLACE STORAGE INTEGRATION bronze.s3_int_orders
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::296368270186:role/health_care_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://hospital-health-care/'
  );

  -- run this after creation to get trust policy values for AWS IAM
  DESC STORAGE INTEGRATION bronze.s3_int_orders;-
  --arn:aws:iam::546602405306:user/178e1000-s
  --QB15303_SFCRole=11_DOTNYp0RHYUUwZ7IEWbEhDHIhPE=

-- ── FILE FORMAT ───────────────────────────────────────────────
-- tells Snowflake how to parse incoming CSV files
-- FIELD_OPTIONALLY_ENCLOSED_BY: handles fields that may have commas inside quotes
-- SKIP_HEADER 1: ignores the first row (column names row)
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- ── EXTERNAL STAGES ──────────────────────────────────────────
-- each stage is a pointer to a specific S3 folder for one domain
-- Snowpipe and COPY INTO use these stages to read files

-- patients stage → reads from s3://hospital-health-care/patients/
CREATE OR REPLACE STAGE patients_s3_stage
URL = 's3://hospital-health-care/patients/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

-- list files currently in the stage (useful for debugging)
list @patients_s3_stage;

-- doctors stage → reads from s3://hospital-health-care/doctors/
CREATE OR REPLACE STAGE doctors_s3_stage
URL = 's3://hospital-health-care/doctors/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @doctors_s3_stage;

-- admissions stage → reads from s3://hospital-health-care/admissions/
CREATE OR REPLACE STAGE admissions_s3_stage
URL = 's3://hospital-health-care/admissions/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @admissions_s3_stage;

-- procedures stage → reads from s3://hospital-health-care/procedures/
CREATE OR REPLACE STAGE procedures_s3_stage
URL = 's3://hospital-health-care/procedures/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @procedures_s3_stage;

-- billing stage → reads from s3://hospital-health-care/billing/
CREATE OR REPLACE STAGE billing_s3_stage
URL = 's3://hospital-health-care/billing/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @billing_s3_stage;


-- ── RAW TABLES ───────────────────────────────────────────────
-- ALL columns are STRING here — we trust nothing from the source yet
-- data types (DATE, TIMESTAMP, NUMBER) are cast and validated in silver layer
-- load_time = audit column to track exactly when each row arrived in Snowflake

-- patients raw table: stores patient CSV rows exactly as received from S3
CREATE OR REPLACE TABLE raw_patients (
  patient_id STRING,
  name STRING,
  dob STRING,           -- will be cast to DATE in silver
  gender STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  insurance_id STRING,
  registration_date STRING,  -- will be cast to DATE in silver
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC stream on raw_patients
-- captures every INSERT that Snowpipe makes into raw_patients
-- silver layer reads this stream instead of scanning the whole table
-- once stream is consumed by a DML (like the silver procedure), it resets
create or replace stream raw_patients_st on table bronze.raw_patients;

-- doctors raw table
CREATE OR REPLACE TABLE raw_doctors (
  doctor_id STRING,
  name STRING,
  specialization STRING,
  join_date STRING,     -- will be cast to DATE in silver
  phone STRING,
  email STRING,
  qualification STRING,
  department STRING,
  status STRING,        -- expected values: ACTIVE / INACTIVE / ON_LEAVE
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC stream on raw_doctors
create or replace stream raw_doctors_st on table bronze.raw_doctors;

-- admissions raw table
CREATE OR REPLACE TABLE raw_admissions (
  admission_id STRING,
  patient_id STRING,
  admission_time STRING,       -- will be cast to TIMESTAMP in silver
  discharge_time STRING,       -- will be cast to TIMESTAMP in silver
  department STRING,
  ward STRING,
  bed_no STRING,               -- will be cast to INT in silver, must be positive
  attending_doctor_id STRING,
  admission_type STRING,       -- expected values: OPD / IPD / ER
  status STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC stream on raw_admissions
create or replace stream raw_admissions_st on table raw_admissions;

-- procedures raw table (surgeries and OT procedures)
CREATE OR REPLACE TABLE raw_procedures (
  procedure_id STRING,
  admission_id STRING,
  patient_id STRING,
  procedure_name STRING,
  surgeon_id STRING,
  scheduled_time STRING,  -- expected: start within ±10 min for efficiency KPI
  start_time STRING,
  end_time STRING,        -- must be >= start_time (validated in silver)
  operating_room STRING,
  anaesthesia_type STRING,
  status STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC stream on raw_procedures
create or replace stream raw_procedures_st on table raw_procedures;

-- billing raw table (claims and payment records)
CREATE OR REPLACE TABLE raw_billing (
  bill_id STRING,
  admission_id STRING,
  patient_id STRING,
  total_amount STRING,          -- must be >= 0 (validated in silver)
  insurance_amount STRING,
  out_of_pocket_amount STRING,
  billing_time STRING,
  payment_mode STRING,
  is_flagged STRING,            -- TRUE/FALSE — suspicious bill flag from source
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC stream on raw_billing
create or replace stream raw_billing_st on table raw_billing;


-- ── SNOWPIPES ─────────────────────────────────────────────────
-- AUTO_INGEST = TRUE: pipe listens to S3 SQS event notifications
-- when a new file lands in S3 folder, SQS triggers the pipe automatically
-- pipe reads the file and runs COPY INTO the raw table — fully serverless
-- after creating each pipe, run DESC PIPE to get the SQS ARN
-- add that SQS ARN to the S3 bucket event notification in AWS console

-- patients pipe: automatically loads patient files into raw_patients
CREATE OR REPLACE PIPE patients_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze.raw_patients (
  patient_id,
  name,
  dob,
  gender,
  email,
  phone,
  address,
  city,
  state,
  insurance_id,
  registration_date
)
FROM @bronze.patients_s3_stage
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

-- REFRESH forces a one-time manual load of files already in S3 (for initial load)
alter pipe patients_pipe refresh;
-- check if pipe is running, paused, or failing
SELECT SYSTEM$PIPE_STATUS('patients_pipe');

select * from bronze.raw_patients;
select * from bronze.raw_patients_st;

-- doctors pipe
CREATE OR REPLACE PIPE doctors_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze.raw_doctors (
  doctor_id,
  name,
  specialization,
  join_date,
  phone,
  email,
  qualification,
  department,
  status
)
FROM @bronze.doctors_s3_stage
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

alter pipe doctors_pipe refresh;

select * from raw_doctors;
select * from bronze.raw_doctors_st;

-- admissions pipe
CREATE OR REPLACE PIPE admissions_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze.raw_admissions (
  admission_id,
  patient_id,
  admission_time,
  discharge_time,
  department,
  ward,
  bed_no,
  attending_doctor_id,
  admission_type,
  status
)
FROM @bronze.admissions_s3_stage
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

alter pipe admissions_pipe refresh;

select * from BRONZE.raw_admissions;
select * from BRONZE.raw_admissions_st;

-- procedures pipe
CREATE OR REPLACE PIPE procedures_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze.raw_procedures (
  procedure_id,
  admission_id,
  patient_id,
  procedure_name,
  surgeon_id,
  scheduled_time,
  start_time,
  end_time,
  operating_room,
  anaesthesia_type,
  status
)
FROM @bronze.procedures_s3_stage
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

-- truncate used during development to reload test data without duplicates
truncate table bronze.raw_procedures;

alter pipe procedures_pipe refresh ;

select * from bronze.raw_procedures;
select * from bronze.raw_procedures_st;

-- billing pipe
CREATE OR REPLACE PIPE billing_pipe
AUTO_INGEST = TRUE
AS
COPY INTO bronze.raw_billing (
  bill_id,
  admission_id,
  patient_id,
  total_amount,
  insurance_amount,
  out_of_pocket_amount,
  billing_time,
  payment_mode,
  is_flagged
)
FROM @bronze.billing_s3_stage
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

alter pipe billing_pipe refresh;

select * from raw_billing;
select * from bronze.raw_billing_st;

-----------

-- ============================================================
--  SECTION 2 — SILVER LAYER (VALIDATED & CLEANED)
--  reads from bronze streams → validates → merges into silver
--
--  CRITICAL LOADING ORDER (FK dependency chain):
--  1. patients   — no FK deps
--  2. doctors    — no FK deps
--  3. admissions — FK: patient_id → patients, doctor_id → doctors
--  4. procedures — FK: admission_id → admissions
--  5. billing    — FK: admission_id → admissions
-- ============================================================

create schema silver;
use schema silver;

-- silver_patients: cleaned patient records with proper data types
-- patient_id INT PRIMARY KEY = deduplication key for MERGE
CREATE OR REPLACE TABLE silver_patients (
  patient_id INT PRIMARY KEY,
  name STRING,
  dob DATE,              -- cast from STRING (validated with TRY_TO_DATE)
  gender STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  insurance_id STRING,
  registration_date DATE,
  load_time TIMESTAMP
);

-- silver_doctors: cleaned doctor records
CREATE OR REPLACE TABLE silver_doctors (
  doctor_id INT PRIMARY KEY,
  name STRING,
  specialization STRING,
  join_date DATE,        -- cast from STRING (validated with TRY_TO_DATE)
  phone STRING,
  email STRING,
  qualification STRING,
  department STRING,
  status STRING,
  load_time TIMESTAMP
);

-- silver_admissions: validated admission events
-- FOREIGN KEY constraints enforce referential integrity at the table level
-- admissions cannot exist without a valid patient and doctor in silver
CREATE OR REPLACE TABLE silver_admissions (
  admission_id INT PRIMARY KEY,
  patient_id INT,
  admission_time TIMESTAMP,
  discharge_time TIMESTAMP,
  department STRING,
  ward STRING,
  bed_no INT,
  attending_doctor_id INT,
  admission_type STRING,    -- OPD / IPD / ER
  status STRING,
  load_time TIMESTAMP,

  -- referential integrity: patient must be loaded before admission
  FOREIGN KEY (patient_id) REFERENCES silver_patients(patient_id),
  -- referential integrity: doctor must be loaded before admission
  FOREIGN KEY (attending_doctor_id) REFERENCES silver_doctors(doctor_id)
);

-- silver_procedures: validated surgery and procedure records
-- a procedure cannot exist without a parent admission
CREATE OR REPLACE TABLE silver_procedures (
  procedure_id INT PRIMARY KEY,
  admission_id INT,
  patient_id INT,
  procedure_name STRING,
  surgeon_id INT,
  scheduled_time TIMESTAMP,
  start_time TIMESTAMP,
  end_time TIMESTAMP,       -- validated: end_time >= start_time
  operating_room STRING,
  anaesthesia_type STRING,
  status STRING,
  load_time TIMESTAMP,

  -- referential integrity: admission must exist before procedure
  FOREIGN KEY (admission_id) REFERENCES silver_admissions(admission_id)
);

-- silver_billing: validated billing and claims records
-- a bill cannot exist without a parent admission
CREATE OR REPLACE TABLE silver_billing (
  bill_id INT PRIMARY KEY,
  admission_id INT,
  patient_id INT,
  total_amount FLOAT,          -- validated: >= 0 (negatives go to exception_table)
  insurance_amount FLOAT,
  out_of_pocket_amount FLOAT,
  billing_time TIMESTAMP,
  payment_mode STRING,
  is_flagged STRING,           -- Y/N flag for suspicious bills (carried from source)
  load_time TIMESTAMP,

  -- referential integrity: admission must exist before billing
  FOREIGN KEY (admission_id) REFERENCES silver_admissions(admission_id)
);

-- exception_table: central DQ failure log
-- every row that fails any validation lands here with the reason
-- error_type = DATA_ERROR  → bad data type / format
-- error_type = BUSINESS_ERROR → FK violation, temporal rule, or range check
CREATE OR REPLACE TABLE exception_table (
  source_table STRING,    -- which domain failed: patients/doctors/admissions/etc
  record_id STRING,       -- the primary key of the failed row
  error_type STRING,      -- DATA_ERROR or BUSINESS_ERROR
  error_message STRING,   -- human-readable reason
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

select * from silver.exception_table;


-- ── PROCEDURE: load_patients ──────────────────────────────────
-- reads new rows from bronze stream → validates → merges into silver_patients
-- MERGE logic:
--   WHEN MATCHED (patient_id exists): update name, city, state (SCD-1 overwrite)
--   WHEN NOT MATCHED (new patient): insert full row
-- ROW_NUMBER deduplicates if same patient_id appears multiple times in stream
CREATE OR REPLACE PROCEDURE silver.load_patients()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

-- step 1: pull all new rows from the bronze stream into a temp table
-- temp table exists only for the duration of this procedure call
CREATE OR REPLACE TEMP TABLE temp_patients AS
SELECT * FROM bronze.raw_patients_st;

-- step 2: log invalid rows to exception_table (they still go to temp_patients)
-- DQ rules: patient_id must be numeric, dob must be a valid date, email must have @
INSERT INTO silver.exception_table
SELECT 'patients', patient_id, 'DATA_ERROR', 'Invalid patient data', CURRENT_TIMESTAMP
FROM temp_patients
WHERE TRY_TO_NUMBER(patient_id) IS NULL
   OR TRY_TO_DATE(dob) IS NULL
   OR email NOT LIKE '%@%';

-- step 3: filter out invalid rows — only clean rows go forward
CREATE OR REPLACE TEMP TABLE valid_patients AS
SELECT *
FROM temp_patients
WHERE TRY_TO_NUMBER(patient_id) IS NOT NULL
  AND TRY_TO_DATE(dob) IS NOT NULL
  AND email LIKE '%@%';

-- step 4: MERGE into silver_patients
-- ROW_NUMBER picks the latest row per patient_id if duplicates exist in stream
-- WHEN MATCHED: update mutable fields (SCD-1, overwrites previous value)
-- WHEN NOT MATCHED: insert brand new patient record
MERGE INTO silver.silver_patients t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY load_time DESC) rn
    FROM valid_patients
  ) WHERE rn = 1
) s
ON t.patient_id = s.patient_id

WHEN MATCHED THEN UPDATE SET
  t.name = s.name,
  t.city = s.city,
  t.state = s.state,
  t.load_time = s.load_time

WHEN NOT MATCHED THEN INSERT VALUES (
  s.patient_id, s.name, TRY_TO_DATE(s.dob), s.gender, s.email,
  s.phone, s.address, s.city, s.state,
  s.insurance_id, TRY_TO_DATE(s.registration_date), s.load_time
);

RETURN 'Patients loaded';
END;
$$;

select * from bronze.raw_patients_st;
call silver.load_patients();
select * from exception_table;
select * from silver.silver_patients;


-- ── PROCEDURE: load_doctors ───────────────────────────────────
-- same pattern as load_patients: stream → validate → merge
-- checks: doctor_id numeric, join_date valid date, email format valid
CREATE OR REPLACE PROCEDURE silver.load_doctors()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

-- guard: skip execution if stream has no new data
SELECT COUNT(*) INTO :cnt FROM bronze.raw_doctors_st;
IF (cnt < 1) THEN
  RETURN 'No data in doctors stream';
END IF;

-- step 1: pull stream into temp
CREATE or replace TEMP TABLE temp_doctors AS
SELECT * FROM bronze.raw_doctors_st;

-- step 2: log invalid rows
INSERT INTO exception_table
SELECT 'doctors', doctor_id, 'DATA_ERROR', 'Invalid doctor data', CURRENT_TIMESTAMP
FROM temp_doctors
WHERE TRY_TO_NUMBER(doctor_id) IS NULL
   OR TRY_TO_DATE(join_date) IS NULL
   OR email NOT LIKE '%@%';

-- step 3: filter clean rows only
CREATE or replace TEMP TABLE valid_doctors AS
SELECT * FROM temp_doctors
WHERE TRY_TO_NUMBER(doctor_id) IS NOT NULL
  AND TRY_TO_DATE(join_date) IS NOT NULL
  AND email LIKE '%@%';

-- step 4: MERGE into silver_doctors
-- WHEN MATCHED: update name and department (doctor may change dept over time)
-- WHEN NOT MATCHED: insert new doctor record
MERGE INTO silver_doctors t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY doctor_id ORDER BY load_time DESC) rn
    FROM valid_doctors
  ) WHERE rn = 1
) s
ON t.doctor_id = s.doctor_id

WHEN MATCHED THEN UPDATE SET
  t.name = s.name,
  t.department = s.department,
  t.load_time = s.load_time

WHEN NOT MATCHED THEN INSERT VALUES (
  s.doctor_id, s.name, s.specialization, TRY_TO_DATE(s.join_date),
  s.phone, s.email, s.qualification, s.department, s.status, s.load_time
);

RETURN 'Doctors loaded';
END;
$$;


call silver.load_doctors();

select * from silver.silver_doctors;


-- ── PROCEDURE: load_admissions ────────────────────────────────
-- two-level DQ validation before merging:
-- level 1 (DATA_ERROR):    bad data types go to exception_table
-- level 2 (BUSINESS_ERROR): FK violations + temporal rule violations
-- MUST run AFTER load_patients and load_doctors (FK dependency)
CREATE OR REPLACE PROCEDURE load_admissions()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

SELECT COUNT(*) INTO :cnt FROM bronze.raw_admissions_st;
IF (cnt < 1) THEN
  RETURN 'No data in admissions stream';
END IF;

-- step 1: pull stream into temp
CREATE or replace TEMP TABLE temp_adm AS
SELECT * FROM bronze.raw_admissions_st;

-- step 2: level 1 DQ — data type checks
-- admission_id must be numeric, both time fields must be parseable timestamps
INSERT INTO exception_table
SELECT 'admissions', admission_id, 'DATA_ERROR', 'Invalid admission data', CURRENT_TIMESTAMP
FROM temp_adm
WHERE TRY_TO_NUMBER(admission_id) IS NULL
   OR TRY_TO_TIMESTAMP(admission_time) IS NULL
   OR TRY_TO_TIMESTAMP(discharge_time) IS NULL;

-- step 3: filter to type-valid rows only
CREATE or replace TEMP TABLE valid_adm AS
SELECT * FROM temp_adm
WHERE TRY_TO_NUMBER(admission_id) IS NOT NULL
  AND TRY_TO_TIMESTAMP(admission_time) IS NOT NULL
  AND TRY_TO_TIMESTAMP(discharge_time) IS NOT NULL;

-- step 4: level 2 DQ — business rule checks
-- FK: patient_id must exist in silver_patients (loaded first)
-- FK: attending_doctor_id must exist in silver_doctors (loaded second)
-- temporal rule: discharge cannot happen before admission
INSERT INTO exception_table
SELECT 'admissions', admission_id, 'BUSINESS_ERROR', 'FK or time issue', CURRENT_TIMESTAMP
FROM valid_adm
WHERE patient_id NOT IN (SELECT patient_id FROM silver_patients)
   OR attending_doctor_id NOT IN (SELECT doctor_id FROM silver_doctors)
   OR discharge_time < admission_time;

-- step 5: MERGE only rows passing both DQ levels
MERGE INTO silver_admissions t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY admission_id ORDER BY load_time DESC) rn
    FROM valid_adm
    WHERE patient_id IN (SELECT patient_id FROM silver_patients)
      AND attending_doctor_id IN (SELECT doctor_id FROM silver_doctors)
      AND discharge_time >= admission_time
  ) WHERE rn = 1
) s
ON t.admission_id = s.admission_id

WHEN MATCHED THEN UPDATE SET t.patient_id = s.patient_id

WHEN NOT MATCHED THEN INSERT VALUES (
  s.admission_id, s.patient_id, s.admission_time, s.discharge_time,
  s.department, s.ward, s.bed_no, s.attending_doctor_id,
  s.admission_type, s.status, s.load_time
);

RETURN 'Admissions loaded';
END;
$$;

call load_admissions();
select * from silver_admissions;


-- ── PROCEDURE: load_procedures ────────────────────────────────
-- validates procedure records before loading
-- FK: admission_id must exist in silver_admissions
-- temporal: end_time must be >= start_time (catches OT overruns)
-- MUST run AFTER load_admissions (FK dependency)
select * from bronze.raw_procedures;
select * from silver_procedures;


CREATE OR REPLACE PROCEDURE load_procedures()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

SELECT COUNT(*) INTO :cnt FROM bronze.raw_procedures_st;
IF (cnt < 1) THEN
  RETURN 'No data in procedures stream';
END IF;

-- step 1: pull stream into temp
CREATE or replace TEMP TABLE temp_proc AS
SELECT * FROM bronze.raw_procedures_st;

-- step 2: level 1 DQ — procedure_id must be numeric
INSERT INTO exception_table
SELECT 'procedures', procedure_id, 'DATA_ERROR', 'Invalid procedure data', CURRENT_TIMESTAMP
FROM temp_proc
WHERE TRY_TO_NUMBER(procedure_id) IS NULL;

-- step 3: filter type-valid rows
CREATE or replace TEMP TABLE valid_proc AS
SELECT * FROM temp_proc
WHERE TRY_TO_NUMBER(procedure_id) IS NOT NULL;

-- step 4: level 2 DQ — business rule checks
-- FK: admission must exist in silver_admissions
-- temporal: end_time cannot be before start_time (invalid OT record)
INSERT INTO exception_table
SELECT 'procedures', procedure_id, 'BUSINESS_ERROR', 'FK or time issue', CURRENT_TIMESTAMP
FROM valid_proc
WHERE admission_id NOT IN (SELECT admission_id FROM silver_admissions)
   OR end_time < start_time;

-- step 5: MERGE — no WHEN MATCHED because procedure records don't change
MERGE INTO silver_procedures t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY procedure_id ORDER BY load_time DESC) rn
    FROM valid_proc
    WHERE admission_id IN (SELECT admission_id FROM silver_admissions)
      AND end_time >= start_time
  ) WHERE rn = 1
) s
ON t.procedure_id = s.procedure_id

WHEN NOT MATCHED THEN INSERT VALUES (
  s.procedure_id, s.admission_id, s.patient_id, s.procedure_name,
  s.surgeon_id, s.scheduled_time, s.start_time, s.end_time,
  s.operating_room, s.anaesthesia_type, s.status, s.load_time
);

RETURN 'Procedures loaded';
END;
$$;

select * from bronze.raw_patients;
call load_procedures();


-- ── PROCEDURE: load_billing ───────────────────────────────────
-- validates billing records before loading
-- FK: admission_id must exist in silver_admissions
-- business rule: total_amount must not be negative (catches fraudulent records)
-- MUST run AFTER load_admissions (FK dependency)
CREATE OR REPLACE PROCEDURE load_billing()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

SELECT COUNT(*) INTO :cnt FROM bronze.raw_billing_st;
IF (cnt < 1) THEN
  RETURN 'No data in billing stream';
END IF;

-- step 1: pull stream into temp
CREATE or replace TEMP TABLE temp_bill AS
SELECT * FROM bronze.raw_billing_st;

-- step 2: level 1 DQ — bill_id and total_amount must be numeric
INSERT INTO exception_table
SELECT 'billing', bill_id, 'DATA_ERROR', 'Invalid billing data', CURRENT_TIMESTAMP
FROM temp_bill
WHERE TRY_TO_NUMBER(bill_id) IS NULL
   OR TRY_TO_NUMBER(total_amount) IS NULL;

-- step 3: filter type-valid rows
CREATE or replace TEMP TABLE valid_bill AS
SELECT * FROM temp_bill
WHERE TRY_TO_NUMBER(bill_id) IS NOT NULL
  AND TRY_TO_NUMBER(total_amount) IS NOT NULL;

-- step 4: level 2 DQ — business rule checks
-- FK: admission must exist in silver_admissions
-- range check: negative total_amount is an anomaly
INSERT INTO exception_table
SELECT 'billing', bill_id, 'BUSINESS_ERROR', 'FK or negative amount', CURRENT_TIMESTAMP
FROM valid_bill
WHERE admission_id NOT IN (SELECT admission_id FROM silver_admissions)
   OR total_amount < 0;

-- step 5: MERGE — no WHEN MATCHED because billing records are immutable
MERGE INTO silver_billing t
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY bill_id ORDER BY load_time DESC) rn
    FROM valid_bill
    WHERE admission_id IN (SELECT admission_id FROM silver_admissions)
      AND total_amount >= 0
  ) WHERE rn = 1
) s
ON t.bill_id = s.bill_id

WHEN NOT MATCHED THEN INSERT VALUES (
  s.bill_id, s.admission_id, s.patient_id,
  s.total_amount, s.insurance_amount, s.out_of_pocket_amount,
  s.billing_time, s.payment_mode, s.is_flagged, s.load_time
);

RETURN 'Billing loaded';
END;
$$;

call load_billing();

select * from silver.silver_billing;



-- ── PROCEDURE: run_full_pipeline ─────────────────────────────
-- master orchestration: calls all 5 procedures in the correct FK order
-- patients and doctors must run BEFORE admissions
-- admissions must run BEFORE procedures and billing
-- called by the scheduled Task every 5 minutes
CREATE OR REPLACE PROCEDURE run_full_pipeline()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
  res_patients STRING;
  res_doctors STRING;
  res_admissions STRING;
  res_procedures STRING;
  res_billing STRING;
BEGIN

-- execution order is critical — FK dependencies must be respected
res_patients := (CALL load_patients());
res_doctors := (CALL load_doctors());
res_admissions := (CALL load_admissions());
res_procedures := (CALL load_procedures());
res_billing := (CALL load_billing());

-- return combined status string for monitoring
RETURN 
    'Patients: ' || res_patients || ' | ' ||
    'Doctors: ' || res_doctors || ' | ' ||
    'Admissions: ' || res_admissions || ' | ' ||
    'Procedures: ' || res_procedures || ' | ' ||
    'Billing: ' || res_billing;

END;
$$;

call silver.run_full_pipeline();

select * from silver.silver_procedures;

-- ── TASK: healthcare_pipeline_task ───────────────────────────
-- Snowflake Task runs run_full_pipeline() every 5 minutes automatically
-- tasks are created in SUSPENDED state by default — RESUME activates it
-- the task triggers even if stream has no data (procedures have internal guards)
CREATE OR REPLACE TASK healthcare_pipeline_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
AS
  CALL silver.run_full_pipeline();

-- must manually resume the task after creation
ALTER TASK SILVER.healthcare_pipeline_task RESUME;


-- ── GOVERNANCE: DYNAMIC DATA MASKING (PII Protection) ────────
-- sensitive columns are hidden from non-admin roles using tag-based masking
-- ACCOUNTADMIN / SYSADMIN → see real data
-- all other roles          → see ****MASKED****
-- this satisfies HIPAA-style data access controls

-- create a reusable PII tag to label sensitive columns
CREATE OR REPLACE TAG silver.pii_tag;

-- masking policy: returns real value for admins, masked string for all others
CREATE OR REPLACE MASKING POLICY silver.pii_mask_policy 
AS (val STRING)
RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN') THEN val
    ELSE '****MASKED****'
END;

-- bind the masking policy to the PII tag
-- any column tagged with pii_tag will automatically use this policy
ALTER TAG silver.pii_tag 
SET MASKING POLICY silver.pii_mask_policy;

-- apply PII tag to sensitive columns in silver_patients
ALTER TABLE silver_patients 
MODIFY COLUMN email SET TAG pii_tag = 'PII';

ALTER TABLE silver_patients 
MODIFY COLUMN phone SET TAG pii_tag = 'PII';

ALTER TABLE silver_patients 
MODIFY COLUMN insurance_id SET TAG pii_tag = 'PII';

-- apply PII tag to sensitive columns in silver_doctors
ALTER TABLE silver_doctors 
MODIFY COLUMN email SET TAG pii_tag = 'PII';

ALTER TABLE silver_doctors 
MODIFY COLUMN phone SET TAG pii_tag = 'PII';


-- ============================================================
--  SECTION 3 — GOLD LAYER (ANALYTICS READY)
--  dim_patient / dim_doctor: SCD-2 dimensions with history tracking
--  fact_admissions / fact_procedures / fact_billing: Dynamic Tables
--  KPI views: pre-aggregated business metrics for BI dashboards
-- ============================================================

create schema gold;
USE SCHEMA gold;

-- ── SCD-2 DIMENSION TABLES ───────────────────────────────────
-- effective_start_date: when this version of the record became active
-- effective_end_date:   when this version was superseded (NULL = still active)
-- current_flag:         'Y' = latest active record, 'N' = historical version

-- dim_patient: patient dimension with full change history
CREATE OR REPLACE TABLE dim_patient (
    patient_id NUMBER,
    name STRING,
    dob DATE,
    gender STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    insurance_id STRING,
    registration_date DATE,
    effective_start_date DATE,   -- date this version became active
    effective_end_date DATE,     -- date this version was expired (NULL if current)
    current_flag STRING          -- 'Y' = active row, 'N' = historical row
);

-- dim_doctor: doctor dimension with full change history
-- tracks department transfers, status changes, specialization updates
CREATE OR REPLACE TABLE dim_doctor (
    doctor_id NUMBER,
    name STRING,
    specialization STRING,
    join_date DATE,
    phone STRING,
    email STRING,
    qualification STRING,
    department STRING,
    status STRING,
    effective_start_date DATE,
    effective_end_date DATE,
    current_flag STRING
);


-- ── PROCEDURE: dim_patient (SCD-2 load) ──────────────────────
-- SCD-2 logic: expire old → insert new version
-- step 1: UPDATE existing 'Y' rows → set end date to yesterday, flag to 'N'
-- step 2: INSERT all current silver_patients as new 'Y' rows with today as start
-- result: full history preserved, latest version always queryable with current_flag='Y'
CREATE OR REPLACE PROCEDURE gold.dim_patient()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- STEP 1: expire the current active rows
-- sets effective_end_date = yesterday so history chain is continuous
UPDATE gold.dim_patient t
SET 
    effective_end_date = CURRENT_DATE - 1,
    current_flag = 'N'
FROM silver.silver_patients s
WHERE t.patient_id = s.patient_id
  AND t.current_flag = 'Y';

-- STEP 2: insert latest data from silver as new active version
-- CURRENT_DATE as effective_start_date, NULL as effective_end_date (still active)
INSERT INTO gold.dim_patient
SELECT
    s.patient_id,
    s.name,
    s.dob,
    s.gender,
    s.email,
    s.phone,
    s.address,
    s.city,
    s.state,
    s.insurance_id,
    s.registration_date,
    CURRENT_DATE,   -- effective_start_date
    NULL,           -- effective_end_date: NULL means this version is still active
    'Y'             -- current_flag
FROM silver.silver_patients s;

RETURN 'DIM_PATIENT SIMPLE SCD DONE';

END;
$$;

call dim_patient();

-- ── PROCEDURE: dim_doctor (SCD-2 load) ───────────────────────
-- identical SCD-2 pattern as dim_patient
-- expire old active rows → insert fresh current rows from silver
CREATE OR REPLACE PROCEDURE gold.dim_doctor()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- EXPIRE OLD: mark currently active doctor rows as historical
UPDATE gold.dim_doctor t
SET 
    effective_end_date = CURRENT_DATE - 1,
    current_flag = 'N'
FROM silver.silver_doctors s
WHERE t.doctor_id = s.doctor_id
  AND t.current_flag = 'Y';

-- INSERT LATEST: bring in all current silver data as new active version
INSERT INTO gold.dim_doctor
SELECT
    s.doctor_id,
    s.name,
    s.specialization,
    s.join_date,
    s.phone,
    s.email,
    s.qualification,
    s.department,
    s.status,
    CURRENT_DATE,   -- effective_start_date
    NULL,           -- effective_end_date
    'Y'             -- current_flag
FROM silver.silver_doctors s;

RETURN 'DIM_DOCTOR SIMPLE SCD DONE';

END;
$$;

call dim_doctor();
select * from silver.silver_admissions;
select * from bronze.raw_admissions;
select * from gold.dim_patient;
select * from gold.dim_doctor;

-- ── DYNAMIC FACT TABLES ───────────────────────────────────────
-- Dynamic Tables auto-refresh every 5 minutes from silver (TARGET_LAG)
-- Snowflake handles the refresh automatically — no stored procedure needed
-- always stays in sync with silver layer for real-time analytics

-- fact_admissions: core admission events
-- used for bed occupancy KPI and admission turnaround time KPI
CREATE OR REPLACE DYNAMIC TABLE gold.fact_admissions
TARGET_LAG = '5 minutes'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    admission_id,
    patient_id,
    attending_doctor_id AS doctor_id,
    admission_time,
    discharge_time,
    department,
    bed_no
FROM silver.silver_admissions;

select * from gold.fact_admissions;

-- fact_procedures: surgery and OT procedure events
-- used for surgery efficiency KPI and doctor workload KPI
CREATE OR REPLACE DYNAMIC TABLE gold.fact_procedures
TARGET_LAG = '5 minutes'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    procedure_id,
    admission_id,
    surgeon_id AS doctor_id,
    scheduled_time,
    start_time,
    end_time,
    operating_room
FROM silver.silver_procedures;


select * from gold.fact_procedures;

-- fact_billing: billing and claims events
-- used for billing accuracy KPI
CREATE OR REPLACE DYNAMIC TABLE gold.fact_billing
TARGET_LAG = '5 minutes'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    bill_id,
    admission_id,
    patient_id,
    total_amount,
    billing_time
FROM silver.silver_billing;

select * from silver.silver_billing;


-- ── KPI VIEWS ─────────────────────────────────────────────────
-- 5 business KPIs as required by hackathon use case
-- all built on gold dynamic tables — always reflect latest data

-- KPI 1: Bed Occupancy & Utilization Score
-- formula: total occupied hours / (number of distinct beds × 24 hours)
-- purpose: optimize bed allocation, identify wards near capacity, reduce ER overflow
create or replace view bed_occ as
WITH usage AS (
    SELECT 
        SUM(DATEDIFF('hour', admission_time, discharge_time)) AS occupied_hours,
        COUNT(DISTINCT bed_no) AS total_beds
    FROM gold.fact_admissions
    WHERE admission_time IS NOT NULL 
      AND discharge_time IS NOT NULL
)

SELECT 
    occupied_hours * 1.0 / (total_beds * 24) AS bed_utilization_score
FROM usage;


select * from bed_occ;

-- KPI 2: Admission Turnaround Time (ATT)
-- formula: average hours from patient admission to discharge
-- purpose: measure hospital throughput efficiency, flag long stays
CREATE OR REPLACE VIEW att AS 
SELECT 
    AVG(DATEDIFF('hour', admission_time, discharge_time)) AS avg_stay_hours
FROM gold.fact_admissions
WHERE admission_time IS NOT NULL 
  AND discharge_time IS NOT NULL;
select * from att;

-- KPI 3: Surgery Efficiency Index
-- formula: percentage of surgeries that started within ±10 minutes of scheduled time
-- purpose: OT planning effectiveness and surgeon punctuality tracking
create view surgey_efficiny as
SELECT 
    COUNT(CASE 
        WHEN ABS(DATEDIFF('minute', scheduled_time, start_time)) <= 10 
        THEN 1 END) * 100.0
    / NULLIF(COUNT(*),0) AS surgery_efficiency_index
FROM gold.fact_procedures;

select * from surgey_efficiny;

-- KPI 4: Doctor Workload Index
-- formula: average number of daily tasks (admissions + procedures) per doctor
-- UNION ALL combines both workload sources before averaging per doctor per day
-- purpose: identify overloaded doctors, detect staffing gaps, balance workload
CREATE OR REPLACE VIEW kpi_doctor_workload AS
WITH combined AS (
    -- count each admission as one unit of work for the attending doctor
    SELECT 
        doctor_id,
        DATE(admission_time) AS day
    FROM gold.fact_admissions

    UNION ALL

    -- count each procedure as one unit of work for the surgeon
    SELECT 
        doctor_id,
        DATE(start_time) AS day
    FROM gold.fact_procedures
),

agg AS (
    SELECT 
        doctor_id,
        day,
        COUNT(*) AS daily_work
    FROM combined
    GROUP BY doctor_id, day
)

SELECT 
    doctor_id,
    AVG(daily_work) AS doctor_workload_index
FROM agg
GROUP BY doctor_id;


-- KPI 5: Billing Accuracy Index
-- formula: 1 - (number of billing anomalies / total bills)
-- anomaly definition: total_amount < 0 (negative) OR > 100,000 (unusually high)
-- purpose: fraud detection, revenue assurance, insurance partner transparency
CREATE OR REPLACE VIEW kpi_data_quality AS
WITH total_bills AS (
    SELECT COUNT(*) AS total FROM gold.fact_billing
),
anomalies AS (
    SELECT COUNT(*) AS anomaly_count
    FROM gold.fact_billing
    WHERE total_amount < 0 
       OR total_amount > 100000
)

SELECT 
    1 - (anomaly_count * 1.0 / NULLIF(total,0)) 
    AS billing_accuracy_index
FROM total_bills, anomalies;