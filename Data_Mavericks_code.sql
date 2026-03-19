--created databes for the use case
create database health_care;

--raw layer bronce layer
create schema bronze;

--storage integration to connect aws and snowflake
CREATE OR REPLACE STORAGE INTEGRATION bronze.s3_int_orders
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::296368270186:role/health_care_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://hospital-health-care/'
  );

  DESC STORAGE INTEGRATION bronze.s3_int_orders;
  --arn:aws:iam::546602405306:user/178e1000-s
  --QB15303_SFCRole=11_DOTNYp0RHYUUwZ7IEWbEhDHIhPE=

--file format to specify in the satges and copy into function  
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

--patients stage
CREATE OR REPLACE STAGE patients_s3_stage
URL = 's3://hospital-health-care/patients/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @patients_s3_stage;

--doctors satge
CREATE OR REPLACE STAGE doctors_s3_stage
URL = 's3://hospital-health-care/doctors/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @doctors_s3_stage;

--admissions stage
CREATE OR REPLACE STAGE admissions_s3_stage
URL = 's3://hospital-health-care/admissions/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @admissions_s3_stage;

--procedures stage
CREATE OR REPLACE STAGE procedures_s3_stage
URL = 's3://hospital-health-care/procedures/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @procedures_s3_stage;

--billing stage
CREATE OR REPLACE STAGE billing_s3_stage
URL = 's3://hospital-health-care/billing/'
STORAGE_INTEGRATION = s3_int_orders
FILE_FORMAT = csv_format;

list @billing_s3_stage;


---raw tables creation--------------------

--patientss raw table----------------------------------------
CREATE OR REPLACE TABLE raw_patients (
  patient_id STRING,
  name STRING,
  dob STRING,
  gender STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  state STRING,
  insurance_id STRING,
  registration_date STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream raw_patients_st on table bronze.raw_patients;

--doctors raw table -----------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_doctors (
  doctor_id STRING,
  name STRING,
  specialization STRING,
  join_date STRING,
  phone STRING,
  email STRING,
  qualification STRING,
  department STRING,
  status STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream raw_doctors_st on table bronze.raw_doctors;

--addmissions raw table -------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_admissions (
  admission_id STRING,
  patient_id STRING,
  admission_time STRING,
  discharge_time STRING,
  department STRING,
  ward STRING,
  bed_no STRING,
  attending_doctor_id STRING,
  admission_type STRING,
  status STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream raw_admissions_st on table raw_admissions;

--proceduress raw table ------------------------------------------------------
CREATE OR REPLACE TABLE raw_procedures (
  procedure_id STRING,
  admission_id STRING,
  patient_id STRING,
  procedure_name STRING,
  surgeon_id STRING,
  scheduled_time STRING,
  start_time STRING,
  end_time STRING,
  operating_room STRING,
  anaesthesia_type STRING,
  status STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream raw_procedures_st on table raw_procedures;

--billing raw table --------------------------------------------------
CREATE OR REPLACE TABLE raw_billing (
  bill_id STRING,
  admission_id STRING,
  patient_id STRING,
  total_amount STRING,
  insurance_amount STRING,
  out_of_pocket_amount STRING,
  billing_time STRING,
  payment_mode STRING,
  is_flagged STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

create or replace stream raw_billing_st on table raw_billing;


--pipe creations for automation

--patients pipe--------------------------
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

alter pipe patients_pipe refresh;
SELECT SYSTEM$PIPE_STATUS('patients_pipe');

select * from bronze.raw_patients;
select * from bronze.raw_patients_st;

--doctor pipe ------------------------------------------------------------
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

--admission pipe ----------------------------------------
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

--procedures pipes-------------------------
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

truncate table bronze.raw_procedures;

alter pipe procedures_pipe refresh ;

select * from bronze.raw_procedures;
select * from bronze.raw_procedures_st;

--billing pipe-------------------------------------------------------
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

--------------------------------------------------SILVER LAYER----------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
create schema silver;
use schema silver;

CREATE OR REPLACE TABLE silver_patients (
  patient_id INT PRIMARY KEY,
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
  load_time TIMESTAMP
);

---doctors
CREATE OR REPLACE TABLE silver_doctors (
  doctor_id INT PRIMARY KEY,
  name STRING,
  specialization STRING,
  join_date DATE,
  phone STRING,
  email STRING,
  qualification STRING,
  department STRING,
  status STRING,
  load_time TIMESTAMP
);

--admissions
CREATE OR REPLACE TABLE silver_admissions (
  admission_id INT PRIMARY KEY,
  patient_id INT,
  admission_time TIMESTAMP,
  discharge_time TIMESTAMP,
  department STRING,
  ward STRING,
  bed_no INT,
  attending_doctor_id INT,
  admission_type STRING,
  status STRING,
  load_time TIMESTAMP,

  FOREIGN KEY (patient_id) REFERENCES silver_patients(patient_id),
  FOREIGN KEY (attending_doctor_id) REFERENCES silver_doctors(doctor_id)
);

--proceduresss tableee
CREATE OR REPLACE TABLE silver_procedures (
  procedure_id INT PRIMARY KEY,
  admission_id INT,
  patient_id INT,
  procedure_name STRING,
  surgeon_id INT,
  scheduled_time TIMESTAMP,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  operating_room STRING,
  anaesthesia_type STRING,
  status STRING,
  load_time TIMESTAMP,

  FOREIGN KEY (admission_id) REFERENCES silver_admissions(admission_id)
);

--billing
CREATE OR REPLACE TABLE silver_billing (
  bill_id INT PRIMARY KEY,
  admission_id INT,
  patient_id INT,
  total_amount FLOAT,
  insurance_amount FLOAT,
  out_of_pocket_amount FLOAT,
  billing_time TIMESTAMP,
  payment_mode STRING,
  is_flagged STRING,
  load_time TIMESTAMP,

  FOREIGN KEY (admission_id) REFERENCES silver_admissions(admission_id)
);

--exception TABLE
CREATE OR REPLACE TABLE exception_table (
  source_table STRING,
  record_id STRING,
  error_type STRING,
  error_message STRING,
  load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

select * from silver.exception_table;

--procedures ----for automation---------------------------------------------------------
CREATE OR REPLACE PROCEDURE silver.load_patients()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN


CREATE OR REPLACE TEMP TABLE temp_patients AS
SELECT * FROM bronze.raw_patients_st;

INSERT INTO silver.exception_table
SELECT 'patients', patient_id, 'DATA_ERROR', 'Invalid patient data', CURRENT_TIMESTAMP
FROM temp_patients
WHERE TRY_TO_NUMBER(patient_id) IS NULL
   OR TRY_TO_DATE(dob) IS NULL
   OR email NOT LIKE '%@%';

CREATE OR REPLACE TEMP TABLE valid_patients AS
SELECT *
FROM temp_patients
WHERE TRY_TO_NUMBER(patient_id) IS NOT NULL
  AND TRY_TO_DATE(dob) IS NOT NULL
  AND email LIKE '%@%';

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


--doctors procedure ---------------
CREATE OR REPLACE PROCEDURE silver.load_doctors()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE cnt INT;
BEGIN

SELECT COUNT(*) INTO :cnt FROM bronze.raw_doctors_st;
IF (cnt < 1) THEN
  RETURN 'No data in doctors stream';
END IF;

CREATE or replace TEMP TABLE temp_doctors AS
SELECT * FROM bronze.raw_doctors_st;

INSERT INTO exception_table
SELECT 'doctors', doctor_id, 'DATA_ERROR', 'Invalid doctor data', CURRENT_TIMESTAMP
FROM temp_doctors
WHERE TRY_TO_NUMBER(doctor_id) IS NULL
   OR TRY_TO_DATE(join_date) IS NULL
   OR email NOT LIKE '%@%';

CREATE or replace TEMP TABLE valid_doctors AS
SELECT * FROM temp_doctors
WHERE TRY_TO_NUMBER(doctor_id) IS NOT NULL
  AND TRY_TO_DATE(join_date) IS NOT NULL
  AND email LIKE '%@%';

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

--admissions
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

CREATE or replace TEMP TABLE temp_adm AS
SELECT * FROM bronze.raw_admissions_st;

INSERT INTO exception_table
SELECT 'admissions', admission_id, 'DATA_ERROR', 'Invalid admission data', CURRENT_TIMESTAMP
FROM temp_adm
WHERE TRY_TO_NUMBER(admission_id) IS NULL
   OR TRY_TO_TIMESTAMP(admission_time) IS NULL
   OR TRY_TO_TIMESTAMP(discharge_time) IS NULL;

CREATE or replace TEMP TABLE valid_adm AS
SELECT * FROM temp_adm
WHERE TRY_TO_NUMBER(admission_id) IS NOT NULL
  AND TRY_TO_TIMESTAMP(admission_time) IS NOT NULL
  AND TRY_TO_TIMESTAMP(discharge_time) IS NOT NULL;

INSERT INTO exception_table
SELECT 'admissions', admission_id, 'BUSINESS_ERROR', 'FK or time issue', CURRENT_TIMESTAMP
FROM valid_adm
WHERE patient_id NOT IN (SELECT patient_id FROM silver_patients)
   OR attending_doctor_id NOT IN (SELECT doctor_id FROM silver_doctors)
   OR discharge_time < admission_time;

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

--procedures
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

CREATE or replace TEMP TABLE temp_proc AS
SELECT * FROM bronze.raw_procedures_st;

INSERT INTO exception_table
SELECT 'procedures', procedure_id, 'DATA_ERROR', 'Invalid procedure data', CURRENT_TIMESTAMP
FROM temp_proc
WHERE TRY_TO_NUMBER(procedure_id) IS NULL;

CREATE or replace TEMP TABLE valid_proc AS
SELECT * FROM temp_proc
WHERE TRY_TO_NUMBER(procedure_id) IS NOT NULL;

INSERT INTO exception_table
SELECT 'procedures', procedure_id, 'BUSINESS_ERROR', 'FK or time issue', CURRENT_TIMESTAMP
FROM valid_proc
WHERE admission_id NOT IN (SELECT admission_id FROM silver_admissions)
   OR end_time < start_time;

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

--billings
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

CREATE or replace TEMP TABLE temp_bill AS
SELECT * FROM bronze.raw_billing_st;

INSERT INTO exception_table
SELECT 'billing', bill_id, 'DATA_ERROR', 'Invalid billing data', CURRENT_TIMESTAMP
FROM temp_bill
WHERE TRY_TO_NUMBER(bill_id) IS NULL
   OR TRY_TO_NUMBER(total_amount) IS NULL;

CREATE or replace TEMP TABLE valid_bill AS
SELECT * FROM temp_bill
WHERE TRY_TO_NUMBER(bill_id) IS NOT NULL
  AND TRY_TO_NUMBER(total_amount) IS NOT NULL;

INSERT INTO exception_table
SELECT 'billing', bill_id, 'BUSINESS_ERROR', 'FK or negative amount', CURRENT_TIMESTAMP
FROM valid_bill
WHERE admission_id NOT IN (SELECT admission_id FROM silver_admissions)
   OR total_amount < 0;

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



---all proceduress
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

-- CALL EACH PROCEDURE AND STORE RESULT
res_patients := (CALL load_patients());
res_doctors := (CALL load_doctors());
res_admissions := (CALL load_admissions());
res_procedures := (CALL load_procedures());
res_billing := (CALL load_billing());

-- RETURN COMBINED STATUS
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

--task
CREATE OR REPLACE TASK healthcare_pipeline_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
AS
  CALL silver.run_full_pipeline();

ALTER TASK SILVER.healthcare_pipeline_task RESUME;


--MASKING
CREATE OR REPLACE TAG silver.pii_tag;
CREATE OR REPLACE MASKING POLICY silver.pii_mask_policy 
AS (val STRING)
RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN') THEN val
    ELSE '****MASKED****'
END;

ALTER TAG silver.pii_tag 
SET MASKING POLICY silver.pii_mask_policy;

--patients
ALTER TABLE silver_patients 
MODIFY COLUMN email SET TAG pii_tag = 'PII';

ALTER TABLE silver_patients 
MODIFY COLUMN phone SET TAG pii_tag = 'PII';

ALTER TABLE silver_patients 
MODIFY COLUMN insurance_id SET TAG pii_tag = 'PII';

--doctors
ALTER TABLE silver_doctors 
MODIFY COLUMN email SET TAG pii_tag = 'PII';

ALTER TABLE silver_doctors 
MODIFY COLUMN phone SET TAG pii_tag = 'PII';



--------------------------------------GOLD SCHEMA-------------------------------------
----------------------------------------------------------------
create schema gold;
USE SCHEMA gold;
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

    effective_start_date DATE,
    effective_end_date DATE,
    current_flag STRING
);

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


CREATE OR REPLACE PROCEDURE gold.dim_patient()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- STEP 1: EXPIRE EXISTING RECORD
UPDATE gold.dim_patient t
SET 
    effective_end_date = CURRENT_DATE - 1,
    current_flag = 'N'
FROM silver.silver_patients s
WHERE t.patient_id = s.patient_id
  AND t.current_flag = 'Y';

-- STEP 2: INSERT LATEST DATA
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
    CURRENT_DATE,
    NULL,
    'Y'
FROM silver.silver_patients s;

RETURN 'DIM_PATIENT SIMPLE SCD DONE';

END;
$$;

call dim_patient();

---------------
CREATE OR REPLACE PROCEDURE gold.dim_doctor()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- EXPIRE OLD
UPDATE gold.dim_doctor t
SET 
    effective_end_date = CURRENT_DATE - 1,
    current_flag = 'N'
FROM silver.silver_doctors s
WHERE t.doctor_id = s.doctor_id
  AND t.current_flag = 'Y';

-- INSERT LATEST
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
    CURRENT_DATE,
    NULL,
    'Y'
FROM silver.silver_doctors s;

RETURN 'DIM_DOCTOR SIMPLE SCD DONE';

END;
$$;

call dim_doctor();
select * from silver.silver_admissions;
select * from bronze.raw_admissions;
select * from gold.dim_patient;
select * from gold.dim_doctor;

----fact tabless
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


----------KPIS---------------
-- 1. bed occupancy & utilization score

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

--att
CREATE OR REPLACE VIEW att AS 
SELECT 
    AVG(DATEDIFF('hour', admission_time, discharge_time)) AS avg_stay_hours
FROM gold.fact_admissions
WHERE admission_time IS NOT NULL 
  AND discharge_time IS NOT NULL;
select * from att;

--surgery efficeny
create view surgey_efficiny as
SELECT 
    COUNT(CASE 
        WHEN ABS(DATEDIFF('minute', scheduled_time, start_time)) <= 10 
        THEN 1 END) * 100.0
    / NULLIF(COUNT(*),0) AS surgery_efficiency_index
FROM gold.fact_procedures;

select * from surgey_efficiny;

--doctor workload
CREATE OR REPLACE VIEW kpi_doctor_workload AS
WITH combined AS (
    SELECT 
        doctor_id,
        DATE(admission_time) AS day
    FROM gold.fact_admissions

    UNION ALL

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


--billing accuracy

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


