CREATE DATABASE healthcare;
CREATE SCHEMA healthcare.raw;
--CREATE SCHEMA healthcare.staging;
--CREATE SCHEMA healthcare.transformed;
--CREATE SCHEMA healthcare.analytics;
CREATE ROLE healthcare_role;
GRANT USAGE ON DATABASE healthcare TO ROLE healthcare_role;
GRANT USAGE ON SCHEMA healthcare.raw TO ROLE healthcare_role;
GRANT USAGE ON SCHEMA healthcare.staging TO ROLE healthcare_role;

GRANT USAGE ON SCHEMA healthcare.raw TO ROLE healthcare_role;
SHOW GRANTS TO ROLE healthcare_role;

CREATE WAREHOUSE healthcare_warehouse
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
GRANT USAGE ON WAREHOUSE healthcare_warehouse TO ROLE healthcare_role;


CREATE TABLE healthcare.raw.patient_data (
  patient_id STRING,
  first_name STRING,
  last_name STRING,
  date_of_birth DATE,
  gender STRING,
  race STRING,
  ethnicity STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  insurance_type STRING,
  primary_care_provider_id STRING
);


CREATE TABLE healthcare.raw.medical_events (
  event_id STRING,
  patient_id STRING,
  event_date TIMESTAMP_NTZ,
  event_type STRING,
  diagnosis_code STRING,
  procedure_code STRING,
  medication_code STRING,
  provider_id STRING,
  facility_id STRING,
  notes STRING
);



CREATE TABLE healthcare.raw.claims_data (
  claim_id STRING,
  patient_id STRING,
  service_date DATE,
  claim_date DATE,
  claim_amount FLOAT,
  claim_status STRING,
  provider_id STRING,
  diagnosis_codes ARRAY,
  procedure_codes ARRAY
);


CREATE TABLE healthcare.raw.pharmacy_data (
  prescription_id STRING,
  patient_id STRING,
  medication_code STRING,
  fill_date DATE,
  days_supply INT,
  quantity FLOAT,
  pharmacy_id STRING
);


CREATE TABLE healthcare.raw.provider_data (
  provider_id STRING,
  provider_name STRING,
  specialty STRING,
  npi_number STRING,
  facility_id STRING
);


CREATE TABLE healthcare.raw.care_gaps (
  gap_id STRING,
  patient_id STRING,
  gap_type STRING,
  identified_date DATE,
  status STRING,
  recommended_action STRING
);


  --- with new aws account on 13-12 
CREATE OR REPLACE STAGE healthcare.raw.medical_events_stage
  URL = 's3://healthcare-analytics-inovalon/medical_events/'
 -- STORAGE_INTEGRATION = my_s3_integration
-- Credentials = (AWS_KEY_ID = '',  )
 CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
  FILE_FORMAT =  my_csv_format   --(TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');





  --- with new aws account on 13-12 
CREATE OR REPLACE STAGE healthcare.raw.patient_data_stage
  URL = 's3://healthcare-analytics-inovalon/patient_data/'
 -- STORAGE_INTEGRATION = my_s3_integration
-- Credentials = (AWS_KEY_ID = '',  )
 CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '/EjO73jmNPtezbh1VeubYsQ')
  FILE_FORMAT =  my_csv_format   --(TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


  

  --- with new aws account on 13-12 
CREATE OR REPLACE STAGE healthcare.raw.claims_data_stage
  URL = 's3://healthcare-analytics-inovalon/claims_data/'
 
 CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
  FILE_FORMAT =  my_csv_format;   --(TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


  

  
CREATE OR REPLACE STAGE healthcare.raw.provider_data_stage
  URL = 's3://healthcare-analytics-inovalon/provider_data/' 
 CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
  FILE_FORMAT =  my_csv_format;   --(TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

  

  
  
CREATE OR REPLACE STAGE healthcare.raw.pharmacy_data_stage
  URL = 's3://healthcare-analytics-inovalon/pharmacy_data/'
 
 CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
  FILE_FORMAT =  my_csv_format;   --(TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


drop stage healthcare.raw.claims_data_stage;
drop stage healthcare.raw.provider_data_stage;
drop stage healthcare.raw.pharmacy_data_stage;



--Creating format and the snowpipe. 

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  DATE_FORMAT = 'YYYY-MM-DD';  -- Specify the expected date format


  

CREATE OR REPLACE PIPE healthcare.raw.medical_events_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO healthcare.raw.medical_events
FROM  @healthcare.raw.medical_events_stage
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';




CREATE OR REPLACE PIPE healthcare.raw.patient_data_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO healthcare.raw.patient_data
FROM  @healthcare.raw.patient_data_stage
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';




CREATE OR REPLACE PIPE healthcare.raw.claims_data_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO healthcare.raw.claims_data
FROM  @healthcare.raw.claims_data_stage
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';




CREATE OR REPLACE PIPE healthcare.raw.provider_data_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO healthcare.raw.provider_data
FROM  @healthcare.raw.provider_data_stage
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';





CREATE OR REPLACE PIPE healthcare.raw.pharmacy_data_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO healthcare.raw.pharmacy_data
FROM  @healthcare.raw.pharmacy_data_stage
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';
