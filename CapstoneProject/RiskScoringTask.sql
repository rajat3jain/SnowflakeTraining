-----UDFS

CREATE OR REPLACE FUNCTION calculate_age_factor(date_of_birth DATE)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) > 60 THEN CAST(1 AS FLOAT)
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) BETWEEN 50 AND 60 THEN CAST(0.8 AS FLOAT)
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) BETWEEN 40 AND 50 THEN CAST(0.6 AS FLOAT)
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) BETWEEN 30 AND 40 THEN CAST(0.4 AS FLOAT)
        ELSE CAST(0.2 AS FLOAT)
    END
$$;



CREATE OR REPLACE FUNCTION calculate_claim_factor(claim_amount FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN claim_amount > 10000 THEN CAST(1 AS FLOAT)
        WHEN claim_amount > 5000 THEN CAST(0.75 AS FLOAT)
        WHEN claim_amount > 2000 THEN CAST(0.5 AS FLOAT)
        ELSE CAST(0.2 AS FLOAT)
    END
$$;



CREATE OR REPLACE FUNCTION calculate_event_factor(event_type STRING)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN event_type = 'Surgery' THEN CAST(1 AS FLOAT)
        WHEN event_type = 'Emergency' THEN CAST(0.8 AS FLOAT)
        ELSE CAST(0.4 AS FLOAT)
    END
$$;


---------- stored proc 




-- SELECT COUNT(DISTINCT pd.patient_id) AS missing_in_claims
-- FROM patient_data pd
-- LEFT JOIN claims_data cd ON pd.patient_id = cd.patient_id
-- WHERE cd.patient_id IS NULL;



-- SELECT COUNT(DISTINCT pd.patient_id) AS missing_in_medical_events
-- FROM patient_data pd
-- LEFT JOIN medical_events me ON pd.patient_id = me.patient_id
-- WHERE me.patient_id IS NULL;


-- SELECT COUNT(DISTINCT pd.patient_id) AS missing_in_pharmacy
-- FROM patient_data pd
-- LEFT JOIN pharmacy_data pd_data ON pd.patient_id = pd_data.patient_id
-- WHERE pd_data.patient_id IS NULL;



-- SELECT COUNT(DISTINCT pd.patient_id) AS missing_in_pharmacy
-- FROM patient_data pd
-- LEFT JOIN provider_data pd_data ON pd.patient_id = pd_data.patient_id
-- WHERE pd_data.patient_id IS NULL;






-- select *  from healthcare.raw.patient_data;

-- select * from healthcare.raw.medical_events;

-- select * from healthcare.raw.claims_data;

-- select * from healthcare.raw.provider_data;

-- select * from healthcare.raw.pharmacy_data;


-- select * from risk_scores_table;




-- delete from healthcare.raw.patient_data;
-- delete from healthcare.raw.patient_data;

-- delete from claims_data;

-- delete from provider_data;
-- delete from pharmacy_data;


CALL calculate_risk_scores();



SELECT COUNT(DISTINCT patient_id) AS total_patients FROM medical_events;


select * from medical_events

select * from claims_data

SELECT COUNT(*) FROM risk_scores_table;

--- Stored Proc lastest working 


CREATE OR REPLACE PROCEDURE calculate_risk_scores()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS
$$
  var sql_command;
  var patient_id;
  var age_factor;
  var claim_factor;
  var event_factor;
  var raw_risk_score;
  var normalized_risk_score;

  // Cursor to fetch unique patient_ids
  var stmt = snowflake.createStatement({sqlText: "SELECT DISTINCT patient_id FROM patient_data"});
  var result_set = stmt.execute();
  
  // Loop through each patient
  while (result_set.next()) {
    patient_id = result_set.getColumnValue(1);

    // Calculate the age factor using the UDF
    sql_command = `SELECT calculate_age_factor(date_of_birth) FROM patient_data WHERE patient_id = '${patient_id}'`;
    var age_result = snowflake.createStatement({sqlText: sql_command}).execute();
    age_factor = age_result.next() ? age_result.getColumnValue(1) : null;

    // Calculate the claim factor using the UDF (sum of claims for the patient)
    sql_command = `SELECT SUM(calculate_claim_factor(claim_amount)) FROM claims_data WHERE patient_id = '${patient_id}'`;
    var claim_result = snowflake.createStatement({sqlText: sql_command}).execute();
    claim_factor = claim_result.next() ? claim_result.getColumnValue(1) : null;

    // Calculate the event factor using the UDF (sum of events for the patient)
    sql_command = `SELECT SUM(calculate_event_factor(event_type)) FROM medical_events WHERE patient_id = '${patient_id}'`;
    var event_result = snowflake.createStatement({sqlText: sql_command}).execute();
    event_factor = event_result.next() ? event_result.getColumnValue(1) : null;

    // Log intermediate values for debugging
    console.log(`Patient ID: ${patient_id}, Age Factor: ${age_factor}, Claim Factor: ${claim_factor}, Event Factor: ${event_factor}`);

    // Check for null factors and skip this patient if any are null
    if (age_factor == null || claim_factor == null || event_factor == null) {
        console.log(`Skipping Patient ID ${patient_id} due to null factors.`);
        continue; // Skip this patient
    }

    // Calculate the raw risk score
    raw_risk_score = age_factor + claim_factor + event_factor;

    // Normalize the risk score to be between 0 and 1
    normalized_risk_score = Math.max(Math.min((raw_risk_score - 0) / (3 - 0), 1), 0); // Normalize to range [0, 1]

    // Insert the normalized risk score and the current timestamp into the risk_scores_table
    sql_command = `INSERT INTO risk_scores_table (patient_id, risk_score, created_at) 
                   VALUES ('${patient_id}', ${normalized_risk_score}, CURRENT_TIMESTAMP)`;
    
    try {
        snowflake.createStatement({sqlText: sql_command}).execute();
        console.log(`Inserted Patient ID ${patient_id} with Risk Score: ${normalized_risk_score}`);
    } catch (err) {
        console.error(`Error inserting Patient ID ${patient_id}: ${err}`);
    }
  }

  return 'Risk scores calculated and inserted successfully';
$$
;


----------task

CREATE OR REPLACE TASK calculate_risk_scores_task
  WAREHOUSE = compute_wh  -- Specify the warehouse to use
  SCHEDULE = 'USING CRON 0 */9 * * * UTC'  -- Runs every 9 hours at minute 0
AS
CALL calculate_risk_scores();


