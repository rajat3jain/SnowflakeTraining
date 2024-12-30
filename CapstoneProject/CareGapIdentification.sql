---creating view for identifying gaps. 



CREATE OR REPLACE VIEW v_care_gap_no_medication_after_emergency AS
SELECT
    m.patient_id,
    m.event_id,
    m.event_date,
    m.event_type,
    m.diagnosis_code,
    m.procedure_code,
    'Care Gap: No Medication After Emergency' AS gap_type,
    CURRENT_DATE AS identified_date,
    'Open' AS status,
    'Recommend prescribing medication or follow-up care' AS recommended_action
FROM
    medical_events m
LEFT JOIN pharmacy_data p
    ON m.patient_id = p.patient_id
    AND p.fill_date > m.event_date
WHERE
    m.event_type = 'Emergency'
    AND p.prescription_id IS NULL;




CREATE OR REPLACE VIEW v_care_gap_no_follow_up_after_claim AS
SELECT
    c.patient_id,
    c.claim_id,
    c.service_date,
    'Care Gap: No Follow-Up After Claim' AS gap_type,
    CURRENT_DATE AS identified_date,
    'Open' AS status,
    'Recommend follow-up visit or additional care' AS recommended_action
FROM
    claims_data c
LEFT JOIN medical_events m
    ON c.patient_id = m.patient_id
    AND m.event_date > c.service_date
WHERE
    m.event_id IS NULL;


--create tasks 


CREATE OR REPLACE TASK insert_care_gaps_task
  WAREHOUSE = compute_WH
  SCHEDULE = 'USING CRON 0 */9 * * * UTC'  -- Run every 9 hours
AS
  -- Insert Care Gap: No Medication After Emergency
  INSERT INTO care_gaps (gap_id, patient_id, gap_type, identified_date, status, recommended_action)
  SELECT
      CONCAT('GAP_', m.patient_id, '_', m.event_id) AS gap_id,
      m.patient_id,
      m.gap_type,
      m.identified_date,
      m.status,
      m.recommended_action
  FROM
      v_care_gap_no_medication_after_emergency m;

  -- Insert Care Gap: No Follow-Up After Claim
  INSERT INTO care_gaps (gap_id, patient_id, gap_type, identified_date, status, recommended_action)
  SELECT
      CONCAT('GAP_', c.patient_id, '_', c.claim_id) AS gap_id,
      c.patient_id,
      c.gap_type,
      c.identified_date,
      c.status,
      c.recommended_action
  FROM
      v_care_gap_no_follow_up_after_claim c;




