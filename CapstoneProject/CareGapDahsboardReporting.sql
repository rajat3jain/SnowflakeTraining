SELECT
    COUNT(*) AS total_care_gaps
FROM
    care_gaps;




SELECT
    
    COUNT(*) AS total_gaps
FROM
    care_gaps
GROUP BY
    patient_id
HAVING
    COUNT(*) > 1
ORDER BY
    total_gaps DESC;
