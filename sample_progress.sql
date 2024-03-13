WITH engagement_metrics AS (
    SELECT
        pe.unique_id AS engagement_id,
        pe.individual_id,
        IF(engagement_count_week1 > 1, 1, 0) AS metric_week1,
        IF(engagement_count_week2 > 1, 1, 0) AS metric_week2,
        IF(engagement_count_week3 > 1, 1, 0) AS metric_week3,
        IF(engagement_count_week4 > 1, 1, 0) AS metric_week4,
        IF(engagement_count_week5 > 1, 1, 0) AS metric_week5,
        IF(engagement_count_week6 > 1, 1, 0) AS metric_week6,
        IF(engagement_count_week7 > 1, 1, 0) AS metric_week7,
        IF(engagement_count_week8 > 1, 1, 0) AS metric_week8,
        IF(engagement_count_week9 > 1, 1, 0) AS metric_week9,
        IF(engagement_count_week10 > 1, 1, 0) AS metric_week10,
        IF(engagement_count_week11 > 1, 1, 0) AS metric_week11,
        IF(engagement_count_week12 > 1, 1, 0) AS metric_week12,
        IF(special_appointment_week9 > 1, 1, 0) AS special_week9,
        IF(special_appointment_week12 > 1, 1, 0) AS special_week12
    FROM 
        `engagement_data_table` pe
),
attendance_summary AS (
    SELECT
        *,
        (metric_week1 + metric_week2 + metric_week3 + metric_week4 + metric_week5 + 
         metric_week6 + metric_week7 + metric_week8 + metric_week9 + metric_week10 + 
         metric_week11 + metric_week12) + 
         IF(special_week12 = 1, 2 - IF(metric_week11 = 1, 1, 0) - IF(metric_week10 = 1, 1, 0), 0) + 
         IF(special_week9 = 1, 2 - IF(metric_week8 = 1, 1, 0) - IF(metric_week7 = 1, 1, 0), 0) AS total_attendance
    FROM 
        engagement_metrics
),
consolidated_summary AS (
    SELECT 
        c.unique_id AS summary_id,
        c.individual_id,
        c.creation_date AS referral_date,
        DATE_TRUNC('month', c.creation_date) AS referral_month,
        con.contract_identifier
    FROM 
        `case_summary_table` c
        INNER JOIN `contract_table` con ON con.unique_id = c.contract_id
        INNER JOIN `user_summary_table` u ON u.unique_id = c.individual_id
    WHERE 
        NOT LOWER(CONCAT(u.name_first, u.name_last)) LIKE '%duplicate%'
        AND NOT LOWER(CONCAT(u.name_first, u.name_last)) LIKE '%test%'
        AND (NOT LOWER(u.contact_email) LIKE '%example.com' OR u.contact_email IS NULL)
)

SELECT 
    attendance_summary.*,
    contract_identifier,
    referral_date,
    referral_month
FROM 
    consolidated_summary
    INNER JOIN attendance_summary ON attendance_summary.engagement_id = consolidated_summary.summary_id

