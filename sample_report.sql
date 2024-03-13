WITH participant_contracts AS (
    SELECT 
        con.contract_identifier AS contract_type,
        u.unique_identifier AS user_reference,
        c.case_reference,
        c.creation_timestamp,
        DATE_TRUNC('month', c.creation_timestamp) AS creation_month,
        c.current_status
    FROM 
        `cases` c
        INNER JOIN `contracts` con ON con.id = c.contract_id
        INNER JOIN `participants` u ON u.id = c.user_id
    WHERE 
        NOT LOWER(CONCAT(u.first_name_masked, u.last_name_masked)) LIKE '%duplicate%'
        AND NOT LOWER(CONCAT(u.first_name_masked, u.last_name_masked)) LIKE '%test%'
        AND (NOT LOWER(u.email_masked) LIKE '%example.com' OR u.email_masked IS NULL)
),
initial_engagement AS (
    SELECT 
        ch.case_reference,
        MIN(ch.timestamp) AS initial_engagement_date
    FROM 
        participant_contracts
        LEFT JOIN `case_history` ch ON ch.case_reference = participant_contracts.case_reference
    WHERE 
        ch.next_status = 'Awaiting Treatment'
    GROUP BY 
        ch.case_reference
),
appointment_counts AS (
    SELECT
        aa.case_reference,
        COUNT(aa.case_reference) AS total_appointments
    FROM 
        `appointments` aa
    GROUP BY 
        aa.case_reference
),
status_summary AS (
    SELECT
        aa.case_reference,
        aa.encounter_status,
        IF(aa.no_show = false, 1, 0) AS attended
    FROM 
        `appointments` aa
    PIVOT (
        SUM(attended) FOR encounter_status IN ('Initial', 'Week1', 'Week2', 'Week3', 'Week4',
                                               'Week5', 'Week6', 'Week7', 'Week8', 'Week9',
                                               'Week10', 'Week11', 'Week12')
    )
),
pathway_analysis AS (   
    SELECT 
        p.case_reference,
        p.pathway_type,
        ROW_NUMBER() OVER(PARTITION BY p.case_reference ORDER BY p.last_update DESC)
    FROM 
        `pathway_info` p
),
adjusted_appointments AS (
    SELECT
        pa.case_reference,
        ie.initial_engagement_date,
        ac.total_appointments,
        ss.Week9,
        ss.Week12,
        IF(ie.initial_engagement_date IS NULL, false, true) AS has_engaged,
        IF(IFNULL(ac.total_appointments,0) > 0, true, false) AS has_participated,
        CASE
            WHEN pa.pathway_type IN ('Type1', 'Type1App', 'Type1Phone') THEN
                patient_progress.metrics
            ELSE
                ac.total_appointments
                + IFNULL(patient_progress.extra_sessions,0)
                + IFNULL(ss.Week9 > 0, 2 - IF(ss.Week8 > 1, 1, 0) - IF(ss.Week7 > 1, 1, 0))
                + IFNULL(ss.Week12 > 0, 2 - IF(ss.Week11 > 1, 1, 0) - IF(ss.Week10 > 1, 1, 0))
        END AS total_adjusted_appointments,
        IFNULL(pa.pathway_type, latest_pathway.pathway_type) AS pathway_classification
    FROM 
        `participant_aggregate` pa
        LEFT JOIN initial_engagement ie ON pa.case_reference = ie.case_reference
        LEFT JOIN appointment_counts ac ON pa.case_reference = ac.case_reference
        LEFT JOIN status_summary ss ON pa.case_reference = ss.case_reference
        LEFT JOIN `progress_tracking`
        LEFT JOIN pathway_analysis
)

SELECT 
    *,
    CASE
        WHEN contract_type IN ('ContractA', 'ContractB', 'ContractC', 'ContractD')
             AND total_adjusted_appointments >= 8 THEN true
        ELSE IF(total_adjusted_appointments >= 9, true, false)
    END AS completion_status
FROM 
    adjusted_appointments

