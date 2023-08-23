with phe_t2_patients as (
    select 
        con.contract_name,
        u.user_id,
        c.id,
        c.date_created,
        date_trunc(c.date_created, MONTH)
        c.state,
        c.closure_reason,
        c.pathway,
    from `data-warehouse-prod-308513.mart_gb_data_analyst.hcs_gb_case_te` c
    inner join `data-warehouse-prod-308513.mart_gb_data_analyst.hcs_gb_contract_te` con
    inner join `data-warehouse-prod-308513.mart_gb_data_analyst.core_user_te` u
    where c.contract_id in (149, 150, 151, 152, 153, 158, 159, 161, 168, 183, 185, 193, 195)
    and not lower (concat(u.first_name, u.last_name)) like '%duplicate%'
    and not lower (concat(u.first_name, u.last_name)) like '%test%'
    and not lower (u.email) like '@oviva.com' or u.email is null
),
ia as (
    select 
        ch.case_id,
        min(ch.date_created)
    from phe_t2_patients
    left join `data-warehouse-prod-308513.mart_gb_data_analyst.hcs_gb_case_history_te` ch
    where ch.state_after = 'TREATMENT_PENDING'
    group by ch.case_id
),
number_of_appts as (
    select
    aa.case_id,
    count(aa.case_id),
    from `data-warehouse-prod-308513.mart_gb_ops.t2wm.all_attended_appointments` aa
    group by aa.case_id,
    
