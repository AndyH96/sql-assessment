with week_boolean as (
    select
        pe.case_id,
        pe.patient_user_id,
        if(week1_engagements  >1, 1, 0) week1,
        if(week2_engagements  >1, 1, 0) week2,
        if(week3_engagements  >1, 1, 0) week3,
        if(week4_engagements  >1, 1, 0) week4,
        if(week5_engagements  >1, 1, 0) week5,
        if(week6_engagements  >1, 1, 0) week6,
        if(week7_engagements  >1, 1, 0) week7,
        if(week8_engagements  >1, 1, 0) week8,
        if(week9_engagements  >1, 1, 0) week9,
        if(week10_engagements  >1, 1, 0) week10,
        if(week11_engagements  >1, 1, 0) week11,
        if(week12_engagements  >1, 1, 0) week12,
        if(week9_appointments  >1, 1, 0) week9_appointments,
        if(week12_appointments >1, 1, 0) week12_appointments,
    from `patient_engagement` pe
),
-- count attended appointments and create logic for week 12 and week 9
weeks_attended as (
    select *,
        (week1 + week2 + week3 + week4 + week5 + week6 + week7 + week8 + week9 + week10 + week11 + week12)
        + if(week12_appointments = 1,2 - if(week11 = 1,1,0) - if(week10 = 1,1,0)0)
        + if(week9_appointments  = 1,2 - if(week8  = 1,1,0) - if(week7  = 1,1,0)0)
    weeks_attended
    from week_boolean
),
cs as (
    select 
        c.id as case_id,
        c.patient_user_id,
        c.date_created as referral_date,
        date_trunc(c.date_created_month) as referral_month,
        con.contract_name,
    from `case_te` c
    inner join `contracts` con on con.id = c.contract_id
    inner join `user_te` u on u.user_id = c.patient_user_id
    where not lower (concat(u.first_name, u.last_name)) like '%duplicate%'
      and not lower (concat(u.first_name, u.last_name)) like '%test%'
      and not lower (u.email) like '@oviva.com' or u.email is null)
)

select 
    weeks_attended.*,
    contract_name,
    referral_date,
    referral_month,
from cs
inner join weeks_attended as wa on wa.case_id = cs.case_id
