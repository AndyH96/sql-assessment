with people as (
    select 
        con.contract_name,
        u.user_id,
        c.id,
        c.date_created,
        date_trunc(c.date_created, MONTH)
        c.state,
    from `case_te` c
    inner join `contract_te` con
    inner join `user_te` u
    where not lower (concat(u.first_name, u.last_name)) like '%duplicate%'
    and not lower (concat(u.first_name, u.last_name)) like '%test%'
    and not lower (u.email) like '@sample.com' or u.email is null
),
ia as (
    select 
        ch.case_id,
        min(ch.date_created)
    from people
    left join `case_history_te` ch
    where ch.state_after = 'TREATMENT_PENDING'
    group by ch.case_id
),
number_of_appts as (
    select
        aa.case_id,
        count(aa.case_id),
    from `attended_appointments` aa
    group by aa.case_id
),
attendance_by_bu_status as (
    select
        *
    from (
        select
            aa.case_id,
            aa.status,
            if(aa.dna = false, 1, 0)
        from `attended_appointments` aa
    )
    pivot (sum(attended)for status in ('INITIAL_APPT', 'WEEK_1', 'WEEK_2', 'WEEK_3','WEEK_4',
                                        'WEEK_5', 'WEEK_6', 'WEEK_7', 'WEEK_8', 'WEEK_9',
                                        'WEEK_10', 'WEEK_11', 'WEEK_12'))
),
pathways as (   
    select 
        p.case_id,
        p.pathway,
        row_number()over(partition by p.case_id) order by p._dwh_last_update desc
    from `case_pathways` 
),
appts_adjusted as (
    select
        p.* except(pathway),
        ia.enrolment_date,
        number_of_appts.number_of_appts_attended,
        attendance_by_bu_status.WEEK_9,
        attendance_by_bu_status.WEEK_12,
        if(ia.enrolment_date is null, false, true) enrolled,
        if(ifnull(number_of_appts.number_of_appts_attended,0) > 0, true false) participant,
        case
        -- if level 1 use engagement + appointment combo from patient progress table
            when if (p.pathway,most_recent_pathway.pathway) in ('Level 1',
                                                                'Level 1 App',
                                                                'Level 1 Phone')
        then pp.weeks_attended
        -- level 2/3 keep adjustments for week 9 and 12 but also inclide extra week for 20 min phone calls (tracking changed from 2x10 to 1x20)
        else
        -- if attended week 9 backfill for week 8 and 7. If attended week 8 or 7 subtract appointments to avoid over counting
            number_of_appts.number_of_appts_attended
            + ifnull(ppath.extra_appointments,0) -- add extra for phone pathway, return 0 if not phone.
            + if null(attendance_by_bu_status.WEEK_9 > 0, 2  - if(attendance_by_bu_status.WEEK_8 >1, 1, 0)   - if(attendance_by_bu_status.WEEK_7 >1, 1, 0) 
            + if null(attendance_by_bu_status.WEEK_12 > 0, 2 - if(attendance_by_bu_status.WEEK_11 >1, 1, 0) - if(attendance_by_bu_status.WEEK_10 >1, 1, 0) 
        end as number_of_appts_attended_adjusted,
        ifnull (p.pathway, most_recent_pathway.pathway) as pathway
    from phe_t2_patients as p 
    left join ia
    left join number_of_appts
    left join attendance_by_bu_status
    left join `patient_progress`
    left join phone pathway

select 
    *,
    case
        when contract_name in ('Sample1',
                               'Sample2',
                               'Sample3',
                               'Sample4')
        and number_of_appts_attended_adjusted >= 8 then true
        else if(number_of_appts_attended_adjusted >= 9, true false)
        end as completed
from appts_adjusted