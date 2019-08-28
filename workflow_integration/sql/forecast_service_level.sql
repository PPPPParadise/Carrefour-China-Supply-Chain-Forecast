/* ===================================================
                       Module 3
                    Service level
      Step 2: Applying IT rules + safety net
======================================================
*/

-- From: vartefact.service_level_calculation
-- To: _
-- Usage: service level = 1 if some conditions are met,
--        then if we still have an issue we use 0.6 as a safety
-- Note: _

create table vartefact.service_level_safety2_vinc as
with 
    p4cm_store_item_preprocess as
    (
        select
            dept_code,
            item_code,
            sub_code,
            store_code,
            date_key,
            (case 
                when item_stop = 'Y' or sub_stop = 'Y'
                then 1 else 0 
            end) as item_stop
        from ods.p4cm_store_item
        where store_code in (select stostocd from vartefact.forecast_store_code_scope_sprint4)
            and substring(store_code,1,1) = '1'  
            and date_key >= '20190101'
            and item_code in (select item_code from vartefact.forecast_item_details)
    ),
    item_store_active as
    (
        select
            dept_code,
            item_code,
            sub_code,
            store_code,
            date_key,
            (case when (sum(item_stop) OVER ( PARTITION BY item_code, sub_code, store_code, dept_code
                ORDER BY date_key asc
                ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) ) > 0
                then 1 else 0 
            end) AS item_month_stop
        from p4cm_store_item_preprocess
    ),
    has_item_been_stopped_last_month as
    (
        select
            dept_code,
            item_code,
            sub_code,
            date_key,
            -- be 1 only if item inactive in all stores once during last month
            min(item_month_stop) as item_month_stop     
        from item_store_active
        group by dept_code, item_code, sub_code, date_key
    ),
    adding_stop_flag as
    (
        select
            a.*,
            b.item_month_stop
        from vartefact.service_level_calculation2 a
        left join has_item_been_stopped_last_month b
            on a.dept_code = b.dept_code
            and a.item_code = b.item_code
            and a.sub_code = b.sub_code
            and a.date_key = b.date_key
    ),
    vincent_rules as
    (
        select
            item_id,
            dept_code,
            item_code,
            sub_code,
            -- supplier_code,
            date_key,
            case 
                when (  item_month_stop = 1 
                        or last_trxns_count < 8
                        or abs(datediff(now(),to_timestamp(date_key,'yyyyMMdd'))) > 62)
                then 1 else service_level 
            end as service_level
        from adding_stop_flag
    ),
    safety_net as
    (
        select
            item_id,
            dept_code,
            item_code,
            sub_code,
            -- supplier_code,
            date_key,
            case when service_level < 0.6 then 0.6 else service_level end as service_level
        from vincent_rules
    )
select 
    *
from safety_net
