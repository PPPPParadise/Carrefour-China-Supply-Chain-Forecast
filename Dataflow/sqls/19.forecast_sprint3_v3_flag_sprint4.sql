/*
Description: Create table "{database}.forecast_sprint3_v2"
             1. Cast next_dm_v5_extract_datetime from timestamp to string
             2. Add assortment information:
                assortment_avg_nsp, assortment_active_flag
Input:  {database}.forecast_sprint2_final_flag_sprint4 
        ods.dim_calendar 
        {database}.forecast_assortment_full
Output: {database}.forecast_sprint3_v3_flag
*/

-- drop table if exists {database}.forecast_sprint3_v3_flag_sprint4;

create table {database}.forecast_sprint3_v3_flag_sprint4  as
-- 8899631 row(s)
with date_week_mapping as (
select
    date_value,
    week_key
from ods.dim_calendar 
),

assortment_add_week_key as (
SELECT
    a.*,
    cast(a.psp as double) as psp_double,
    cast(unit_code as int) as unit_code_int,
    b.week_key
from {database}.forecast_assortment_full a
left join date_week_mapping b
on b.date_value = to_timestamp(a.date_key, 'yyyyMMdd')
),

assortment_info_week as (
select
    store_code,
    item_id,
    sub_id,
    week_key,
    -- count how many days an item is active in a certain week
    sum(if(item_stop='N', 1, 0)) as non_stop_days,
    -- count how many days an item is inactive in a certain week
    sum(if(item_stop='Y', 1, 0)) as stop_days,
    avg(nsp/unit_code_int) as nsp_avg,
    avg(psp_double/unit_code_int) as psp_avg

from assortment_add_week_key
where item_id is not null
group by
    store_code,
    item_id,
    sub_id,
    week_key
)

select
    a.*,
    b.nsp_avg as assortment_avg_nsp,
    -- b.psp_avg as assortment_avg_psp,
    -- If an item is active morn than 7 days in a week, we think it is active
    if(b.non_stop_days >= 7, 1, 0) as assortment_active_flag

from {database}.forecast_sprint2_final_flag_sprint4 a
left join assortment_info_week b
on b.store_code = a.store_code
and b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.week_key = a.week_key
;


-- INVALIDATE METADATA {database}.forecast_sprint3_v3_flag_sprint4;
