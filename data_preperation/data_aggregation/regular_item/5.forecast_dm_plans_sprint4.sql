/*
Description: Create table "{database}.forecast_dm_plans_sprint4",
             create a table containing all the validated dm_theme_ids and the item list in the dm,
             also everything planned, for example, slot_type_code.....

Input:  ods.nsa_dm_theme
        nsa.dm_extract_log

Output: {database}.forecast_dm_plans_sprint4
Created: 2019-07-01
*/

-- drop table if exists {database}.forecast_dm_plans_sprint4;

create table {database}.forecast_dm_plans_sprint4 as
with extract_order_max as (
select
    dm_theme_id,
    max(extract_order) as latest_extract_order 
    
from nsa.dm_extract_log 
where extract_datetime >= to_timestamp({starting_date}, 'yyyyMMdd')
and extract_order between 40 and 50
group by dm_theme_id 
),

extract_info as (
select 
    b.*
from extract_order_max a 
left join nsa.dm_extract_log b 
on a.dm_theme_id = b.dm_theme_id
and a.latest_extract_order = b.extract_order
),

active_dm as (
select  
    *
from ods.nsa_dm_theme
where effective_year >= cast(substr(cast({starting_date} as string), 1, 4) as int)
and theme_status <> '-1'     -- -1 means not validated
) 

select 
    a.dm_theme_id as dm_theme,
    a.theme_start_date,
    a.theme_end_date,
    a.theme_cn_desc,
    a.theme_en_desc,
    a.effective_year,
    a.theme_status,
    a.nl,
    a.theme_pages,
    b.* 

from active_dm a 
left join extract_info b 
on a.dm_theme_id = b.dm_theme_id
;

-- INVALIDATE METADATA {database}.forecast_dm_plans_sprint4;
