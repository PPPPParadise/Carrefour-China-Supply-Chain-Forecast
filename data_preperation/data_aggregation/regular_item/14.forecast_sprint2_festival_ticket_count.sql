/*
Description: Create table "{database}.festival_mapping_flag"
             create the relationship between date_key and festival type
             and count how many ticket_ids are made during the festival period in each year

Input:  {database}.forecast_trxn_flag_v1_sprint4
        {database}.chinese_festivals     -- This table is created by Artefact

Output: {database}.forecast_sprint2_festival_ticket_count_flag
*/

-- drop table if exists {database}.forecast_sprint2_festival_ticket_count_flag;

create table {database}.forecast_sprint2_festival_ticket_count_flag as
-- 416521 row(s)
with daily_ticket_count as (
select
    date_key,
    count(ticket_id) as ticket_count,
    item_id,
    sub_id,
    store_code

from {database}.forecast_trxn_flag_v1_sprint4
group by date_key, item_id, sub_id, store_code
),

add_festival_type as (
select
    a.*,
    b.festival_type
from daily_ticket_count a
inner join {database}.chinese_festivals b
on b.date_key = a.date_key
),

tb1 as (
select
    item_id,
    sub_id,
    store_code,
    festival_type,
    substring(date_key,1,4) as year_key,
    sum(ticket_count) as ticket_count

from add_festival_type
group by item_id, sub_id, store_code, festival_type, year_key
)

select
    *
from tb1
;

-- INVALIDATE METADATA {database}.forecast_sprint2_festival_ticket_count_flag;
