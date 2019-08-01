/*
Input: 
    {database}.dm_daily_sales
Output:  
    {database}.dm_pattern_percentage 
*/

-- 2. 
-- drop table if exists {database}.dm_pattern_percentage;

create view {database}.dm_pattern_percentage as 
with dm_pattern as (
SELECT
    *,
    datediff(to_timestamp(date_key, 'yyyyMMdd'), theme_start_date) as day_number
from {database}.dm_daily_sales 
where dm_pattern_code is not null
),

pattern_subfamily_store_sales as (
select
    sub_family_code,
    store_code,
    dm_pattern_code,
    sum(daily_sales_sum) as daily_sales_sum_dm

from dm_pattern
group by
    sub_family_code,
    store_code,
    dm_pattern_code
),

pattern_subfamily_store_sales_day as (
select
    sub_family_code,
    store_code,
    dm_pattern_code,
    day_number,
    sum(daily_sales_sum) as daily_sales_sum_dm_day

from dm_pattern
group by
    sub_family_code,
    store_code,
    day_number,
    dm_pattern_code
),

combine as (
select
    a.*,
    b.daily_sales_sum_dm

from pattern_subfamily_store_sales_day a
left join pattern_subfamily_store_sales b
on b.sub_family_code = a.sub_family_code
and b.store_code = a.store_code
and b.dm_pattern_code = a.dm_pattern_code
)

select
    *,
    if(daily_sales_sum_dm = 0, 0, daily_sales_sum_dm_day / daily_sales_sum_dm)  as dm_day_percentage

from combine
;
