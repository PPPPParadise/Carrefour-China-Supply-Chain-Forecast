/*
Input: 
    {database}.forecast_w2d_good_regular_days
Output: 
    {database}.forecast_regular_dayofweek_percentage
*/

-- 3. calculate regular-dayofweek sales percentage 
create view {database}.forecast_regular_dayofweek_percentage as
with all_sales as (
SELECT
    item_id,
    sub_id,
    store_code,
    sum(daily_sales_sum) as all_sales
from {database}.forecast_w2d_good_regular_days
group by
    item_id,
    sub_id,
    store_code
),

day_of_week_sales as (
select
    item_id,
    sub_id,
    store_code,
    day_of_week,
    sum(daily_sales_sum) as day_of_week_all_sales
from {database}.forecast_w2d_good_regular_days
group by
    item_id,
    sub_id,
    store_code,
    day_of_week
)

select
    a.*,
    b.all_sales,
    if(b.all_sales = 0, 0, a.day_of_week_all_sales / b.all_sales)  as weekday_percentage

from day_of_week_sales a
left join all_sales b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
;