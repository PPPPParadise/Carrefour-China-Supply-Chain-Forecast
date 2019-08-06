/*
Input: 
    {database}.forecast_w2d_good_regular_days
Output: 
    {database}.subfamily_store_weekday_percentage
*/

-- 3. store_subfamily weekday-per on regular weeks
create view {database}.subfamily_store_weekday_percentage as
with add_sub_family as (
SELECT
    *,
    concat(dept_code, substr(item_code, 1, 3)) as sub_family_code

from {database}.forecast_w2d_good_regular_days
),

weekday_sales as (
select
    day_of_week,
    sub_family_code,
    store_code,
    sum(daily_sales_sum) as daily_sales_sum_on_weekday

from add_sub_family
group by
    day_of_week,
    store_code,
    sub_family_code
),

all_week_sales as (
select
    sub_family_code,
    store_code,
    sum(daily_sales_sum) as all_daily_sales

from add_sub_family
group by
    store_code,
    sub_family_code
)

select
    a.*,
    b.all_daily_sales,
    if(b.all_daily_sales = 0, 0, a.daily_sales_sum_on_weekday / b.all_daily_sales)  as weekday_subfamily_sales_percentage

from weekday_sales a
left join all_week_sales b
on b.sub_family_code = a.sub_family_code
and b.store_code = a.store_code
;
