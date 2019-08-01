/*
Input: 
    {database}.forecast_sprint3_v10_flag_sprint4
    ods.dim_calendar
    {database}.forecast_sprint4_add_dm_to_daily

Output: {database}.forecast_regular_day
*/

-- 1. Select regular records
create view {database}.forecast_regular_day as
with week_day as (
    select
        date_key,
        week_key
    from ods.dim_calendar
    where date_key >= '{starting_date}'
    and date_key < '{ending_date}'
    group by
        date_key,
        week_key
),

regular_week as (
    SELECT
        item_id,
        sub_id,
        store_code,
        week_key,
        group_family_code,
        family_code,
        sub_family_code

    from {database}.forecast_sprint3_v10_flag_sprint4
    where holiday_count_new = 0   -- no holiday 
    and ind_current_on_dm_flag = 0   -- no dm 
    and sales_qty_sum <> 0   -- at least sold  once 
    and week_key > (select min(week_key) from week_day)
    and week_key < (select max(week_key) from week_day)
),

add_week_key as (
    select
        a.*,
        b.week_key
    from {database}.forecast_sprint4_add_dm_to_daily a
    left join week_day b
    on b.date_key = a.date_key
),

week_to_regular_day as (
select
    a.*,
    b.item_id as regular_flag

from add_week_key  a
left join regular_week b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.store_code = a.store_code
and b.week_key = a.week_key
)
select
    item_id,
    sub_id,
    store_code,
    date_key,
    out_of_stock_flag,
    daily_sales_sum,
    dept_code,
    item_code,
    sub_code,
    week_key,
    dayofweek(to_timestamp(date_key, 'yyyyMMdd')) as day_of_week

from week_to_regular_day
where regular_flag is not null  
and date_key >= '{starting_date}'
and date_key < '{ending_date}'
;
