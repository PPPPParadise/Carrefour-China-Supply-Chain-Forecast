/*
Description: Create table "{database}.forecast_sprint2_trxn_week_features_flag"
             Add statistical features to the weekly aggregated table:
             For example:
             planned_bp_sold_week, planned_bp_flag, curr_psp_start_dow, fam_sales_qty_mean_4w,
             item_store_mean_52points, psp_nsp_ratio...

Input:  
        ods.dim_calendar
        {database}.forecast_sprint4_day_to_week_test 
        {database}.temperature_info_fc1
        pricing.competitor_dm_list
        {database}.lastest_active_status

Output: {database}.forecast_sprint2_trxn_week_features_flag
*/

-- drop table if exists {database}.forecast_sprint2_trxn_week_features_flag_sprint4;

create table {database}.forecast_sprint2_trxn_week_features_flag_sprint4 as
-- 8899631 row(s)
-- with planned_bp_sold_date as (
-- select
--     plan_bp.store_code,
--     plan_bp.item_id,
--     plan_bp.sub_id,
--     plan_bp.planned_bp_sold_week

-- from (  SELECT
--             a.store_code,
--             a.item_id,
--             a.sub_id,
--             b.week_key as planned_bp_sold_week

--         from {database}.forecast_supplier_bp_day_flag a
--         left join ods.dim_calendar b
--         on b.date_value = a.trxn_date
--     ) plan_bp
-- group by store_code, item_id,  sub_id, planned_bp_sold_week
-- )

select
    a.*,
    -- if(s.planned_bp_sold_week is not null, 1, 0) as planned_bp_flag,  -- newly added planned_bp_flag
    -- -- 这一列在后续有没有用到？？
    
    substring(a.sub_family_code, 1, 3) as group_family_code,

    (sum(case when coupon_disc_sum > 0 or promo_disc_sum > 0 or mpsp_disc_sum > 0 then 1 else 0 end)
        over (partition by a.store_code,a.item_id,a.sub_id order by a.week_key asc rows between unbounded preceding and 1 preceding)) /
    nullif((sum(case when a.sales_qty_sum > 0 then 1 else 0 end)
        over (partition by a.store_code,a.item_id,a.sub_id order by a.week_key asc rows between unbounded preceding and 1 preceding)), 0)
        as disc_ratio,

    sum(a.sales_qty_sum) over (partition by a.store_code,a.item_id, a.sub_id, a.trxn_year order by a.week_key asc
                                rows between unbounded preceding and 1 preceding) /
                                nullif(e.fam_store_sales_qty_cum, 0) as sales_qty_fam_store_wgt,

    b.dow as curr_psp_start_dow,
    c.dow as curr_psp_end_dow,
    datediff(c.date_value, b.date_value) as curr_psp_days,

    ((cast(substring(a.week_key,3,2) as int) - 16) *52 + cast(substring(a.week_key,5,2) as int))
        - ((cast(substring(b.week_key,3,2) as int) - 16) *52 + cast(substring(b.week_key,5,2) as int)) as curr_psp_start_week_count,

    ((cast(substring(c.week_key,3,2) as int) - 16) *52 + cast(substring(c.week_key,5,2) as int))
        - ((cast(substring(a.week_key,3,2) as int) - 16) *52 + cast(substring(a.week_key,5,2) as int)) as curr_psp_end_week_count,

    (case when f.week_key <= a.week_key then ((cast(substring(d.week_key,3,2) as int) - 16) *52 + cast(substring(d.week_key,5,2) as int))
        - ((cast(substring(a.week_key,3,2) as int) - 16) *52 + cast(substring(a.week_key,5,2) as int))
        else null end) as next_dm_start_week_count,

    g.fam_sales_qty_mean_4w,
    g.fam_sales_qty_mean_12w,
    g.fam_sales_qty_mean_24w,
    g.fam_sales_qty_mean_52w,
    g.fam_sales_qty_min_roll,
    g.fam_sales_qty_max_roll,

    h.subfam_sales_qty_mean_4w,
    h.subfam_sales_qty_mean_12w,
    h.subfam_sales_qty_mean_24w,
    h.subfam_sales_qty_mean_52w,

    -- i.fam_sales_qty_cum,
    sum(a.sales_qty_sum) over (partition by a.item_id,a.sub_id,a.trxn_year order by a.week_key asc rows between unbounded preceding and 1 preceding) /
        nullif(i.fam_sales_qty_cum, 0) as sales_qty_fam_wgt,

    j.item_store_mean_4points,
    j.item_store_mean_12points,
    j.item_store_mean_24points,
    j.item_store_mean_52points,

    k.high_temp_month,
    k.low_temp_month,

    if (l.competitor_item_id is null, 0, 1) as ind_competitor,
    m.week_begin_date,
    m.week_end_date,
    a.current_dm_psp/a.current_dm_nsp as psp_nsp_ratio,

    n.active_flag,   


    datediff(to_timestamp(a.next_dm_end_date, 'yyyy-MM-dd'),
            to_timestamp(a.next_dm_start_date, 'yyyy-MM-dd')) as next_dm_days,   -- How long will the next dm last

    p.next_dm_w1_sales_qty,   -- will be used in prom model
    q.next_dm_w2_sales_qty, -- will be used in prom model

    r.last_year_sales_qty
 --  r.last_year_ticket_count

from {database}.forecast_sprint4_day_to_week_test a

left join (select
                cast(to_date(date_value) as string) as date_value_str,
                dow,
                week_key,
                date_value

            from ods.dim_calendar
            group by date_value_str, dow, week_key, date_value
            ) as b
on b.date_value_str = a.current_dm_psp_start_date

left join ( select
                cast(to_date(date_value) as string) as date_value_str,
                dow,
                week_key,
                date_value
            from ods.dim_calendar
            group by date_value_str, dow, week_key, date_value
            ) as c
on c.date_value_str = a.current_dm_psp_end_date

left join( select
                cast(to_date(date_value) as string) as date_value_str,
                dow,
                week_key,
                date_value

            from ods.dim_calendar
            group by date_value_str, dow, week_key, date_value
            ) as d
on  d.date_value = a.next_dm_start_date


left join( select
                week_key,
                store_code,
                family_code,
                trxn_year,
                sum(sum(sales_qty_sum)) over (partition by store_code, family_code, trxn_year
                                                order by week_key asc rows between unbounded preceding and 1 preceding)
                                            as fam_store_sales_qty_cum

            from {database}.forecast_sprint4_day_to_week_test
            group by family_code, store_code, week_key, trxn_year
            ) as e
on a.family_code = e.family_code
and a.store_code = e.store_code
and a.week_key = e.week_key
and a.trxn_year = e.trxn_year

-- filter next DM info if release date after
left join ( select
                date_value,
                week_key
            from ods.dim_calendar
            group by date_value, week_key
            ) as f
on  f.date_value = to_date(a.next_dm_v5_extract_datetime)


-- family rolling mean, max, min
left join
(select
    week_key,
    store_code,
    family_code,
    avg(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between 4 preceding and 1 preceding) as fam_sales_qty_mean_4w,
    avg(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between 12 preceding and 1 preceding) as fam_sales_qty_mean_12w,
    avg(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between 24 preceding and 1 preceding) as fam_sales_qty_mean_24w,
    avg(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between 52 preceding and 1 preceding) as fam_sales_qty_mean_52w,
    min(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between unbounded preceding and 1 preceding) as fam_sales_qty_min_roll,
    max(sum(sales_qty_sum)) over (partition by store_code,family_code order by week_key asc rows between unbounded preceding and 1 preceding) as fam_sales_qty_max_roll
from {database}.forecast_sprint4_day_to_week_test
group by week_key, store_code, family_code) as g
on a.family_code = g.family_code
and a.store_code = g.store_code
and a.week_key = g.week_key

-- sub_family rolling mean
left join
(select
    week_key,
    store_code,
    sub_family_code,
    avg(sum(sales_qty_sum)) over (partition by store_code,sub_family_code order by week_key asc rows between 4 preceding and 1 preceding) as subfam_sales_qty_mean_4w,
    avg(sum(sales_qty_sum)) over (partition by store_code,sub_family_code order by week_key asc rows between 12 preceding and 1 preceding) as subfam_sales_qty_mean_12w,
    avg(sum(sales_qty_sum)) over (partition by store_code,sub_family_code order by week_key asc rows between 24 preceding and 1 preceding) as subfam_sales_qty_mean_24w,
    avg(sum(sales_qty_sum)) over (partition by store_code,sub_family_code order by week_key asc rows between 52 preceding and 1 preceding) as subfam_sales_qty_mean_52w
from {database}.forecast_sprint4_day_to_week_test
group by week_key, store_code, sub_family_code) as h
on a.sub_family_code = h.sub_family_code
    and a.store_code = h.store_code
    and a.week_key = h.week_key

-- fam_sales_qty_cum
left join
(select
    week_key,
    family_code,
    trxn_year,
    sum(sum(sales_qty_sum)) over (partition by family_code,trxn_year order by week_key asc rows between unbounded preceding and 1 preceding) as fam_sales_qty_cum
from {database}.forecast_sprint4_day_to_week_test
group by family_code, week_key, trxn_year) as i
on a.family_code = i.family_code
    and a.week_key = i.week_key
    and a.trxn_year = i.trxn_year

-- item_store rolling mean
left join
(select
    week_key,
    store_code,
    item_id,
    sub_id,
    avg(sum(sales_qty_sum)) over(partition by store_code,item_id,sub_id order by week_key rows between 4 preceding and 1 preceding ) as item_store_mean_4points,
    avg(sum(sales_qty_sum)) over(partition by store_code,item_id,sub_id order by week_key rows between 12 preceding and 1 preceding ) as item_store_mean_12points,
    avg(sum(sales_qty_sum)) over(partition by store_code,item_id,sub_id order by week_key rows between 24 preceding and 1 preceding ) as item_store_mean_24points,
    avg(sum(sales_qty_sum)) over(partition by store_code,item_id,sub_id order by week_key rows between 52 preceding and 1 preceding ) as item_store_mean_52points
from {database}.forecast_sprint4_day_to_week_test
group by week_key, store_code, item_id, sub_id) as j
on a.week_key = j.week_key
    and a.store_code = j.store_code
    and a.item_id = j.item_id
    and a.sub_id = j.sub_id

-- low + high month temperature
left join (
  select
      month,
      city_code,
      cast(high_temp as float) as high_temp_month,
      cast(low_temp as float) as low_temp_month
  from {database}.temperature_info_fc1
  group by month, city_code, high_temp_month, low_temp_month
) as k
on substring(cast(a.trxn_month as string) ,5,2) = k.month
and a.city_code = k.city_code


-- competitor item flag
left join (
  select
    city_code as competitor_city_code,
    item_id as competitor_item_id
    from pricing.competitor_dm_list
    -- Here, we assume only the competitors in the same city is "real_competitors", need to be improved.
    where city_code in (select distinct(city_code) from {database}.forecast_sprint4_day_to_week_test)
    and create_date >= to_timestamp(cast({starting_date} as string), 'yyyyMMdd')
    group by city_code, item_id
  ) as l
on a.city_code = l.competitor_city_code
and a.item_id = l.competitor_item_id

-- week_key begin_date + end_date
left join (
  select
    week_key,
    to_date(to_timestamp(min(date_key), 'yyyyMMdd')) as week_begin_date,
    to_date(to_timestamp(max(date_key), 'yyyyMMdd')) as week_end_date
  from ods.dim_calendar
  where date_key >= cast({starting_date} as string)
  and date_key < cast({ending_date} as string)
  group by week_key
) as m
on a.week_key = m.week_key

-- active item flag
-- This one may have to be changed according to the new assorment table
left join (
  select
    store_code,
    item_id,
    sub_id,
    (case when stop_month is null then 1 else 0 end) as active_flag
  from {database}.lastest_active_status   
  group by store_code, item_id, sub_id, stop_month
) as n
on a.store_code = n.store_code
    and a.item_id = n.item_id
    and a.sub_id = n.sub_id

-- add next_dm_w1_sales_qty
left join (
  select
    week_key,
    store_code,
    item_id,
    sub_id,
    sales_qty_sum as next_dm_w1_sales_qty
  from {database}.forecast_sprint4_day_to_week_test
) as p
on concat(cast(year(a.next_dm_start_date) as string),
    lpad(cast(weekofyear(a.next_dm_start_date) as string),2,'0')) = p.week_key
    and a.store_code = p.store_code
    and a.item_id = p.item_id
    and a.sub_id = p.sub_id

-- add next_dm_w2_sales_qty
left join (
  select
    week_key,
    store_code,
    item_id,
    sub_id,
    sales_qty_sum as next_dm_w2_sales_qty

  from {database}.forecast_sprint4_day_to_week_test
  group by week_key, store_code, item_id, sub_id, sales_qty_sum
) as q
on concat(cast(year(date_add(a.next_dm_start_date, 7)) as string),
            lpad(cast(weekofyear(date_add(a.next_dm_start_date, 7)) as string),2,'0')) = q.week_key
and a.store_code = q.store_code
and a.item_id = q.item_id
and a.sub_id = q.sub_id

-- add last year same week: sales_qty, trxn number
left join (
  select
    week_key,
    item_id,
    sub_id,
    store_code,
    sales_qty_sum as last_year_sales_qty
-- ticket_count as last_year_ticket_count

  from {database}.forecast_sprint4_day_to_week_test
  group by week_key, item_id, sub_id, store_code, sales_qty_sum
) as r
on a.week_key = cast(cast(b.week_key as int) + 100 as string)
    and a.item_id = r.item_id
    and a.sub_id = r.sub_id
    and a.store_code = r.store_code

-- left join planned_bp_sold_date as s
-- on s.store_code = a.store_code
-- and s.item_id = a.item_id
-- and s.sub_id = a.sub_id
-- and s.planned_bp_sold_week = a.week_key
;


-- INVALIDATE METADATA {database}.forecast_sprint2_trxn_week_features_flag_sprint4;
