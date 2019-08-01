/*
Description: Create table "{database}.forecast_sprint3_coupon_item_link"
             Add the coupon information to {database}.forecast_sprint3_v3_flag on weekly level

Input:  {database}.forecast_sprint3_v3_flag_sprint4
        ods.dim_calendar
        {database}.coupon_mapping

Output: {database}.forecast_sprint3_coupon_item_link_flag
*/

-- drop table {database}.forecast_sprint3_coupon_item_link_flag_sprint4;

create table {database}.forecast_sprint3_coupon_item_link_flag_sprint4 as
-- 10807335 row(s)
with coupon_mapping_add_week as (
    with calendar_info as (
    select
        date_value,
        week_key
    from ods.dim_calendar
    -- where date_value >= to_timestamp('2017-01-01', 'yyyy-MM-dd')
    -- and date_value <= to_timestamp('2019-12-31', 'yyyy-MM-dd')
    where date_value >= to_timestamp(cast({starting_date} as string), 'yyyyMMdd')
    and date_value < to_timestamp(cast({ending_date} as string), 'yyyyMMdd')
    group by date_value, week_key
    ),

    coupon_info as (
    SELECT
        coupon_activity_id,
        coupon_activity_name_chn,
        coupon_activity_d_id,
        coupon_used_date_start,
        coupon_used_date_end,
        storecitylevel,
        coupon_city_store_dtl,
        coupon_activity_type_id,
        coupon_typecode,
        coupon_count1,
        promotion_level,
        item_id,
        sub_id,
        item_code,
        sub_code

    from {database}.coupon_mapping
    ),

    tb1 as (
    select
        a.*,
        b.*
    from calendar_info a
    left join coupon_info b
    on a.date_value >= b.coupon_used_date_start
    and a.date_value <= b.coupon_used_date_end
    )
    select
        coupon_activity_id,
        coupon_activity_name_chn,
        coupon_activity_d_id,
        coupon_used_date_start,
        coupon_used_date_end,
        storecitylevel,
        coupon_city_store_dtl,
        coupon_activity_type_id,
        coupon_typecode,
        coupon_count1,
        promotion_level,
        item_id,
        sub_id,
        item_code,
        sub_code,
        date_value,
        week_key

    from tb1
    group by
        coupon_activity_id,
        coupon_activity_name_chn,
        coupon_activity_d_id,
        coupon_used_date_start,
        coupon_used_date_end,
        storecitylevel,
        coupon_city_store_dtl,
        coupon_activity_type_id,
        coupon_typecode,
        coupon_count1,
        promotion_level,
        item_id,
        sub_id,
        item_code,
        sub_code,
        date_value,
        week_key
),

city_level_coupon as (
select
    coupon_activity_id,
    coupon_activity_d_id,
    storecitylevel,
    coupon_city_store_dtl,

    item_id,
    sub_id,
    item_code,
    sub_code,
    week_key,

    coupon_activity_type_id,
    coupon_typecode,
    coupon_count1,
    promotion_level

from coupon_mapping_add_week
where storecitylevel = 'C'
group by
    coupon_activity_id,
    coupon_activity_d_id,
    storecitylevel,
    coupon_city_store_dtl,
    item_id,
    sub_id,
    item_code,
    sub_code,
    week_key,

    coupon_activity_type_id,
    coupon_typecode,
    coupon_count1,
    promotion_level
),

store_level_coupon as (
select
    coupon_activity_id,
    coupon_activity_d_id,
    storecitylevel,
    coupon_city_store_dtl,
    item_id,
    sub_id,
    item_code,
    sub_code,
    week_key,

    coupon_activity_type_id,
    coupon_typecode,
    coupon_count1,
    promotion_level

from coupon_mapping_add_week
where storecitylevel = 'S'
group by
    coupon_activity_id,
    coupon_activity_d_id,
    storecitylevel,
    coupon_city_store_dtl,
    item_id,
    sub_id,
    item_code,
    sub_code,
    week_key,

    coupon_activity_type_id,
    coupon_typecode,
    coupon_count1,
    promotion_level
),

before_agg as (
SELECT
    a.item_id,
    a.sub_id,
    a.city_code,
    a.store_code,
    a.week_key,
    a.week_begin_date,
    a.week_end_date,

    b.item_id as coupon_item_id_city,
    b.sub_id as coupon_sub_key_city,
    b.coupon_activity_id as coupon_activity_id_city,
    b.coupon_activity_d_id as coupon_activity_d_id_city,
    b.week_key as coupon_week_key_city,
    b.coupon_city_store_dtl as coupon_city_store_dtl_city,
    b.coupon_activity_type_id as coupon_activity_type_id_city,
    b.coupon_typecode as coupon_typecode_city,
    b.coupon_count1 as coupon_count1_city,
    b.promotion_level as promotion_level_city,

    c.item_id as coupon_item_id_store,
    c.sub_id as coupon_sub_key_store,
    c.coupon_activity_id as coupon_activity_id_store,
    c.coupon_activity_d_id as coupon_activity_d_id_store,
    c.week_key as coupon_week_key_store,
    c.coupon_city_store_dtl as coupon_city_store_dtl_store,
    c.coupon_activity_type_id as coupon_activity_type_id_store,
    c.coupon_typecode as coupon_typecode_store,
    c.coupon_count1 as coupon_count1_store,
    c.promotion_level as promotion_level_store

from {database}.forecast_sprint3_v3_flag_sprint4 a

left join city_level_coupon b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and instr(b.coupon_city_store_dtl, a.city_code) >= 1
and b.week_key = a.week_key

left join store_level_coupon c
on c.item_id = a.item_id
and c.sub_id = a.sub_id
and instr(c.coupon_city_store_dtl, a.store_code) >=1
and c.week_key = a.week_key
)

select
    item_id,
    sub_id,
    city_code,
    store_code,
    week_key,
    week_begin_date,
    week_end_date,

    coupon_item_id_city,
    coupon_sub_key_city,
    coupon_activity_id_city,
    coupon_activity_d_id_city,
    coupon_week_key_city,
    coupon_city_store_dtl_city,
    coupon_activity_type_id_city,
    coupon_typecode_city,
    coupon_count1_city,
    promotion_level_city,

    coupon_item_id_store,
    coupon_sub_key_store,
    coupon_activity_id_store,
    coupon_activity_d_id_store,
    coupon_week_key_store,
    coupon_city_store_dtl_store,
    coupon_activity_type_id_store,
    coupon_typecode_store,
    coupon_count1_store,
    promotion_level_store

from before_agg
group by
    item_id,
    sub_id,
    city_code,
    store_code,
    week_key,
    week_begin_date,
    week_end_date,

    coupon_item_id_city,
    coupon_sub_key_city,
    coupon_activity_id_city,
    coupon_activity_d_id_city,
    coupon_week_key_city,
    coupon_city_store_dtl_city,
    coupon_activity_type_id_city,
    coupon_typecode_city,
    coupon_count1_city,
    promotion_level_city,

    coupon_item_id_store,
    coupon_sub_key_store,
    coupon_activity_id_store,
    coupon_activity_d_id_store,
    coupon_week_key_store,
    coupon_city_store_dtl_store,
    coupon_activity_type_id_store,
    coupon_typecode_store,
    coupon_count1_store,
    promotion_level_store
;

-- invalidate metadata {database}.forecast_sprint3_coupon_item_link_flag;
