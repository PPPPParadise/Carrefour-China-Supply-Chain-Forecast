/*
Input:  ods.dim_calendar
        {database}.forecast_sprint4_add_dm_to_daily
        {database}.public_holidays
Output: {database}.forecast_spirnt4_day_to_week_tobedelete
*/

-- drop table if exists {database}.forecast_spirnt4_day_to_week;

create table {database}.forecast_sprint4_day_to_week as
-- 8898212 row(s)
with week_date_mapping as (

  with date_week as (
    select
          date_key,
          week_key
    from ods.dim_calendar
    where date_key >= cast({starting_date} as string)
    and date_key < cast({ending_date} as string)
    group by
          date_key,
          week_key
    ),
   week_month_year as (
    select
        week_key,
        max(year_key) as trxn_year,
        max(month_key) as trxn_month
    from ods.dim_calendar
    where date_key >= cast({starting_date} as string)
    and date_key < cast({ending_date} as string)
    group by
        week_key

   )
   select
        a.date_key,
        a.week_key,
        b.trxn_year,
        b.trxn_month
    from date_week a
    left join week_month_year b
    on a.week_key = b.week_key
),

add_week_key as (
  select
      a.*,
      b.week_key,
      b.trxn_year,
      b.trxn_month

  from {database}.forecast_sprint4_add_dm_to_daily a    

  left join week_date_mapping b
  on b.date_key = a.date_key
),

day_to_week as (
  select
      item_id,
      sub_id,
      store_code,
      city_code,
      sub_code,
      week_key,
      trxn_year,
      trxn_month,
      sum(daily_sales_sum) as sales_qty_sum,   
      sum(ext_amt_sum) as ext_amt_sum,

      min(coupon_disc_min) as coupon_disc_min,
      max(coupon_disc_max) as coupon_disc_max,
      avg(coupon_disc_mean) as coupon_disc_mean,
      sum(coupon_disc_sum) as coupon_disc_sum,

      sum(promo_disc_sum) as promo_disc_sum,
      sum(mpsp_disc_sum) as mpsp_disc_sum,
      if(sum(sales_amt_sum) is not null, sum(sales_amt_sum), 0) as sales_amt_sum

  from add_week_key
  group by
      item_id,
      sub_id,
      store_code,
      city_code,
      sub_code,
      week_key,
      trxn_year,
      trxn_month
),

add_holiday_to_week as (
    with holiday_info as (
    select
        to_timestamp(date_key,  'yyyy-MM-dd')  as date_key_ts,
        cast(holiday as int) as holiday
    from {database}.public_holidays
    ),
    holiday_add_week_key as (
        select
            a.*,
            b.week_key
        from holiday_info a
        left join ods.dim_calendar b
        on b.date_value = a.date_key_ts
    ),
    holiday_count as (
    select
        week_key,
        sum(holiday) as holiday_count_new
    from holiday_add_week_key
    group by week_key
    )
    select
       a.*,
       b.holiday_count_new
    from day_to_week a
    left join holiday_count b
    on a.week_key = b.week_key
),

add_ndv_dm as (
  with dm_week_level_num as (
    select
        item_id,
        sub_id,
        store_code,
        week_key,
        count(distinct current_dm_theme_id) as ndv_current_dm_id,
        count(distinct next_dm_theme_id) as ndv_next_dm_theme_id,
        max(current_dm_theme_id) as current_dm_theme_id,
        max(next_dm_theme_id) as next_dm_theme_id

    from add_week_key
    group by item_id, sub_id, store_code, week_key
  )
  select
      a.*,
      b.ndv_current_dm_id,
      b.ndv_next_dm_theme_id,
      b.current_dm_theme_id,
      b.next_dm_theme_id

  from add_holiday_to_week a
  left join dm_week_level_num b
  on b.item_id = a.item_id
  and b.sub_id = a.sub_id
  and b.store_code = a.store_code
  and b.week_key = a.week_key
),

trxn_week_add_current_dm as (
    select
        a.*,
        b.theme_start_date as current_theme_start_date,
        b.theme_end_date as current_theme_end_date,
        b.page_no as current_dm_page_no,
        b.page_strategy as current_dm_page_strategy,
        b.slot_type_code as current_dm_slot_type_code,

        b.nsp as current_dm_nsp,
        b.psp as current_dm_psp,
        b.msp as current_dm_msp,
        b.psp_start_date as current_dm_psp_start_date,
        b.psp_end_date as current_dm_psp_end_date,
        b.psp_end_date as current_dm_end_date,

        b.prom_type as current_prom_type,
        if(a.current_dm_theme_id is null, 0, 1) as ind_current_on_dm_flag

    from add_ndv_dm a
    left join {database}.forecast_dm_plans_sprint4 b
    on b.dm_theme_id = a.current_dm_theme_id
    and b.city_code = a.city_code
    and b.item_id = a.item_id
    and b.sub_code = a.sub_code
),

trxn_week_add_next_dm as (
    select
        a.*,
        b.theme_start_date as next_dm_start_date,
        b.theme_end_date as next_dm_end_date,
        b.nl as next_dm_nl,
        b.psp as next_dm_psp,
        b.prom_type as next_dm_prom_type,
        b.nsp as next_dm_nsp,
        b.page_strategy as next_dm_page_strategy,
        b.page_strategy_name as next_dm_page_strategy_name,
        b.slot_type_code as next_dm_slot_type_code,
        b.slot_type_name as next_dm_slot_type_name,
        b.page_no as next_dm_page_no,
        b.msp as next_dm_msp,
        b.msp_start_date as next_dm_msp_start_date,
        b.msp_end_date as next_dm_msp_end_date,
        b.extract_datetime as next_dm_v5_extract_datetime

    from trxn_week_add_current_dm a
    left join {database}.forecast_dm_plans_sprint4 b
    on b.dm_theme_id = a.next_dm_theme_id
    and b.city_code = a.city_code
    and b.item_id = a.item_id
    and b.sub_code = a.sub_code
),

trxn_week_add_family_codes as (
  select
      a.*,
      b.family_code,
      b.sub_family_code,
      c.item_seasonal_code

  from trxn_week_add_next_dm a
  left join {database}.forecast_item_id_family_codes_sprint4 b
  on b.item_id = a.item_id

  left join bidata.dim_itemsubgl c
  on c.item_id = a.item_id
)


select
  -- three_brand_item_list_holding_code,
  -- three_brand_item_list_id,
  item_id,
  sub_id,
  store_code,
  city_code,
  sub_code,
  week_key,
  trxn_year,
  trxn_month,
  sales_qty_sum,
  ext_amt_sum,
  coupon_disc_min,
  coupon_disc_max,
  coupon_disc_mean,
  coupon_disc_sum,
  promo_disc_sum,
  mpsp_disc_sum,
  sales_amt_sum,
  holiday_count_new,
  ndv_current_dm_id,
  ndv_next_dm_theme_id,
  current_dm_theme_id,
  next_dm_theme_id,
  current_theme_start_date,
  current_theme_end_date,
  current_dm_page_no,
  current_dm_page_strategy,
  current_dm_slot_type_code,
  current_dm_nsp,
  current_dm_psp,
  current_dm_msp,
  current_dm_psp_start_date,
  current_dm_psp_end_date,
  current_dm_end_date,
  current_prom_type,
  ind_current_on_dm_flag,
  next_dm_start_date,
  next_dm_end_date,
  next_dm_nl,
  next_dm_psp,
  next_dm_prom_type,
  next_dm_nsp,
  next_dm_page_strategy,
  next_dm_page_strategy_name,
  next_dm_slot_type_code,
  next_dm_slot_type_name,
  next_dm_page_no,
  next_dm_msp,
  next_dm_msp_start_date,
  next_dm_msp_end_date,
  next_dm_v5_extract_datetime,
  family_code,
  sub_family_code,
  item_seasonal_code

-- from add_three_brands_item_id
from trxn_week_add_family_codes
group by
  -- three_brand_item_list_holding_code,
  -- three_brand_item_list_id,
  item_id,
  sub_id,
  store_code,
  city_code,
  sub_code,
  week_key,
  trxn_year,
  trxn_month,
  sales_qty_sum,
  ext_amt_sum,
  coupon_disc_min,
  coupon_disc_max,
  coupon_disc_mean,
  coupon_disc_sum,
  promo_disc_sum,
  mpsp_disc_sum,
  sales_amt_sum,
  holiday_count_new,
  ndv_current_dm_id,
  ndv_next_dm_theme_id,
  current_dm_theme_id,
  next_dm_theme_id,
  current_theme_start_date,
  current_theme_end_date,
  current_dm_page_no,
  current_dm_page_strategy,
  current_dm_slot_type_code,
  current_dm_nsp,
  current_dm_psp,
  current_dm_msp,
  current_dm_psp_start_date,
  current_dm_psp_end_date,
  current_dm_end_date,
  current_prom_type,
  ind_current_on_dm_flag,
  next_dm_start_date,
  next_dm_end_date,
  next_dm_nl,
  next_dm_psp,
  next_dm_prom_type,
  next_dm_nsp,
  next_dm_page_strategy,
  next_dm_page_strategy_name,
  next_dm_slot_type_code,
  next_dm_slot_type_name,
  next_dm_page_no,
  next_dm_msp,
  next_dm_msp_start_date,
  next_dm_msp_end_date,
  next_dm_v5_extract_datetime,
  family_code,
  sub_family_code,
  item_seasonal_code
;


