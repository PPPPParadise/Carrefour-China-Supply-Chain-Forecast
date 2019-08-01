/*
Input:
      {database}.forecast_trxn_flag_v1_sprint4   
      {database}.forecast_sprint4_full_date_daily_sales  
      ods.p4md_stogld
      {database}.p4cm_item_map_complete
      {database}.forecast_sprint4_out_of_stock_median   
Output: 
      {database}.forecast_sprint4_trxn_to_day
*/

-- original agg by date
-- drop table if exists {database}.forecast_sprint4_trxn_to_day;

create table {database}.forecast_sprint4_trxn_to_day as
-- 71192153 row(s)
-- -- 61139184 row(s)  new 
  with trxn_to_day as (
  select
      three_brand_item_list_id,
      store_code,
      item_id,
      sub_id,
      date_key,

      (case when sum(isnull(bp_flag, 0)) != 0 then 1 else 0 end) as bp_flag,
      sum(sales_qty) as sales_qty_sum,
      sum(ext_amt) as ext_amt_sum,
      sum(sales_amt) as sales_amt_sum,
      -- round(avg(cal_nsp), 2) as cal_nsp,

      sum(coupon_disc) as coupon_disc_sum,
      min(coupon_disc) as coupon_disc_min,
      max(coupon_disc) as coupon_disc_max,

      sum(promo_disc) as promo_disc_sum,
      sum(mpsp_disc) as mpsp_disc_sum,

      stddev_pop(coupon_disc) as coupon_disc_std,
      avg(coupon_disc) as coupon_disc_mean

  from {database}.forecast_trxn_flag_v1_sprint4   
  group by
      three_brand_item_list_id,
      store_code,
      item_id,
      sub_id,
    -- sub_code,
      date_key
  ),

  add_daily_median as (
  select
      a.* ,
      b.median_daily_sales_qty

  from {database}.forecast_sprint4_full_date_daily_sales a  
  left join {database}.forecast_sprint4_out_of_stock_median b   

  on a.item_id = b.item_id
  and a.sub_id = b.sub_id
  and a.store_code = b.store_code
  and a.date_key = b.date_key
  ),
  
  replace_daily_median as (
  select
      *,
      if(median_daily_sales_qty > sales_qty_daily,
          median_daily_sales_qty, sales_qty_daily) as daily_sales_replaced  
  from add_daily_median
  ),

  median_daily_trxn as (
  select
       a.out_of_stock_flag,
       a.daily_sales_replaced,   
      -- b.*   
       b.three_brand_item_list_id,
       b.store_code,
       b.item_id,
       b.sub_id,
       b.date_key,
       b.bp_flag,
       b.sales_qty_sum,
       b.ext_amt_sum,
       b.sales_amt_sum,
       b.coupon_disc_sum,
       b.coupon_disc_min,
       b.coupon_disc_max,
       b.promo_disc_sum,
       b.mpsp_disc_sum,
       b.coupon_disc_std,
       b.coupon_disc_mean

  from replace_daily_median a
  left join trxn_to_day b   
  on b.item_id = a.item_id
  and b.sub_id = a.sub_id
  and b.store_code = a.store_code
  and b.date_key = a.date_key
  ),

  store_city_code as (
    select
      stostocd as store_code,
      stocity as city_code

    from ods.p4md_stogld
    group by
      stostocd,
      stocity
  ),
  
  -- Using the finshed item_id_code_mapping 
  -- add_item_sub_code as (
  -- select
  --     item_id,
  --     sub_id,
  --     item_code,
  --     dept_code,
  --     sub_code,
  --     date_key,
  --     store_code
  -- from {database}.forecast_sprint4_stock 
  -- group by
  --     item_id,
  --     sub_id,
  --     item_code,
  --     dept_code,
  --     sub_code,
  --     date_key,
  --     store_code
  -- )
  add_item_sub_code as (
  select
      item_id,
      sub_id,
      item_code,
      dept_code,
      sub_code,
      date_key
  from {database}.p4cm_item_map_complete
  group by
      item_id,
      sub_id,
      item_code,
      dept_code,
      sub_code,
      date_key
  )

  select
      a.item_id,
      a.sub_id,
      a.store_code,
      a.date_key,
      a.out_of_stock_flag,
      -- b.out_of_stock_flag,
      -- b.sale_qty_sum,
      -- b.sub_code,
      b.ext_amt_sum,
      b.sales_amt_sum,
      b.coupon_disc_min,
      b.coupon_disc_max,
      b.coupon_disc_sum,
     -- b.coupon_disc_std,
      b.promo_disc_sum,
      b.mpsp_disc_sum,
      b.coupon_disc_std,
      b.coupon_disc_mean,
      b.daily_sales_replaced,
      if(b.daily_sales_replaced is not null, b.daily_sales_replaced, 0) as daily_sales_sum,
      c.city_code,
      d.dept_code,
      d.item_code,
      d.sub_code

  from {database}.forecast_sprint4_full_date_daily_sales  a
  left join median_daily_trxn b
  on b.item_id = a.item_id
  and b.sub_id = a.sub_id
  and b.store_code = a.store_code
  and b.date_key = a.date_key

  left join store_city_code c
  on c.store_code = a.store_code

  left join add_item_sub_code d
  on d.item_id = a.item_id
  and d.sub_id = a.sub_id
  -- and d.store_code = a.store_code
  and d.date_key = a.date_key
  ;

-- INVALIDATE METADATA {database}.forecast_sprint4_trxn_to_day;
