/* ===================================================
                       Module 3
                     DM dataflow
     Step 6: compute baseline 4 weeks before DM
======================================================
*/

-- From: {database}.forecast_sprint4_promo_uplift
--       ods.dim_calendar
--       {database}.forecast_trxn_flag_v1_sprint4

-- To: {database}.forecast_sprint4_promo_with_coupon
-- Usage: compute the baseline generated 4 weeks before the DM
-- Note: _

create table {database}.forecast_sprint4_promo_with_baseline as
with baseline_period as (
select
    a.*,
    concat(substring(b.stamp_4wbef,1,4), substring(b.stamp_4wbef,6,2), substring(b.stamp_4wbef,9,2)) as date_key_4w_bef,
    concat(substring(b.stamp_8wbef,1,4), substring(b.stamp_8wbef,6,2), substring(b.stamp_8wbef,9,2)) as date_key_8w_bef
from {database}.forecast_sprint4_promo_uplift as a
left join
(select
    substring(cast(max(date_value) as string),1,10) as stamp_key,
    cast(subdate(max(date_value), 28) as string) as stamp_4wbef,
    cast(subdate(max(date_value), 56) as string) as stamp_8wbef
from ods.dim_calendar
group by date_key) as b
on a.current_dm_psp_start_date = b.stamp_key),

daily_sales as
(select
    item_id,
    sub_id,
    store_code,
    date_key,
    sum(sales_qty) as sales_qty_sum
from {database}.forecast_trxn_flag_v1_sprint4
where sales_qty > 0
group by item_id, sub_id, store_code, date_key),

date_key_dupp as
(select
    a.*,
    b.sales_qty_sum
from baseline_period as a
left join
(select
    item_id,
    sub_id,
    store_code,
    date_key,
    sales_qty_sum
from daily_sales) as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.date_key_4w_bef >= b.date_key
    and a.date_key_8w_bef <= b.date_key)

select
    item_id,
    sub_id,
    store_code,
    current_dm_theme_id,
    sub_code,
    city_code,
    dm_sales_qty,
    out_of_stock_flag,
    nl,
    current_dm_theme_en_desc,
    current_theme_start_date,
    current_theme_end_date,
    current_dm_psp,
    current_dm_nsp,
    current_dm_psp_nsp_ratio,
    current_dm_psp_start_date,
    current_dm_psp_end_date,
    current_dm_page_strategy_code,
    current_dm_slot_type_code,
    current_dm_slot_type_name,
    current_dm_page_no,
    item_seasonal_code,
    sub_family_code,
    family_code,
    psp_start_week,
    psp_start_month,
    psp_end_week,
    last_year_dm_sales,
    last_year_dm_psp_nsp_ratio,
    last_year_fam_dm_sales_avg,
    last_5dm_sales_avg,
    fam_last_5dm_sales_avg,
    festival_type,
    lunar_last_year_week_key,
    lunar_2w_bef,
    lunar_2w_aft,
    lunar_6w_bef,
    lunar_6w_aft,
    last_year_lunar_sales_qty_1m_avg,
    last_year_lunar_sales_qty_1m_sum,
    last_year_lunar_ratio_1m,
    last_year_lunar_sales_qty_3m_sum,
    last_year_lunar_sales_qty_3m_avg,
    last_year_lunar_ratio_3m,
    last_year_dm_sales_vrai_exact,
    vrai_exact_or_lunar_1m,
    vrai_exact_or_lunar_3m,
    three_brand_item_list_holding_code,
    uplift,
    ifnull(sum(sales_qty_sum), 0) as 4w_sales_4w_bef
from date_key_dupp
group by item_id, sub_id, store_code, current_dm_theme_id, sub_code
    , city_code, dm_sales_qty, out_of_stock_flag, nl, current_dm_theme_en_desc
    , current_theme_start_date, current_theme_end_date
    , current_dm_psp, current_dm_nsp, current_dm_psp_nsp_ratio
    , current_dm_psp_start_date, current_dm_psp_end_date, current_dm_page_strategy_code
    , current_dm_slot_type_code, current_dm_slot_type_name, current_dm_page_no
    , item_seasonal_code, sub_family_code, family_code, psp_start_week, psp_start_month
    , psp_end_week, last_year_dm_sales, last_year_dm_psp_nsp_ratio
    , last_year_fam_dm_sales_avg, last_5dm_sales_avg, fam_last_5dm_sales_avg, festival_type
    , lunar_last_year_week_key, lunar_2w_bef, lunar_2w_aft, lunar_6w_bef, lunar_6w_aft
    , last_year_lunar_sales_qty_1m_avg, last_year_lunar_sales_qty_1m_sum
    , last_year_lunar_ratio_1m, last_year_lunar_sales_qty_3m_sum, last_year_lunar_sales_qty_3m_avg
    , last_year_lunar_ratio_3m, last_year_dm_sales_vrai_exact, vrai_exact_or_lunar_1m
    , vrai_exact_or_lunar_3m, three_brand_item_list_holding_code, uplift;

