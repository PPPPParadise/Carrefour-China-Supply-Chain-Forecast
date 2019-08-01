
/* ======================================================
                         Module 3
                       DM dataflow
     Step 8: Compute average discount per slot type
=========================================================
*/
-- Input: 
--  {database}.forecast_trxn_v7_full_item_id_sprint4_promo 
--  {database}.forecast_sprint4_promo_with_coupon 
-- Output: {database}.forecast_sprint4_promo_mecha_v4 
   
-- Usage: compute the average discount per slot type
-- Note: _

create table {database}.forecast_sprint4_promo_mecha_v4 as
with forecast_trxn_v7_full_item_id_sprint4_promo as (
    with dm_slot_type_name_info as (
        select 
            dm_theme_id,
            item_id,
            sub_code,
            city_code,
            max(slot_type_name) as slot_type_name
            
        from nsa.dm_extract_log
        where extract_order = 50 
        and dm_theme_id in (select distinct current_dm_theme_id
                            from {database}.forecast_trxn_v7_full_item_id_sprint4)
        and item_id in (select distinct item_id 
                            from {database}.forecast_trxn_v7_full_item_id_sprint4)
        group by 
            dm_theme_id,
            item_id,
            sub_code,
            city_code
    )
    select 
        a.*,
        b.slot_type_name as current_dm_slot_type_name
    from {database}.forecast_trxn_v7_full_item_id_sprint4 a 
    left join dm_slot_type_name_info b 
    on b.item_id = a.item_id 
    and b.sub_code = a.sub_code
    and b.city_code = a.city_code
    and b.dm_theme_id = a.current_dm_theme_id
),

coup_disc_ratio_per_slot as
(select
    current_dm_slot_type_code,
    current_dm_slot_type_name,
    avg(coupon_disc / nullif(ext_amt,0)) as coup_disc_ratio
from forecast_trxn_v7_full_item_id_sprint4_promo
group by current_dm_slot_type_code, current_dm_slot_type_name),

slot_type_map_daily as
(select
    substring(cast(trxn_date as string),1,10) as date_k,
    item_id,
    sub_id,
    store_code,
    current_dm_slot_type_name,
    current_dm_slot_type_code
from forecast_trxn_v7_full_item_id_sprint4_promo
group by substring(cast(trxn_date as string),1,10), item_id, sub_id, store_code
    , current_dm_slot_type_code, current_dm_slot_type_name),

can_be_joined as
(select
    a.*,
    b.current_dm_slot_type_name as slot_type_name_all,
    b.current_dm_slot_type_code as slot_type_code_all
from {database}.forecast_sprint4_promo_with_coupon as a
left join
slot_type_map_daily as b
on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code
    and a.current_dm_psp_start_date <= b.date_k
    and a.current_dm_psp_end_date >= b.date_k),

mech_ratio_added as
(select
    a.*,
    b.coup_disc_ratio as coup_disc_ratio_mech
from can_be_joined as a
left join 
coup_disc_ratio_per_slot as b
on a.slot_type_name_all = b.current_dm_slot_type_name
    and a.slot_type_code_all = b.current_dm_slot_type_code)

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
    4w_sales_4w_bef,
    coupon_disc_ratio_avg_max,
    max(coup_disc_ratio_mech) as coup_disc_ratio_mech_max
from mech_ratio_added
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
    , vrai_exact_or_lunar_3m, three_brand_item_list_holding_code, uplift, 4w_sales_4w_bef, coupon_disc_ratio_avg_max;
