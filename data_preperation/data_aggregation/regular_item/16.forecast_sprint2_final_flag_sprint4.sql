/*
Description: Create table "{database}.forecast_sprint2_final_flag": the dataset to feed sprint2 model
            Add item descriptions and barcode
            Add out_of_stock flag from poisson distribution
            Add festival ticket count

Input:  {database}.forecast_sprint2_trxn_week_features_flag_sprint4
        ods.p4md_itmbar
        {database}.forecast_item_id_family_codes_sprint4
        {database}.poisson_filtering
        {database}.chinese_festivals
        ods.dim_calendar 
        {database}.forecast_sprint2_festival_ticket_count_flag

Output: {database}.forecast_sprint2_final_flag
*/

-- drop table if exists {database}.forecast_sprint2_final_flag_sprint4;

create table {database}.forecast_sprint2_final_flag_sprint4 as
-- 8899631 row(s)
with table2 as (
  with table1 as (
    SELECT itbitmid, itbsubid, itbcre, itbbar,
            row_number() over (partition by itbitmid, itbsubid order by itbcre) as row_
    FROM ods.p4md_itmbar
    WHERE itbbarmj = 'Y'
    and enddate is NULL
   )
select
    *
from table1
where row_ = 1
),

add_desc_bar as (
SELECT
    a.*,
    b.family_edesc,
    b.family_ldesc,
    b.sub_family_edesc,
    b.sub_family_ldesc,
    b.itmstkun,
    b.itmpack,
    b.itmcapa,
    b.itmcaput,
    b.group_family_edesc,
    b.itmedesc,
    b.itmldesc,
    c.itbbar as barcode

from {database}.forecast_sprint2_trxn_week_features_flag_sprint4 a
left join {database}.forecast_item_id_family_codes_sprint4 b
on b.item_id = a.item_id
left join table2 c
on a.item_id = c.itbitmid
and a.sub_id = c.itbsubid
),

out_of_stock as (
    select
        cast(item_id as int) as item_id,
        cast(sub_id as int) as sub_id,
        store_code,
        date_key,
        cast(out_stock_flag as int) as out_stock_flag
    from {database}.poisson_filtering
),

date_week_link as (
        select
            week_key,
            date_key
        from ods.dim_calendar 
        group by date_key, week_key
),

add_week_key as (
    select
        a.*,
        b.week_key

    from out_of_stock a
    left join date_week_link b
    on b.date_key = a.date_key
),

week_key_out_stock as (
    select
    week_key,
    max(out_stock_flag) as out_stock_flag,
    store_code,
    sub_id,
    item_id

from add_week_key
group by item_id, sub_id, week_key, store_code
),

add_out_stock_flag as (
select
    a.*,
    b.out_stock_flag

from  add_desc_bar a
left join week_key_out_stock b
on b.item_id = a.item_id
and b.sub_id = a.sub_id
and b.week_key = a.week_key
and b.store_code = a.store_code
),

add_festival_type_to_all_feature as (
    select
    a.*,
    if(c.festival_type is not null, cast(c.festival_type as int), null) as festival_type

    from  add_out_stock_flag as a
    left join
        (   select
                week_key,
                max(festival_type) as festival_type
            from (  select
                        cal.week_key,
                        festival_type
                    from (
                        select
                            date_key,
                            festival_type
                        from {database}.chinese_festivals
                        ) as fest
                    left join  ods.dim_calendar as cal
                    on fest.date_key = cal.date_key
            ) as a
            group by week_key
        ) as c
    on a.week_key = c.week_key
),


add_ticket_count as (
    select
        a.*,
        b.ticket_count as last_year_festival_ticket_count

    from add_festival_type_to_all_feature a
    left join ( select
                    item_id,
                    sub_id,
                    store_code,
                    year_key,
                    ticket_count,
                    if(festival_type is not null, cast(festival_type as int), null) as festival_type
                from {database}.forecast_sprint2_festival_ticket_count_flag
                ) as b
    on a.festival_type = b.festival_type
    and substring(a.week_key,1,4) = cast(cast(b.year_key as int) + 1 as string)
    and a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.store_code = b.store_code

)
select
    active_flag,
    barcode,
    -- bp_flag,
    -- cal_nsp,
    city_code,
    coupon_disc_max,
    coupon_disc_mean,
    coupon_disc_min,
    -- coupon_disc_std,
    coupon_disc_sum,
    curr_psp_days,
    curr_psp_end_dow,
    curr_psp_end_week_count,
    curr_psp_start_dow,
    curr_psp_start_week_count,
    current_dm_end_date,
    current_dm_msp,
    current_dm_nsp,
    current_dm_page_no,
    current_dm_page_strategy,
    current_dm_psp,
    current_dm_psp_end_date,
    current_dm_psp_start_date,
    current_dm_slot_type_code,
    current_dm_theme_id,
    current_prom_type,
    current_theme_end_date,
    current_theme_start_date,
    disc_ratio,
    ext_amt_sum,
    fam_sales_qty_max_roll,
    fam_sales_qty_mean_12w,
    fam_sales_qty_mean_24w,
    fam_sales_qty_mean_4w,
    fam_sales_qty_mean_52w,
    fam_sales_qty_min_roll,
    family_code,
    family_edesc,
    family_ldesc,
    festival_type,
    group_family_code,
    group_family_edesc,
    high_temp_month,
    holiday_count_new,
    ind_competitor,
    ind_current_on_dm_flag,
    item_id,
    item_seasonal_code,
    item_store_mean_12points,
    item_store_mean_24points,
    item_store_mean_4points,
    item_store_mean_52points,
    itmcapa,
    itmcaput,
    itmedesc,
    itmldesc,
    itmpack,
    itmstkun,
    last_year_festival_ticket_count,
    last_year_sales_qty,
    low_temp_month,
    mpsp_disc_sum,
    next_dm_days,
    next_dm_end_date,
    next_dm_msp,
    next_dm_msp_end_date,
    next_dm_msp_start_date,
    next_dm_nl,
    next_dm_nsp,
    next_dm_page_no,
    next_dm_page_strategy,
    next_dm_page_strategy_name,
    next_dm_prom_type,
    next_dm_psp,
    next_dm_slot_type_code,
    next_dm_slot_type_name,
    next_dm_start_date,
    next_dm_start_week_count,
    next_dm_theme_id,
    -- next_dm_v5_extract_datetime,
    cast(next_dm_v5_extract_datetime as string) as next_dm_v5_extract_datetime,
    next_dm_w1_sales_qty,
    next_dm_w2_sales_qty,
    out_stock_flag,
    -- planned_bp_flag,
    promo_disc_sum,
    psp_nsp_ratio,
    sales_amt_sum,
    sales_qty_fam_store_wgt,
    sales_qty_fam_wgt,
    sales_qty_sum,
    store_code,
    sub_code,
    sub_family_code,
    sub_family_edesc,
    sub_family_ldesc,
    sub_id,
    subfam_sales_qty_mean_12w,
    subfam_sales_qty_mean_24w,
    subfam_sales_qty_mean_4w,
    subfam_sales_qty_mean_52w,
    three_brand_item_list_holding_code,
    three_brand_item_list_id,
    trxn_month,
  --   trxn_week,
    trxn_year,
  --  unit_code,
    week_begin_date,
    week_end_date,
    week_key
from add_ticket_count
group by
    active_flag,
    barcode,
  --  bp_flag,
  --  cal_nsp,
    city_code,
    coupon_disc_max,
    coupon_disc_mean,
    coupon_disc_min,
  --  coupon_disc_std,
    coupon_disc_sum,
    curr_psp_days,
    curr_psp_end_dow,
    curr_psp_end_week_count,
    curr_psp_start_dow,
    curr_psp_start_week_count,
    current_dm_end_date,
    current_dm_msp,
    current_dm_nsp,
    current_dm_page_no,
    current_dm_page_strategy,
    current_dm_psp,
    current_dm_psp_end_date,
    current_dm_psp_start_date,
    current_dm_slot_type_code,
    current_dm_theme_id,
    current_prom_type,
    current_theme_end_date,
    current_theme_start_date,
    disc_ratio,
    ext_amt_sum,
    fam_sales_qty_max_roll,
    fam_sales_qty_mean_12w,
    fam_sales_qty_mean_24w,
    fam_sales_qty_mean_4w,
    fam_sales_qty_mean_52w,
    fam_sales_qty_min_roll,
    family_code,
    family_edesc,
    family_ldesc,
    festival_type,
    group_family_code,
    group_family_edesc,
    high_temp_month,
    holiday_count_new,
    ind_competitor,
    ind_current_on_dm_flag,
    item_id,
    item_seasonal_code,
    item_store_mean_12points,
    item_store_mean_24points,
    item_store_mean_4points,
    item_store_mean_52points,
    itmcapa,
    itmcaput,
    itmedesc,
    itmldesc,
    itmpack,
    itmstkun,
    last_year_festival_ticket_count,
    last_year_sales_qty,
    low_temp_month,
    mpsp_disc_sum,
    next_dm_days,
    next_dm_end_date,
    next_dm_msp,
    next_dm_msp_end_date,
    next_dm_msp_start_date,
    next_dm_nl,
    next_dm_nsp,
    next_dm_page_no,
    next_dm_page_strategy,
    next_dm_page_strategy_name,
    next_dm_prom_type,
    next_dm_psp,
    next_dm_slot_type_code,
    next_dm_slot_type_name,
    next_dm_start_date,
    next_dm_start_week_count,
    next_dm_theme_id,
    next_dm_v5_extract_datetime,
    next_dm_w1_sales_qty,
    next_dm_w2_sales_qty,
    out_stock_flag,
    -- planned_bp_flag,
    promo_disc_sum,
    psp_nsp_ratio,
    sales_amt_sum,
    sales_qty_fam_store_wgt,
    sales_qty_fam_wgt,
    sales_qty_sum,
    store_code,
    sub_code,
    sub_family_code,
    sub_family_edesc,
    sub_family_ldesc,
    sub_id,
    subfam_sales_qty_mean_12w,
    subfam_sales_qty_mean_24w,
    subfam_sales_qty_mean_4w,
    subfam_sales_qty_mean_52w,
    three_brand_item_list_holding_code,
    three_brand_item_list_id,
    trxn_month,
  --  trxn_week,
    trxn_year,
  --  unit_code,
    week_begin_date,
    week_end_date,
    week_key
;

-- INVALIDATE METADATA  {database}.forecast_sprint2_final_flag_sprint4;
