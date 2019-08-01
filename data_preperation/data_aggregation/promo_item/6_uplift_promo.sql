/*
Input: 
    {database}.dm_mapping_1719_dates_last_version
    nsa.dm_extract_log
    {database}.forecast_trxn_flag_v1_sprint4
    ods.dim_calendar
Output: {database}.uplift_promo
*/ 

create table {database}.uplift_promo as
with psp_period as (
    select
        distinct
        a.*,
        b.item_id
    from {database}.dm_mapping_1719_dates_last_version as a
    left join nsa.dm_extract_log as b
    on cast(a.current_dm_theme_id as int) = b.dm_theme_id
),

trxn_level_period_added as (
    select
        a.*,
        b.sub_id,
        b.store_code,
        b.sub_family_code,
        b.three_brand_item_list_holding_code,
        b.sales_qty,
        b.reward_amt
    from psp_period as a
    left join {database}.forecast_trxn_flag_v1_sprint4 as b
    on a.item_id = b.item_id
        and a.dm_start_date <= date_key
        and a.dm_end_date >= date_key
),

item_agg as (
    -- aggregate to by item_store_dm with DM
    select
        item_id,
        sub_id,
        store_code,
        sub_family_code,
        three_brand_item_list_holding_code,
        current_dm_theme_id,
        min(dm_start_date) as dm_start_date,
        min(dm_end_date) as dm_end_date,
        sum(sales_qty) as sales_qty,
        sum(reward_amt) as reward_amt
    from trxn_level_period_added
    group by item_id, sub_id, store_code, current_dm_theme_id, sub_family_code, 
            three_brand_item_list_holding_code
    having sum(reward_amt) > 0
),

in_dm_subfam_sales as (
select
    store_code,
    sub_family_code,
    three_brand_item_list_holding_code,
    current_dm_theme_id,
    min(dm_start_date) as dm_start_date,
    min(dm_end_date) as dm_end_date,
    sum(sales_qty) as sales_qty_ly_dm
from item_agg
group by store_code, sub_family_code, three_brand_item_list_holding_code, current_dm_theme_id
),

2w_bef_start_date as (
    select
        a.*,
        concat(substring(b.date_value,1,4), substring(b.date_value,6,2), substring(b.date_value,9,2)) as w2_bef_date_key
    from in_dm_subfam_sales as a
    left join (
        select
            date_key,
            cast(subdate(max(date_value), 14) as string) as date_value
        from ods.dim_calendar
        group by date_key
    ) as b
    on a.dm_start_date = b.date_key
),

item_in_promotion as (
    select
        a.*,
        b.item_id,
        b.sub_id
    from 2w_bef_start_date as a
    left join (
        select
            item_id,
            sub_id,
            store_code,
            sub_family_code,
            three_brand_item_list_holding_code,
            current_dm_theme_id
        from item_agg) as b
        on a.store_code = b.store_code
            and a.sub_family_code = b.sub_family_code
            and a.three_brand_item_list_holding_code = b.three_brand_item_list_holding_code
            and a.current_dm_theme_id = b.current_dm_theme_id
),

in_promo_bef_dupp as (
select
    a.*,
    b.sales_qty
from item_in_promotion as a
left join {database}.forecast_trxn_flag_v1_sprint4 as b
on a.sub_family_code = b.sub_family_code
    and a.three_brand_item_list_holding_code = b.three_brand_item_list_holding_code
    and a.store_code = b.store_code
    and a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.w2_bef_date_key <= b.date_key
    and a.dm_start_date >= b.date_key
),

agg_2w_bef as (
-- aggregate to by item_store_dm
select
    store_code,
    sub_family_code,
    three_brand_item_list_holding_code,
    current_dm_theme_id,
    dm_start_date,
    dm_end_date,
    sales_qty_ly_dm,
    w2_bef_date_key,
    sum(sales_qty) as sales_qty_2w_bef
from in_promo_bef_dupp
where sales_qty > 0
group by store_code, current_dm_theme_id, sub_family_code, three_brand_item_list_holding_code, 
        dm_start_date, dm_end_date, sales_qty_ly_dm, w2_bef_date_key
),

start_stamp as (
select
    a.*,
    b.start_stamp
from agg_2w_bef as a
left join (
    select
        date_key,
        max(date_value) as start_stamp
    from ods.dim_calendar
    group by date_key) as b
    on a.dm_start_date = b.date_key
),

end_stamp as (
select
    a.*,
    b.end_stamp
from start_stamp as a
left join (
    select
        date_key,
        max(date_value) as end_stamp
    from ods.dim_calendar
    group by date_key) as b
    on a.dm_end_date = b.date_key
)

select * from end_stamp;
