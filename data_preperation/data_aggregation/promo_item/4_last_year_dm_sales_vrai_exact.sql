-- =========================================
-- ==             VRAI EXACT              ==
-- =========================================
-- more item probably
/*
Input: 
    {database}.dm_mapping_1719_dates_last_version 
    {database}.forecast_dm_plans_sprint4
    {database}.forecast_trxn_flag_v1_sprint4    
Output: {database}.last_year_dm_sales_vrai_exact
*/ 

create table {database}.last_year_dm_sales_vrai_exact as
with psp_period as (
    select
        distinct
        a.*,
        b.item_id, 
        b.sub_code 
    from {database}.dm_mapping_1719_dates_last_version as a
    left join (
            select *
            from {database}.forecast_dm_plans_sprint4
            )as b
    on cast(a.current_dm_theme_id as int) = b.dm_theme_id
),

trxn_level_period_added as (
    select
        a.*,
        b.sub_id,
        b.store_code,
        b.sales_qty,
        b.reward_amt
    from psp_period as a
    left join {database}.forecast_trxn_flag_v1_sprint4 as b
    on a.item_id = b.item_id
        and a.sub_code = b.sub_code
        and a.dm_start_date <= date_key
        and a.dm_end_date >= date_key
)

select
    item_id,
    sub_id,
    store_code,
    current_dm_theme_id,
    sum(sales_qty) as sales_qty,
    sum(reward_amt) as reward_amt 
from trxn_level_period_added
group by item_id, sub_id, store_code, current_dm_theme_id
having sum(reward_amt) > 0;