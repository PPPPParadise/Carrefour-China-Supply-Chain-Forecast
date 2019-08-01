-- ==============================================
-- ==           AGGREGATION PER DM             ==
-- ==============================================
-- upcoming DM added with sales as NULL
/*
Input: 
    {database}.forecast_sprint4_add_dm_to_daily
    {database}.p4cm_item_map_complete_2021
    {database}.forecast_trxn_flag_v1_sprint4
    nsa.dm_extract_log
    ods.nsa_dm_theme
    ods.p4md_stogld
    bidata.dim_itemsubgl

Output: {database}.forecast_sprint4_dm_agg_v2
*/ 

create table {database}.forecast_sprint4_dm_agg_v2 as
with dm_table as (
    select
        item_id,
        sub_id,
        store_code,
        current_dm_theme_id,
        max(sub_code) as sub_code,
        max(city_code) as city_code,
        sum(daily_sales_sum) as dm_sales_qty,
        max(out_of_stock_flag) as out_of_stock_flag
    from {database}.forecast_sprint4_add_dm_to_daily
    where current_dm_theme_id is not null
    group by store_code, item_id, sub_id, current_dm_theme_id
),

upcoming_dm as (
    -- create a table with DM 2019+   
    select
        item_id,
        sub_code,
        city_code,
        dm_theme_id,
        concat(substring(max(psp_start_date),1,4),substring(max(psp_start_date),6,2),substring(max(psp_start_date),9,2)) as date_key
    from nsa.dm_extract_log
    where extract_order = 50 
    -- 
    and dm_theme_id in (select dm_theme_id from ods.nsa_dm_theme where theme_status <> '-1')
    group by item_id, sub_code, city_code, dm_theme_id
    having max(psp_start_date) > '{ending_date}' 
),

store_code_added as (
    -- add all stores
    select
        a.*,
        b.store_code
    from upcoming_dm as a
    left join(
        select
            stocity as city_code,
            stostocd as store_code
        from ods.p4md_stogld
        group by stocity, stostocd
        ) as b
        on a.city_code = b.city_code
),

sub_id_added as (
    -- add sub_is
    select
        a.*,
        b.sub_id
    from store_code_added as a
    left join (
        select
            item_id,
            sub_code,
            date_key,
            max(sub_id) as sub_id
        from {database}.p4cm_item_map_complete 

        group by 
            item_id, 
            sub_code, 
            date_key
        ) as b
        on a.item_id = b.item_id
            and a.sub_code = b.sub_code
            and a.date_key = b.date_key
),

only_fc_scope as (
    -- select only our scope
    select
        item_id,
        sub_id,
        store_code,
        dm_theme_id as current_dm_theme_id,
        sub_code,
        city_code,
        null as dm_sales_qty,
        0 as out_of_stock_flag
    from sub_id_added
    where item_id in (select distinct item_id from {database}.forecast_trxn_flag_v1_sprint4)
        and store_code in (select distinct store_code from {database}.forecast_trxn_flag_v1_sprint4)
        and date_key > '{ending_date}' 
),

concat_old_and_upcoming as (
-- concat only DM we don't have yet
    select *
    from dm_table
    union all (
        select 
            a.*
        from only_fc_scope a
        left join dm_table b
            on a.item_id = b.item_id
            and a.sub_id = b.sub_id
            and a.store_code = b.store_code
            and a.current_dm_theme_id = b.current_dm_theme_id
        where b.item_id is null)   
),

national_local_added as (
    select
        a.*,
        b.nl,
        b.current_dm_theme_en_desc,
        b.current_theme_start_date,
        b.current_theme_end_date
    from concat_old_and_upcoming as a
    left join (
        select
            dm_theme_id,
            max(nl) as nl,
            max(theme_en_desc) as current_dm_theme_en_desc,
            min(theme_start_date) as current_theme_start_date,
            max(theme_end_date) as current_theme_end_date
        from ods.nsa_dm_theme
        group by dm_theme_id
        ) as b
    on a.current_dm_theme_id = b.dm_theme_id
),

dm_information as (
    select
        a.*,
        b.current_dm_psp,
        b.current_dm_nsp,
        b.current_dm_psp_nsp_ratio,
        b.current_dm_psp_start_date,
        b.current_dm_psp_end_date,
        b.current_dm_page_strategy_code,
        b.current_dm_slot_type_code,
        b.current_dm_slot_type_name,
        b.current_dm_page_no
    from national_local_added as a
    left join (
        select
            item_id,
            sub_code,
            city_code,
            dm_theme_id,
            round(max(psp),2) as current_dm_psp,
            round(max(nsp),2) as current_dm_nsp,
            round(max(psp)/max(nsp),3) as current_dm_psp_nsp_ratio,
            min(psp_start_date) as current_dm_psp_start_date,
            max(psp_end_date) as current_dm_psp_end_date,
            max(page_strategy) as current_dm_page_strategy_code,
            max(slot_type_code) as current_dm_slot_type_code,
            max(slot_type_name) as current_dm_slot_type_name,
            max(page_no) as current_dm_page_no
        from nsa.dm_extract_log
        where extract_order = 50
        group by item_id, sub_code, city_code, dm_theme_id
        ) as b
        on a.item_id = b.item_id
            and a.sub_code = b.sub_code
            and a.city_code = b.city_code
            and a.current_dm_theme_id = b.dm_theme_id
),

item_information as (
    select
        a.*,
        b.item_seasonal_code,
        b.sub_family_code,
        b.family_code
    from dm_information as a
    left join (
        select
            item_id,
            sub_id,
            max(item_seasonal_code) as item_seasonal_code,
            max(sub_family_code) as sub_family_code,
            max(family_code) as family_code
        from bidata.dim_itemsubgl
        group by item_id, sub_id
        ) as b
        on a.item_id = b.item_id
            and a.sub_id = b.sub_id
),

psp_start_week_added as (
    select
        a.*,
        b.week_key as psp_start_week,
        b.month_key as psp_start_month
    from item_information as a
    left join (
        select
            substring(cast(max(date_value) as string),1,10) as date_,
            max(week_key) as week_key,
            max(month_key) as month_key
        from ods.dim_calendar
        group by date_key) as b
        on a.current_dm_psp_start_date = b.date_
),

psp_end_week_added as (
    select
        a.*,
        b.week_key as psp_end_week
    from psp_start_week_added as a
    left join (
        select
            substring(cast(max(date_value) as string),1,10) as date_,
            max(week_key) as week_key
        from ods.dim_calendar
        group by date_key) as b
        on a.current_dm_psp_end_date = b.date_
)

select * from psp_end_week_added;
-- 311716 dm_item_store
-- 344126 dm_item_store with upcoming_dm

