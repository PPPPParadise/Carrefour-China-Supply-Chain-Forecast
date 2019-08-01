/*
Description: Create table "{database}.forecast_trxn_v7_sprint4",
             Adding information to the transaction table:
             including current dm details, next dm details, family_code, sub_family_code, holiday indicator

Input:  dw.trxns_sales_daily_kudu
        {database}.forecast_itemid_list_threebrands_sprint4
        {database}.forecast_store_code_scope_sprint4
        {database}.forecast_dm_plans_sprint4
        {database}.forecast_next_dm_sprint4
        bidata.dim_itemsubgl
        {database}.public_holidays

Output: {database}.forecast_trxn_v7_sprint4
Created: 2019-07-01
*/

-- drop table if exists {database}.forecast_trxn_v7_sprint4;

create table {database}.forecast_trxn_v7_sprint4 as
WITH three_brand_item_list AS (
    SELECT
        *
    FROM {database}.forecast_itemid_list_threebrands_sprint4
),

store_code_list AS (
    SELECT
        stostocd
    FROM {database}.forecast_store_code_scope_sprint4
), 

trxns AS (
    with trxn_original as (
    SELECT
        date_key,
        trxn_date,
        -- year_key,
        -- month_key,
        -- week_key,
        trxn_year,
        trxn_month,
        trxn_week,
        trxn_time,

        ticket_id,
        line_num,
        store_code,
        pos_group,

        sales_type,
        -- city_code,
        -- territory_code,

        item_code,
        sub_code,
        item_id,
        sub_id,
        unit_code,

        cast(unit_code as int) * sales_qty  as sales_qty,
        sales_amt,
        ext_amt,
        reward_point,
        reward_amt,
        coupon_disc,
        promo_disc,
        mpsp_disc,
        channel,
        bp_flag

    FROM dw.trxns_sales_daily_kudu
    -- WHERE trxn_date >= to_timestamp('2017-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')
    WHERE trxn_date >= to_timestamp(cast({starting_date} as string), 'yyyyMMdd')
    AND trxn_date < to_timestamp(cast({ending_date} as string), 'yyyyMMdd')
    AND store_code IN (SELECT stostocd FROM store_code_list)
    AND item_id IN (SELECT DISTINCT(item_id) FROM {database}.forecast_itemid_list_threebrands_sprint4)
    AND sub_id IS NOT NULL
    ),
    year_month_week as (
        select 
            date_key,
            max(month_key) as month_key,
            max(year_key) as year_key,
            max(week_key) as week_key 
        from ods.dim_calendar 
        group by date_key 
    ),
    city_codes as (
        select 
            stostocd as store_code, 
            stocity as city_code, 
            stostate as territory_code 
        from ods.p4md_stogld
        group by 
            stostocd,
            stocity, 
            stostate 
    )
    select 
        a.*,
        b.month_key,
        b.year_key,
        b.week_key,
        c.city_code,
        c.territory_code

    from trxn_original a 
    left join year_month_week b 
    on b.date_key = a.date_key

    left join city_codes c 
    on c.store_code = a.store_code 
),

trxn_add_current_dm as (
    SELECT
        a.*,
        b.dm_theme_id as current_dm_theme_id,
        b.prom_type as current_dm_prom_type,
        b.theme_en_desc as current_dm_theme_en_desc,
        b.theme_start_date as current_theme_start_date,
        b.theme_end_date as current_theme_end_date,
        -- b.city_store_code as current_city_store_code,
        b.city_code as current_city_store_code,

        b.page_no as current_dm_page_no,
        -- b.category_strategy as current_dm_category_strategy,

        b.nsp as current_dm_nsp,
        b.psp as current_dm_psp,
        b.psp_start_date as current_dm_psp_start_date,
        b.psp_end_date as current_dm_psp_end_date,

        b.nl as current_dm_nl,

        -- b.page_strategy_code as current_dm_page_strategy_code,
        b.page_strategy as current_dm_page_strategy_code,
        b.slot_type_code as current_dm_slot_type_code,
        b.dc_flag as current_dm_dc_flag,

        b.msp as current_dm_msp,
        b.msp_get as current_dm_msp_get,
        b.msp_end_date as current_dm_msp_end_date,
        if(b.theme_start_date is NULL, 0, 1) as ind_current_on_dm

    from trxns a
    LEFT JOIN {database}.forecast_dm_plans_sprint4 b
    ON a.item_id = b.item_id
    AND a.sub_code = b.sub_code
    -- AND a.city_code = b.city_store_code
    AND a.city_code = b.city_code

    -- AND a.trxn_time >=  to_date(b.theme_start_date)
    -- AND a.trxn_time <= to_date(b.theme_end_date)

    AND to_date(to_timestamp(a.date_key, 'yyyyMMdd')) >=  to_date(b.theme_start_date)
    AND to_date(to_timestamp(a.date_key, 'yyyyMMdd')) <= to_date(b.theme_end_date)
),

trxn_add_future_dm as (
    SELECT
        trxn_add_current_dm.*,
        future_dm.next_dm_theme_id,
        future_dm.next_dm_start_date as next_dm_start_date,
        datediff(future_dm.next_dm_start_date, trxn_add_current_dm.trxn_date) as time_to_next_dm_days,

        future_dm_detail.theme_end_date as next_dm_end_date,
        future_dm_detail.nl as next_dm_nl,
        future_dm_detail.psp as next_dm_psp,
        future_dm_detail.prom_type as next_dm_prom_type,
        future_dm_detail.nsp as next_dm_nsp,
        future_dm_detail.page_strategy as next_dm_page_strategy,
        future_dm_detail.page_strategy_name as next_dm_page_strategy_name,
        future_dm_detail.slot_type_code as next_dm_slot_type_code,
        future_dm_detail.slot_type_name as next_dm_slot_type_name,
        future_dm_detail.page_no as next_dm_page_no,

        future_dm_detail.msp as next_dm_msp,
        future_dm_detail.extract_datetime as next_dm_v5_extract_datetime

    from trxn_add_current_dm
    left join {database}.forecast_next_dm_sprint4 future_dm

    -- on trxn_add_current_dm.trxn_date = future_dm.trxn_date
    on trxn_add_current_dm.date_key= future_dm.date_key

    and trxn_add_current_dm.item_id = future_dm.item_id
    and trxn_add_current_dm.sub_id = future_dm.sub_id
    and trxn_add_current_dm.city_code = future_dm.city_code

    left join {database}.forecast_dm_plans_sprint4 future_dm_detail
    on future_dm.next_dm_theme_id = future_dm_detail.dm_theme_id
    and future_dm.item_id = future_dm_detail.item_id
    and future_dm.sub_id = future_dm_detail.sub_id
    and future_dm.city_code = future_dm_detail.city_code
),

trxn_add_family_codes as (
    SELECT
        trxn_add_future_dm.*,
        item_info.family_code,
        item_info.sub_family_code,
        item_info.item_seasonal_code

    from trxn_add_future_dm  trxn_add_future_dm
    left join bidata.dim_itemsubgl item_info
    on item_info.item_id = trxn_add_future_dm.item_id
),

holiday_info as (
    select
        to_timestamp(date_key, 'yyyy-MM-dd')  as date_key_ts,
        holiday
    from {database}.public_holidays
),

trxn_add_public_holiday as (
    select
        trxn_add_family_codes.*,
        holiday_info.holiday as ind_holiday

    from trxn_add_family_codes trxn_add_family_codes
    left join holiday_info holiday_info
    -- on holiday_info.date_key_ts = trxn_add_family_codes.trxn_date
    on holiday_info.date_key_ts = to_timestamp(trxn_add_family_codes.date_key, 'yyyyMMdd')
)

select
    *
from trxn_add_public_holiday
;

-- INVALIDATE METADATA {database}.forecast_trxn_v7_sprint4;
