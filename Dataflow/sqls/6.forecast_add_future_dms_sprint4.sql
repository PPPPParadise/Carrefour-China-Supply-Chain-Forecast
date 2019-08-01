/*
Description: Create table "{database}.forecast_add_future_dms_sprint4",
             add all the future dm_theme_ids to a transaction record

Input:  dw.trxns_sales_daily_kudu
        {database}.forecast_store_code_scope_sprint4
        {database}.forecast_itemid_list_threebrands_sprint4
        {database}.forecast_dm_plans_sprint4

Output: {database}.forecast_add_future_dms_sprint4
Created: 2019-07-01
*/

-- drop table if exists {database}.forecast_add_future_dms_sprint4 ;

create table {database}.forecast_add_future_dms_sprint4 as
with
    base as (
        SELECT
            item_id,
            sub_id,
            city_code,
            trxn_date,
            date_key

        from dw.trxns_sales_daily_kudu  
        -- WHERE trxn_date >= to_timestamp('2017-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')
        WHERE trxn_date >= to_timestamp(cast({starting_date} as string), 'yyyyMMdd')
        AND trxn_date < to_timestamp(cast({ending_date} as string), 'yyyyMMdd')
        AND store_code IN (SELECT stostocd FROM {database}.forecast_store_code_scope_sprint4 )
        AND item_id IN (SELECT DISTINCT(item_id) FROM {database}.forecast_itemid_list_threebrands_sprint4)
        AND sub_id IS NOT NULL
        ),

    future_dm_plan as (
        select
            item_id,
            sub_id,
            city_code,
            theme_start_date,
            dm_theme_id
        from {database}.forecast_dm_plans_sprint4
        -- where extract_order = 50
    )

    select
        a.item_id,
        a.sub_id,
        a.city_code,
        a.trxn_date,
        a.date_key,
        b.theme_start_date,
        b.dm_theme_id

    from base a
    left join future_dm_plan b
    on a.item_id = b.item_id
    and a.sub_id = b.sub_id
    and a.city_code = b.city_code
  --  and a.trxn_date < b.theme_start_date
    and to_date(to_timestamp(a.date_key, 'yyyyMMdd')) < to_date(b.theme_start_date)
;

-- INVALIDATE METADATA {database}.forecast_add_future_dms_sprint4;
