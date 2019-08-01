/*
Input: 
        {database}.forecast_sprint4_add_dm_to_daily
        {database}.forecast_item_id_family_codes_sprint4
Output: {database}.forecast_sales_subfamily_store_date 
*/
-- drop table if exists {database}.forecast_sales_subfamily_store_date;
create table {database}.forecast_sales_subfamily_store_date as 
select 
    sub_family_code, 
    store_code, 
    date_key, 
    sum(nvl(sales_amt_sum, 0)) as sales_amt_sum,
    sum(daily_sales_sum) as sales_qty_sum

from {database}.forecast_sprint4_add_dm_to_daily as trxn
left join {database}.forecast_item_id_family_codes_sprint4 as fc
on trxn.item_id = fc.item_id
group by sub_family_code, store_code, date_key
;