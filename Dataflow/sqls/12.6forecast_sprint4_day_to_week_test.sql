/*
Input:  {database}.forecast_itemid_list_threebrands_sprint4
        {database}.forecast_spirnt4_day_to_week_tobedelete
Output: {database}.forecast_spirnt4_day_to_week_test 
*/

-- drop table if exists {database}.forecast_spirnt4_day_to_week_test ;
create table {database}.forecast_sprint4_day_to_week_test as
-- 8899631 row(s) 
with tb1 as (
select
    a.holding_code as three_brand_item_list_holding_code,
    a.item_id as three_brand_item_list_id,
    b.*
    
from {database}.forecast_itemid_list_threebrands_sprint4 a
left join {database}.forecast_sprint4_day_to_week b
on b.item_id = a.item_id
)
select
    *
from tb1
;

-- invalidate metadata {database}.forecast_spirnt4_day_to_week_test;

