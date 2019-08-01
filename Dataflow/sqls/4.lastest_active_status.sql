/*
Description: lastest active status 
Input:  fds.p4cm_daily_stock
        {database}.forecast_store_code_scope_sprint4
        {database}.forecast_itemid_list_threebrands_sprint4
Output: {database}.lastest_active_status
*/

-- Drop table {database}.lastest_active_status;

Create table {database}.lastest_active_status AS
SELECT
  store_code,
  item_id,
  sub_id,
  balance_qty,
  stop_month,
  stop_reason
from fds.p4cm_daily_stock
-- WHERE date_key in (select max(date_key) from fds.p4cm_daily_stock)
WHERE date_key = cast({ending_date} as string)
AND store_code in (select stostocd from {database}.forecast_store_code_scope_sprint4)
AND item_id in (select item_id from {database}.forecast_itemid_list_threebrands_sprint4)
;

-- INVALIDATE METADATA {database}.lastest_active_status;
