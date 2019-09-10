/* =======================================================
                      Module KPI Monitor
                        Stock level - Store
==========================================================
*/

/*
Input:
    * lfms.daily_dcstock
    * vartefact.forecast_dm_dc_orders
    * vartefact.v_forecast_inscope_dc_item_details
*/

select
    store_code, con_holding, rotation, in_dm, date_key, sum(total_stock) as stock_level
from
    vartefact.foreacst_store_monitor
where run_date = "{run_date}"
group by
    store_code, con_holding, rotation, in_dm, date_key