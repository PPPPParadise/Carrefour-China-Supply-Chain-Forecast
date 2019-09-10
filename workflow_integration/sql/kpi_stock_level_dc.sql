/* =======================================================
                      Module KPI Monitor
                        Stock level - DC
==========================================================
*/

/*
Input:
    * vartefact.foreacst_dc_monitor
*/

select
    dc_site, in_dm, holding_code, rotation, date_key, sum(total_stock) as stock_level
from
    {database}.foreacst_dc_monitor
where 
    run_date = {run_date}
group by
    dc_site, in_dm, holding_code, rotation, date_key
