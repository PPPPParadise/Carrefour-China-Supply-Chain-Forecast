/* =======================================================
                      Module KPI Monitor
                     Detention rate store
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * vartefact.foreacst_store_monitor   -- to get item flow type
*/

select
    con_holding, rotation, date_key,
    sum(n_not_oos_items) / sum(n_items) as detention_rate
from
    {database}.foreacst_store_monitor
where 
    run_date = {run_date}
group by
    con_holding, rotation, date_key
order by
    date_key, con_holding, rotation