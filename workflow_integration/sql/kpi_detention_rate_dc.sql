/* =======================================================
                      Module KPI Monitor
                        Detention rate DC
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * vartefact.foreacst_dc_monitor
*/

select
    dc_site, date_key, rotation,
    sum(n_not_oos_items) / sum(n_items) as detention_rate
from
    {database}.foreacst_dc_monitor
where 
    run_date = {run_date}
group by
    dc_site, rotation, date_key
order by
    date_key, dc_site, rotation
