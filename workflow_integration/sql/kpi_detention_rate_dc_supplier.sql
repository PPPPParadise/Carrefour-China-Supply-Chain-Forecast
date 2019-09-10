/* =======================================================
                      Module KPI Monitor
                        Detention rate DC by supplier
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
    dc_site, date_key, rotation, holding_code,
    sum(n_not_oos_items) / sum(n_items) as detention_rate
from
    {database}.foreacst_dc_monitor
where 
    run_date = {run_date}
group by
    dc_site, rotation, holding_code, date_key
order by
    date_key, dc_site, rotation, holding_code


