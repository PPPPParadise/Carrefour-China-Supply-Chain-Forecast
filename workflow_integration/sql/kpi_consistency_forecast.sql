/* =======================================================
                      Module KPI Monitor
                        Consistency
==========================================================
*/
select
    week_start_day,
    run_date,
    dept_code,
    item_code,
    sub_code,
    cast(order_qty + dm_order_qty as int) as order_qty
from
    vartefact.forecast_weekly_forecast_file 
where
    run_date >= '{CONSISTENCY_START}'
    and run_date <= '{CONSISTENCY_END}'
    and week_start_day='{END_MONDAY}'
