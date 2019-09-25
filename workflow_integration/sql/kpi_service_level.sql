/* =======================================================
                      Module KPI Monitor
                        Service level
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * lfms.daily_dctrxn                                    -- received by dc
    * lfms.ord                                             -- ordered by dc
    * vartefact.v_forecast_inscope_dc_item_details         -- items in scope
To:
    * vartefact.monitor_service_level_item_dc
*/

with combine_ordered_received as
(
    select
        rec.item_code,
        rec.sub_code,
        rec.dept_code,
        c.holding_code,
        rec.date_key,
        sum(ord.basic_order_qty) as basic_order_qty,
        sum(coalesce(rec.trxn_qty, 0)) as trxn_qty
    from
        lfms.ord ord -- ordered
        join
            vartefact.v_forecast_inscope_dc_item_details c
        ON 
            ord.department_code = c.dept_code
            and ord.item_code = c.item_code
            and ord.sub_code = c.sub_code
        left join lfms.daily_dctrxn rec  -- received  
        on
            rec.item_code = ord.item_code
            and rec.sub_code = ord.sub_code
            and rec.dept_code = ord.department_code
            and rec.supplier_code = ord.supplier_code
            and rec.order_number = ord.order_number
            and rec.date_key >= "{date_start}"
    where
        rec.dc_site = 'DC1'
        and ord.order_status != 'L'   -- cancelled orders
        -- and ord.mother_warehouse_code not in ('KP01', 'kp01')   -- excluded BPs
        and ord.mother_warehouse_code in ('KS01', 'KX01')
        and ord.date_key between "{date_start}" and "{date_end}"
    group by
        rec.item_code,
        rec.sub_code,
        rec.dept_code,
        c.holding_code,
        rec.date_key
),

service_lvl as
(
    select
        item_code,
        sub_code,
        dept_code,
        holding_code,
        date_key,
        sum(trxn_qty) as trxn_qty_sum,
        sum(basic_order_qty) as basic_order_qty_sum,
        count(*) as last_trxns_count,
        -- calculate service level (+0.001 is to avoid error when 0/0)
        case 
            when sum(trxn_qty) = sum(basic_order_qty)
            then 1 
            else sum(trxn_qty) / (sum(basic_order_qty) + 0.001) 
        end as service_level
    from
        combine_ordered_received a
    group by
        a.item_code,
        a.sub_code,
        a.dept_code,
        a.date_key,
        a.holding_code
)

select * from service_lvl
