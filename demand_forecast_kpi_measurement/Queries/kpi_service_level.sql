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
    * vartefact.forecast_itemid_list_threebrands_sprint4   -- items in scope
To:
    * vartefact.monitor_service_level_item_dc
*/

drop table if exists {database_name}.monitor_service_level_item_dc;

create table {database_name}.monitor_service_level_item_dc as

with combine_ordered_received as
(
    select
        rec.item_code,
        rec.sub_code,
        rec.dept_code,
        -- rec.supplier_code,
        c.holding_code,
        -- ord.mother_warehouse_code,
        rec.date_key,
        sum(rec.trxn_qty) as trxn_qty,
        sum(ord.basic_order_qty) as basic_order_qty
    from
        lfms.daily_dctrxn rec  -- received
        inner join
            lfms.ord ord       -- ordered
        on
            rec.item_code = ord.item_code
            and rec.sub_code = ord.sub_code
            and rec.dept_code = ord.department_code
            and rec.supplier_code = ord.supplier_code
            and rec.order_number = ord.order_number
        left join
            {database_name}.forecast_itemid_list_threebrands_sprint4 c
        on rec.item_id = c.item_id
    where
        rec.item_id in (
            select item_id
            from {database_name}.forecast_itemid_list_threebrands_sprint4
        )
        and rec.dc_site = 'DC1'
        and ord.order_status <> 'L'   -- cancelled orders
        -- and ord.mother_warehouse_code not in ('KP01', 'kp01')   -- excluded BPs
        and ord.mother_warehouse_code in ('KS01', 'KX01')
        and rec.date_key between "{date_start}" and "{date_end}"
    group by
        rec.item_code,
        rec.sub_code,
        rec.dept_code,
        c.holding_code,
        rec.date_key
        -- ord.mother_warehouse_code
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
        sum(trxn_qty) / (sum(basic_order_qty) + 0.001) as service_level
    from
        combine_ordered_received a
    group by
        a.item_code,
        a.sub_code,
        a.dept_code,
        a.date_key,
        a.holding_code
        -- a.mother_warehouse_code
)

select * from service_lvl
