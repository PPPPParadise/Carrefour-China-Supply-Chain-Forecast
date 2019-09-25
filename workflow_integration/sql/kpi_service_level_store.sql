/* =======================================================
                      Module KPI Monitor
                        Service level- Store level
==========================================================
*/

/*
Parameters:
    * database_name, e.g. vartefact
    * date_start & date_end, date range, e.g. '20190101' & '20190830'
Input:
    * fds.p4cm_daily_order 
    * lfms.daily_shipment 
    * vartefact.v_forecast_inscope_store_item_details 
    * vartefact.v_forecast_inscope_dc_item_details   -- items in scope
To:
    * {database_name}.monitor_service_level_item_dc
*/

with order_qty_tb as (
SELECT 
    pdo.date_key,
    pdo.order_date, 
    pdo.store_code,
    pdo.item_id,
    pdo.sub_id,
    pdo.item_code,
    pdo.sub_code,
    pdo.dept_code,
    pdo.supplier_code,
    pdo.order_number,
    pdo.order_qty, 
    pdo.ord_unit_qty, 
    pdo.qty_per_pack,
    pdo.order_qty * pdo.ord_unit_qty as order_qty_in_sku,
    id.con_holding,
    row_number() OVER (PARTITION BY pdo.store_code, pdo.item_code, 
                           pdo.sub_code, pdo.dept_code, 
                           pdo.supplier_code, pdo.order_number
                        ORDER BY pdo.date_key desc) as row_
    
FROM fds.p4cm_daily_order pdo
JOIN vartefact.v_forecast_inscope_store_item_details id
    ON pdo.store_code = id.store_code
    AND pdo.dept_code = id.dept_code
    AND pdo.item_code = id.item_code
    AND pdo.sub_code = id.sub_code
where pdo.order_status = 'C' 
and pdo.order_date between to_timestamp("{date_start}", 'yyyyMMdd') and to_timestamp("{date_end}", 'yyyyMMdd')
), 

order_qty_clean as (
select 
    order_date, 
    store_code,
    item_id,
    sub_id,
    item_code,
    sub_code,
    dept_code,
    supplier_code,
    order_number,
    order_qty, 
    ord_unit_qty, 
    qty_per_pack,
    order_qty_in_sku,
    con_holding
from order_qty_tb
where row_ = 1
group by 
    order_date, 
    store_code,
    item_id,
    sub_id,
    item_code,
    sub_code,
    dept_code,
    supplier_code,
    order_number,
    order_qty, 
    ord_unit_qty, 
    qty_per_pack,
    order_qty_in_sku,
    con_holding
),

store_receive as (
SELECT 
    lds.store_code,
    lds.dept_code,
    lds.item_code,
    lds.sub_code,
    lds.store_order_no, 
    substr(lds.store_order_no, 3, 4) as supplier_code,
    substr(lds.store_order_no, 7, 3) as order_number,
    sum(coalesce(lds.delivery_qty_in_sku, 0)) as delivery_qty_in_sku
FROM lfms.daily_shipment lds
JOIN vartefact.v_forecast_inscope_dc_item_details dc
    ON lds.dept_code = dc.dept_code
    AND lds.item_code = dc.item_code
    AND lds.sub_code = dc.sub_code
where lds.dc_site = 'DC1'
and lds.date_key >= "{date_start}"
and lds.rank = 1  
GROUP BY 
    lds.store_code,
    lds.dept_code,
    lds.item_code,
    lds.sub_code,
    lds.store_order_no,
    dc.holding_code
), 

order_receive as (
select 
    s_ord.order_date, 
    s_ord.store_code, 
    s_ord.item_id,
    s_ord.sub_id,
    s_ord.item_code,
    s_ord.sub_code,
    s_ord.dept_code,
    s_ord.supplier_code,
    s_ord.order_number,
    s_ord.order_qty, 
    s_ord.ord_unit_qty, 
    s_ord.order_qty_in_sku,
    coalesce(s_rec.delivery_qty_in_sku, 0) as delivery_qty_in_sku,
    s_ord.con_holding as holding_code 

from order_qty_clean s_ord

left join store_receive s_rec
on s_rec.store_code = s_ord.store_code
and s_rec.dept_code = s_ord.dept_code
and s_rec.item_code = s_ord.item_code
and s_rec.sub_code = s_ord.sub_code
and s_rec.supplier_code = s_ord.supplier_code
and s_rec.order_number = s_ord.order_number 

), 

service_lvl as (
select
    date_format(order_date, 'yyyyMMdd') as order_date,
    item_code,
    sub_code,
    dept_code,
    holding_code,
    sum(order_qty_in_sku) as order_qty_in_sku_sum, 
    sum(delivery_qty_in_sku) as delivery_qty_in_sku_sum,
    count(1) as orders_count,
    -- calculate service level (+0.001 is to avoid error when 0/0)
    case 
        when sum(delivery_qty_in_sku) = sum(order_qty_in_sku)
        then 1
        when sum(delivery_qty_in_sku) / (sum(order_qty_in_sku) + 0.00001) is null
        then 0
        else sum(delivery_qty_in_sku) / (sum(order_qty_in_sku) + 0.00001)
    end as service_level

from order_receive  
group by
    order_date,
    item_code,
    sub_code, 
    dept_code,
    holding_code
)

select * 
from service_lvl 
