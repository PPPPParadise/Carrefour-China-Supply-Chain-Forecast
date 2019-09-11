/*
Store level: A B and X 
Input: {database_name}.forecast_store_daily_order_files 
       {database_name}.v_forecast_inscope_store_item_details
       fds.p4cm_daily_order  

*/
with 
    time_period as (
        select 
            distinct 
            date_key
        from {database_name}.forecast_store_daily_order_files 
        where date_key between '{date_start}' and '{date_end}'
    ),
    item_list as (
        select 
            cn_name, dept_code, item_code, sub_code, store_code, rotation, 
            con_holding as holding_code, risk_item_unilever
        from {database_name}.v_forecast_inscope_store_item_details
    ),
    complete_item as (
        select 
            tp.date_key,
            il.cn_name,
            il.dept_code,
            il.item_code, 
            il.sub_code,
            il.rotation,
            il.holding_code, 
            il.risk_item_unilever, 
            il.store_code 

        from time_period tp
        left join item_list il
        on 1=1
    ),

    order_qty_model as (
        select 
            a.cn_name,
            a.dept_code,
            a.item_code,
            a.sub_code,
            a.rotation, 
            a.holding_code, 
            a.risk_item_unilever,
            a.store_code,
            a.date_key, 
            b.supplier_code,                  
            b.order_qty_in_pieces as order_qty_model

        from complete_item a 
        left join {database_name}.forecast_store_daily_order_files b 
        on a.dept_code = b.dept_code
        and a.item_code = b.item_code
        and a.sub_code = b.sub_code 
        and a.store_code = b.store_code 
        and a.date_key = b.date_key
    ),

    order_qty_tb as (
        SELECT 
            date_key,
            date_format(order_date, 'yyyyMMdd') as order_date, 
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
            order_qty * ord_unit_qty as order_qty_in_sku,
            row_number() OVER (PARTITION BY store_code, item_code, sub_code, dept_code, 
                                supplier_code, order_number ORDER BY date_key desc) as row_

        FROM fds.p4cm_daily_order 
        where if(order_status is null, 'C', order_status) <> 'L' 
        and date_format(order_date, 'yyyyMMdd') between '{date_start}' and '{date_end}'
        and store_code in (SELECT stostocd 
                           from {database_name}.forecast_store_code_scope_sprint4)
        and item_id in (SELECT 
                            DISTINCT item_id 
                        from {database_name}.forecast_itemid_list_threebrands_sprint4)
        ), 

    order_qty_clean as (
    select 
        order_date,  store_code, item_id, sub_id,
        item_code, sub_code, dept_code, supplier_code,
        order_number, order_qty, ord_unit_qty, qty_per_pack,
        order_qty_in_sku
    from order_qty_tb
    where row_ = 1
    group by 
        order_date, store_code, item_id, sub_id,
        item_code, sub_code, dept_code, supplier_code,
        order_number, order_qty, ord_unit_qty, qty_per_pack,
        order_qty_in_sku
    ),

    carr_order_by_day as (
    select 
        order_date, 
        store_code,
        item_id,
        sub_id,
        item_code,
        sub_code,
        dept_code,
        supplier_code,
        sum(order_qty_in_sku) as order_qty_actual 

    from order_qty_clean 
    group by 
        order_date, 
        store_code,
        item_id,
        sub_id,
        item_code,
        sub_code,
        dept_code,
        supplier_code
    )

select 
    ord_model.date_key,
    ord_model.store_code,
    ord_model.dept_code,
    ord_model.item_code,
    ord_model.sub_code,
    ord_model.cn_name,
    ord_model.rotation, 
    ord_model.supplier_code,
    ord_model.holding_code, 
    ord_model.risk_item_unilever,
    if(ord_model.order_qty_model is null, 0, cast(ord_model.order_qty_model as int)) as order_qty_model,
    if(ord_act.order_qty_actual is null, 0, cast(ord_act.order_qty_actual as int)) as order_qty_actual
    
from order_qty_model ord_model
left join carr_order_by_day ord_act
on ord_model.dept_code = ord_act.dept_code 
and ord_model.item_code = ord_act.item_code 
and ord_model.sub_code = ord_act.sub_code
and ord_model.store_code = ord_act.store_code 
and ord_model.date_key = ord_act.order_date
and ord_model.supplier_code = ord_act.supplier_code 
                                                 
