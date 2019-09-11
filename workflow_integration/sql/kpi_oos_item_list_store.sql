with store_stock_in_scope as
(
    select
        a.store_code, 
        a.date_key, 
        a.dept_code,
        a.item_code,
        a.sub_code,
        a.balance_qty,
        b.rotation,
        b.con_holding,
        b.ds_supplier_code,
        b.cn_name,
        concat(a.dept_code, a.item_code, a.sub_code) as full_item_code,
        a.item_id,
        a.sub_id
    from
        fds.p4cm_daily_stock a
        
    join {database_name}.v_forecast_inscope_store_item_details b
        on a.dept_code = b.dept_code
        and a.item_code = b.item_code
        and a.sub_code = b.sub_code
        and a.store_code = b.store_code
    
    where
        a.date_key = "{oos_check_date}"
)

select itm.*, 
coalesce(fr.daily_sales_prediction, -1) as sales_prediction_with_confidence_interval, 
coalesce(fr.daily_sales_prediction_original, -1) as sales_prediction
from store_stock_in_scope itm
left outer join vartefact.t_forecast_daily_sales_prediction fr
    on itm.item_id = fr.item_id
    and itm.sub_id = fr.sub_id
    and itm.store_code = fr.store_code
    and fr.date_key = "{oos_check_date}"
where balance_qty <= 0 
order by store_code, rotation, con_holding, item_code