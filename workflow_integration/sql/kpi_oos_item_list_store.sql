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
        b.cn_name
    
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

select *
from store_stock_in_scope 
where balance_qty <= 0 
order by store_code, rotation, con_holding, item_code