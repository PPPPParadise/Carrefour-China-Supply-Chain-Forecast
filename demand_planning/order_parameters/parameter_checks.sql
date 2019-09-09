-- DC item not existing in store
select
    distinct dc.item_code,
    dc.sub_code,
    dc.holding_code,
    dc.dept_code,
    dc.rotation,
    dc.dc_status,
    dc.seasonal,
    id.store_status,
    id.item_type
from
    vartefact.v_forecast_inscope_dc_item_details dc
    left join vartefact.forecast_store_item_details id on id.item_code = dc.item_code
    and id.sub_code = dc.sub_code
    and id.dept_code = dc.dept_code
where
    id.store_status is null
    

-- Store item not existing in DC
select
    distinct id.item_code,
    id.sub_code,
    id.con_holding,
    id.dept_code,
    id.rotation,
    id.store_status,
    id.item_type,
    dc.dc_status,
    dc.seasonal
from
    vartefact.v_forecast_inscope_store_item_details id
    left join vartefact.forecast_dc_item_details dc on id.item_code = dc.item_code
    and id.sub_code = dc.sub_code
    and id.dept_code = dc.dept_code
where
    dc.dc_status is null


-- rotation not mathcing
select
    distinct id.item_code,
    id.sub_code,
    id.con_holding,
    id.dept_code,
    id.rotation as store_rotation,
    dc.rotation as dc_rotation,
    id.store_status,
    id.item_type,
    dc.dc_status,
    dc.seasonal
from
    vartefact.v_forecast_inscope_store_item_details id
    join vartefact.forecast_dc_item_details dc on id.item_code = dc.item_code
    and id.sub_code = dc.sub_code
    and id.dept_code = dc.dept_code
    and id.rotation != dc.rotation



select
    distinct concat_ws(dc.dept_code, dc.item_code, dc.sub_code) as dc_item,
    concat_ws(
        store.dept_code,
        store.item_code,
        store.sub_code
    ) as store_item,
    dc.dc_status as dc_dc_status,
    store.dc_status as store_dc_status,
    store.store_status as store_status,
    dc.item_type as dc_item_type,
    store.item_type as store_item_type
from
    vartefact.forecast_dc_item_details dc
    left join vartefact.forecast_store_item_details store on dc.dept_code = store.dept_code
    and dc.item_code = store.item_code
    and dc.sub_code = store.sub_code
where
    dc.dc_status != 'Stop'
    AND dc.seasonal = 'No'
    AND dc.item_type not in ('New', 'Company Purchase', 'Seasonal')
    and store.dc_status is null