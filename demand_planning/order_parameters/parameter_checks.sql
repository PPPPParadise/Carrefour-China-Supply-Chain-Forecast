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