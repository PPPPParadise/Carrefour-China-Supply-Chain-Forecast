INSERT overwrite vartefact.forecast_p4cm_store_item (
	store_code,
	dept_code,
	item_code,
	sub_code,
	item_stop,
	item_stop_reason,
	item_stop_start_date,
	item_stop_end_date,
	shelf_capacity,
	date_key
	)
SELECT DISTINCT psi.store_code,
	psi.dept_code,
	psi.item_code,
	psi.sub_code,
	psi.item_stop,
	psi.item_stop_reason,
	psi.item_stop_start_date,
	psi.item_stop_end_date,
	psi.shelf_capacity,
	psi.date_key
FROM ods.p4cm_store_item psi
JOIN vartefact.v_forecast_inscope_store_item_details id ON psi.item_code = id.item_code
	AND psi.sub_code = id.sub_code
	AND psi.dept_code = id.dept_code
    AND psi.store_code = id.store_code
WHERE psi.date_key = '{0}'
