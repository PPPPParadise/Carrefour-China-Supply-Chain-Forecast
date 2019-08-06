-- create forecast_item_code_id_stock table
INSERT overwrite vartefact.forecast_item_code_id_stock (
	date_key,
	store_code,
	dept_code,
	item_code,
	sub_code,
	item_id,
	sub_id,
	balance_qty,
	main_supplier,
	ds_supplier,
	stop_year,
	stop_month,
	stop_reason
	)
SELECT DISTINCT pds.date_key,
	pds.store_code,
	pds.dept_code,
	pds.item_code,
	pds.sub_code,
	pds.item_id,
	pds.sub_id,
	pds.balance_qty,
	pds.main_supplier,
	pds.ds_supplier,
	pds.stop_year,
	pds.stop_month,
	pds.stop_reason
FROM fds.p4cm_daily_stock pds
JOIN vartefact.item_details id ON 
    pds.item_code = id.item_code
	AND pds.sub_code = id.sub_code
    AND pds.dept_code = id.dept_code
WHERE date_key >= '20190101'


-- create forecast_p4cm_store_item table
insert overwrite vartefact.forecast_p4cm_store_item (
	store_code ,
	dept_code ,
	item_code ,
	sub_code ,
	item_stop ,
	item_stop_reason ,
	item_stop_start_date ,
	item_stop_end_date ,
	shelf_capacity ,
	date_key)
SELECT DISTINCT store_code ,
	psi.dept_code ,
	psi.item_code ,
	psi.sub_code ,
	psi.item_stop ,
	psi.item_stop_reason ,
	psi.item_stop_start_date ,
	psi.item_stop_end_date ,
	psi.shelf_capacity ,
	psi.date_key
FROM ods.p4cm_store_item psi
JOIN vartefact.item_details id ON psi.item_code = id.item_code
	AND psi.sub_code = id.sub_code
    and psi.dept_code = id.dept_code
WHERE psi.date_key >= '20190101'


-- create forecast_dc_latest_sales table
INSERT overwrite TABLE vartefact.forecast_dc_latest_sales PARTITION (date_key)
SELECT tmp.dept_code,
	tmp.item_code,
	tmp.sub_code,
	tmp.con_holding,
	tmp.flow_type,
	tmp.rotation,
	tmp.pcb,
	tmp.dc_supplier_code,
	tmp.max_date_key,
	cast(ord.avg_sales_qty AS DOUBLE) avg_sales_qty,
	'20190107' AS date_key
FROM (
	SELECT id.dept_code,
		id.item_code,
		id.sub_code,
		id.con_holding,
		id.flow_type,
		id.rotation,
		id.pcb,
		id.dc_supplier_code,
		max(ord.date_key) AS max_date_key
	FROM vartefact.forecast_item_details id
	JOIN lfms.ord ord ON id.item_code = ord.item_code
		AND id.sub_code = ord.sub_code
		AND id.dept_code = ord.department_code
	WHERE id.rotation != 'X'
		AND ord.dc_site = 'DC1'
		AND ord.date_key <= '20190107'
	GROUP BY id.dept_code,
		id.item_code,
		id.sub_code,
		id.con_holding,
		id.flow_type,
		id.rotation,
		id.pcb,
		id.dc_supplier_code
	) tmp
JOIN lfms.ord ord ON ord.date_key = tmp.max_date_key
	AND tmp.item_code = ord.item_code
	AND tmp.sub_code = ord.sub_code
	AND tmp.dept_code = ord.department_code
	AND ord.dc_site = 'DC1'