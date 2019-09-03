INSERT overwrite TABLE vartefact.forecast_dc_latest_sales PARTITION (date_key)
SELECT distinct tmp.dept_code,
	tmp.item_code,
	tmp.sub_code,
	tmp.holding_code,
	tmp.rotation,
	tmp.primary_ds_supplier,
	tmp.max_date_key,
	cast(ord2.avg_sales_qty AS DOUBLE) avg_sales_qty,
	'{0}' AS date_key
FROM (
	SELECT id.dept_code,
		id.item_code,
		id.sub_code,
		id.holding_code,
		id.rotation,
		id.primary_ds_supplier,
		max(ord.date_key) AS max_date_key
	FROM vartefact.forecast_dc_item_details id
	JOIN lfms.ord ord ON id.item_code = ord.item_code
		AND id.sub_code = ord.sub_code
		AND id.dept_code = ord.department_code
	WHERE id.rotation != 'X'
		AND ord.dc_site = 'DC1'
		AND ord.date_key <= '{0}'
	GROUP BY id.dept_code,
		id.item_code,
		id.sub_code,
		id.holding_code,
		id.rotation,
		id.primary_ds_supplier
	) tmp
JOIN lfms.ord ord2 ON ord2.date_key = tmp.max_date_key
	AND tmp.item_code = ord2.item_code
	AND tmp.sub_code = ord2.sub_code
	AND tmp.dept_code = ord2.department_code
	AND ord2.dc_site = 'DC1'
