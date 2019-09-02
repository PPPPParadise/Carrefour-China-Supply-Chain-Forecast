INSERT overwrite vartefact.forecast_lfms_daily_dcstock PARTITION (date_key)
SELECT item_id,
	sub_id,
	holding_code,
	stock_available_sku,
	stock_shipment_sku,
	stock_in_transit_sku,
	stock_in_block_sku,
	stock_transfer_by_sku,
	last_receiving_date,
	next_receiving_date,
	load_date,
	dc_site,
	warehouse_code,
	date_key
FROM lfms.daily_dcstock
WHERE date_key = '{0}'
	AND dc_site = 'DC1'
