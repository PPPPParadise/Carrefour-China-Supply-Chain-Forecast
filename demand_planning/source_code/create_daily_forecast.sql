insert into vartefact.t_forecast_daily_sales_prediction 
partition(date_key)
SELECT reg.item_id,
	reg.sub_id,
	reg.store_code,
	reg.week_key,
	reg.daily_sales_pred,
	reg.daily_sales_pred_original,
	reg.daily_order_pred,
	dm.dm_to_daily_pred,
	dm.dm_to_daily_pred_original,
	dm.dm_to_daily_order_pred,
	CASE 
		WHEN dm.dm_to_daily_pred_original IS NULL
			THEN reg.daily_sales_pred
		ELSE dm.dm_to_daily_pred_original
		END AS daily_sales_prediction,
	CASE 
		WHEN dm.dm_to_daily_pred_original IS NULL
			THEN reg.daily_sales_pred_original
		ELSE dm.dm_to_daily_pred_original
		END AS daily_sales_prediction_original,
	reg.date_key
FROM vartefact.forecast_daily_normal_view reg
LEFT OUTER JOIN vartefact.forecast_daily_dm_view  dm ON reg.item_id = dm.item_id
	AND reg.sub_id = dm.sub_id
	AND reg.store_code = dm.store_code
	AND reg.date_key = dm.date_key
	AND reg.date_key > ?
	AND dm.dm_theme_id in ( 29650,29688,29690,29733)
    