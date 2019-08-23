CREATE TABLE vartefact.forecast_simulation_orders_hist_without_ci (
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
    rotation STRING,

	order_day STRING,
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	run_date STRING,
	flow_type STRING
	) STORED AS PARQUET;
         
    
CREATE TABLE vartefact.forecast_simulation_result_without_ci (
    date_key STRING,
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
    rotation STRING,
    
	order_day STRING,
	delivery_day STRING,
	order_qty INT,
 	order_without_pcb DOUBLE,
    is_order_day BOOLEAN,
    matched_sales_start_date STRING,
    matched_sales_end_date STRING,
    start_stock DOUBLE, 
    future_stock DOUBLE, 
    minimum_stock_required DOUBLE,
    dm_delivery DOUBLE, 
    order_delivery DOUBLE,
    predict_sales DOUBLE,
    day_end_stock_with_predict DOUBLE, 
    actual_sales DOUBLE, 
    day_end_stock_with_actual DOUBLE,
    ittreplentyp Integer,
    shelf_capacity String,
    ittminunit Integer
	) PARTITIONED BY (
	run_date STRING,
    flow_type STRING
	) STORED AS PARQUET;

CREATE VIEW vartefact.v_forecast_simulation_lastest_result_without_ci
AS
(
		SELECT r.*
		FROM vartefact.forecast_simulation_result_without_ci r
		JOIN (
			SELECT fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				fsr.flow_type,
				max(fsr.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_result_without_ci fsr
			GROUP BY fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				fsr.flow_type
			) t ON r.item_id = t.item_id
			AND r.sub_id = t.sub_id
			AND r.store_code = t.store_code
			AND r.date_key = t.date_key
			AND r.run_date = t.max_run_date
			AND r.flow_type = t.flow_type
		)

CREATE VIEW vartefact.v_forecast_simulation_stock_without_ci
AS
(
		SELECT r.date_key,
			r.item_id,
			r.sub_id,
			r.store_code,
			r.rotation,
			r.dept_code,
			r.flow_type,
			r.day_end_stock_with_actual
		FROM vartefact.forecast_simulation_result_without_ci r
		JOIN (
			SELECT fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				fsr.flow_type,
				max(fsr.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_result_without_ci fsr
			GROUP BY fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				fsr.flow_type
			) t ON r.item_id = t.item_id
			AND r.sub_id = t.sub_id
			AND r.store_code = t.store_code
			AND r.date_key = t.date_key
			AND r.run_date = t.max_run_date
			AND r.flow_type = t.flow_type
		)


CREATE VIEW vartefact.v_forecast_simulation_orders_without_ci
AS
(
		SELECT o.*
		FROM vartefact.forecast_simulation_orders_hist_without_ci o
		JOIN (
			SELECT fsoh.order_day,
				fsoh.item_id,
				fsoh.sub_id,
				fsoh.store_code,
                fsoh.flow_type,
				max(fsoh.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_orders_hist_without_ci fsoh
			GROUP BY fsoh.order_day,
				fsoh.item_id,
				fsoh.sub_id,
				fsoh.store_code,
                fsoh.flow_type
			) t ON o.item_id = t.item_id
			AND o.sub_id = t.sub_id
			AND o.store_code = t.store_code
			AND o.order_day = t.order_day
            AND o.flow_type = t.flow_type
			AND o.run_date = t.max_run_date
		)


select * from vartefact.v_forecast_simulation_lastest_result_without_ci where date_key='20190716' and flow_type='XDocking_02'
order by item_id, sub_id, store_code



CREATE VIEW temp.v_forecast_daily_sales_prediction AS
SELECT
    reg.item_id,
    reg.sub_id,
    reg.store_code,
    reg.week_key,
    reg.prediction_max,
    reg.prediction,
    reg.order_prediction,
    reg.date_key,
    reg.weekday_percentage,
    reg.impacted_weekday_percentage,
    reg.daily_sales_pred,
    reg.daily_sales_pred_original,
    reg.daily_order_pred,
    dm.dm_to_daily_pred,
    dm.dm_to_daily_pred_original,
    dm.dm_to_daily_order_pred,
    CASE 
		WHEN dm.dm_to_daily_pred IS NULL
			THEN reg.daily_sales_pred
		ELSE dm.dm_to_daily_pred
		END AS daily_sales_prediction,
	CASE 
		WHEN dm.dm_to_daily_pred_original IS NULL
			THEN reg.daily_sales_pred_original
		ELSE dm.dm_to_daily_pred_original
	END AS daily_sales_prediction_original
FROM
    temp.v_forecast_regular_results_week_to_day_original_pred_0617_0929 reg
    LEFT OUTER JOIN temp.forecast_dm_results_to_day_0617_0929 dm ON reg.item_id = dm.item_id
    AND reg.sub_id = dm.sub_id
    AND reg.store_code = dm.store_code
    AND reg.date_key = dm.date_key


insert into temp.t_forecast_simulation_daily_sales_prediction partition(date_key)
SELECT
    reg.item_id,
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
		ELSE dm.dm_to_daily_pred
		END AS daily_sales_prediction,
	CASE 
		WHEN dm.dm_to_daily_pred_original IS NULL
			THEN reg.daily_sales_pred_original
		ELSE dm.dm_to_daily_pred_original
	END AS daily_sales_prediction_original,
    reg.date_key
FROM
    temp.forecast_daily_normal_view reg
    LEFT OUTER JOIN temp.forecast_DM_results_to_day_filtered_predicted_at_0602 dm ON reg.item_id = dm.item_id
    AND reg.sub_id = dm.sub_id
    AND reg.store_code = dm.store_code
    AND reg.date_key = dm.date_key