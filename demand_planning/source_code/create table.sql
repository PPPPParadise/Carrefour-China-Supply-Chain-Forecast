CREATE TABLE vartefact.forecast_item_code_id_stock (
	store_code STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	item_id INT,
	sub_id INT,
	balance_qty DECIMAL(15, 3),
	main_supplier STRING,
	ds_supplier STRING,
	stop_month STRING,
	stop_year STRING,
	stop_reason STRING
	) PARTITIONED BY (date_key STRING) STORED AS parquet
    
CREATE TABLE vartefact.forecast_p4cm_store_item (
	store_code STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	item_stop STRING,
	item_stop_reason STRING,
	item_stop_start_date STRING,
	item_stop_end_date STRING,
	shelf_capacity STRING
	) PARTITIONED BY (date_key STRING)
	stored as parquet
    
CREATE TABLE vartefact.forecast_lfms_daily_dcstock (
	item_id INT,
	sub_id INT,
	holding_code STRING,
	stock_available_sku DECIMAL(15, 3),
	dc_site STRING,
	warehouse_code STRING
	) PARTITIONED BY (date_key STRING) stored AS parquet
    
CREATE TABLE vartefact.forecast_onstock_orders_hist (
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
    
	order_day STRING,
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	run_date STRING,
	item_id INT,
	sub_id INT
	) STORED AS PARQUET;
    
CREATE TABLE vartefact.forecast_xdock_orders_hist (
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,

	order_day STRING,
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	run_date STRING,
	item_id INT,
	sub_id INT
	) STORED AS PARQUET;
         
CREATE TABLE vartefact.forecast_onstock_orders (
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	order_day STRING
	) STORED AS PARQUET;
    
CREATE TABLE vartefact.forecast_xdock_orders (
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	order_day STRING
	) STORED AS PARQUET;
    
    
CREATE TABLE vartefact.forecast_dc_orders (
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	supplier_code STRING,
	delivery_day STRING,
	average_sales DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	order_day STRING
	) STORED AS PARQUET;
    
CREATE TABLE vartefact.forecast_dc_orders_hist (
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	supplier_code STRING,

	order_day STRING,
	delivery_day STRING,
	average_sales DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	run_date STRING,
	item_id INT,
	sub_id INT
	) STORED AS PARQUET;
    
CREATE TABLE vartefact.forecast_simulation_orders_hist (
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
         
         
CREATE TABLE vartefact.forecast_simulation_orders (
	item_id INT,
	sub_id INT,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
    
	con_holding STRING,
	store_code STRING,
	supplier_code STRING,
    rotation STRING,
    
	delivery_day STRING,
	minimum_stock_required DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) PARTITIONED BY (
	order_day STRING,
    flow_type STRING
	) STORED AS PARQUET;    
    
CREATE TABLE vartefact.forecast_simulation_result (
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
 
CREATE TABLE vartefact.forecast_simulation_stock (
	item_id INT,
	sub_id INT,
	store_code STRING,
	rotation STRING,   
	dept_code STRING,
	day_end_stock_with_actual DOUBLE
	) PARTITIONED BY (
	date_key STRING,
	flow_type STRING
	) STORED AS PARQUET;
    
CREATE TABLE vartefact.forecast_simulation_item_status (
	item_id INT,
	sub_id INT,
	store_code STRING,
	rotation STRING,   
	dept_code STRING,
	item_stop_start_date STRING,   
	item_stop_end_date STRING,
	order_date STRING,
	shelf_capacity STRING,
	ittreplentyp INT,
	ittminunit INT
	) PARTITIONED BY (
	delivery_date STRING,
	flow_type STRING
	) STORED AS PARQUET;

CREATE TABLE vartefact.forecast_dm_orders (
	item_id INT,
	sub_id INT,
	store_code STRING,
	theme_start_date STRING,
	theme_end_date STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	city_code STRING,
	dept_code STRING,
	dept STRING,
	item_code STRING,
	sub_code STRING,
	pcb STRING,
	dc_supplier_code STRING,
	ds_supplier_code STRING,
	rotation STRING,
	run_date STRING,
	first_order_date STRING,
	first_delivery_date STRING,
	sales_before_order DOUBLE,
	order_received DOUBLE,
	regular_sales_before_dm DOUBLE,
	four_weeks_after_dm DOUBLE,
	dm_sales DOUBLE,
	current_store_stock DOUBLE,
	dm_order_qty_with_pcb DOUBLE
	) partitioned by (	dm_theme_id INT)
  stored as parquet;
  
CREATE TABLE vartefact.forecast_dc_latest_sales (
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	con_holding STRING,
	flow_type STRING,
	rotation STRING,
	pcb STRING,
	ds_supplier_code STRING,
	max_date_key STRING,
    avg_sales_qty DOUBLE
	) PARTITIONED BY (date_key STRING) 
  STORED AS PARQUET
    
create view vartefact.v_forecast_daily_sales_prediction as
select
reg.item_id,		
	reg.sub_id,		
	reg.store_code,		
	reg.week_key,	
	reg.prediction_max,	
	reg.prediction,	
	reg.order_prediction,	
	reg.date_key,
	reg.weekday_percentage,
	reg.impacgt,
	reg.impacted_weekday_percentage,
	reg.daily_sales_pred,
	reg.daily_sales_pred_original,
	reg.daily_order_pred,
	dm.dm_to_daily_pred,
	dm.dm_to_daily_pred_original,
	dm.dm_to_daily_order_pred,
 case when dm.dm_to_daily_pred_original is null
    then reg.daily_sales_pred
    else dm.dm_to_daily_pred_original
 end as daily_sales_prediction
from vartefact.forecast_regular_results_week_to_day_original_pred reg
left outer join vartefact.forecast_dm_results_to_day dm
on reg.item_id = dm.item_id
and reg.sub_id = dm.sub_id
and reg.store_code = dm.store_code
and reg.date_key = dm.date_key
