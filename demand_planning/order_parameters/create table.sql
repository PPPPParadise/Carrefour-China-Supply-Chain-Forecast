CREATE TABLE vartefact.forecast_script_runs (
	insert_time TIMESTAMP,
	run_date STRING,
	run_status STRING,
	script_name STRING,
	script_type STRING,
	script_parameter STRING,
	output STRING,
	info STRING,
	error STRING
	) STORED AS parquet
    
CREATE TABLE vartefact.forecast_nsa_dm_extract_log (
	item_code STRING,
	sub_code STRING,
	city_code STRING,
	extract_order INT,
	dept_code STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	city_name STRING,
	holding_code STRING,
	item_id INT,
	sub_id INT,
	load_date timestamp
	) PARTITIONED BY (
	dm_theme_id INT, 
	date_key STRING) 
	STORED AS parquet

CREATE TABLE vartefact.forecast_item_code_id_stock (
	con_holding STRING,
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
	npp	decimal(15,4),
	shelf_capacity STRING
	) PARTITIONED BY (date_key STRING)
	stored as parquet
    
CREATE TABLE vartefact.forecast_lfms_daily_dcstock (
	item_id INT,
	sub_id INT,
	holding_code STRING,
	stock_available_sku DECIMAL(15, 3),
	stock_shipment_sku DECIMAL(15, 3),
	stock_in_transit_sku DECIMAL(15, 3),
	stock_in_block_sku DECIMAL(15, 3),
	stock_transfer_by_sku DECIMAL(15, 3),
	last_receiving_date	string,
	next_receiving_date	string,
	load_date	timestamp,
	dc_site STRING,
	warehouse_code STRING
	) PARTITIONED BY (date_key STRING) stored AS parquet
    
CREATE TABLE vartefact.forecast_dc_latest_sales (
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	con_holding STRING,
	rotation STRING,
	ds_supplier_code STRING,
	max_date_key STRING,
    avg_sales_qty DOUBLE
	) PARTITIONED BY (date_key STRING) 
  STORED AS PARQUET
    
    
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
 

CREATE TABLE vartefact.forecast_dm_orders (
	item_id INT,
	sub_id INT,
	store_code STRING,
	con_holding STRING,
	theme_start_date STRING,
	theme_end_date STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	city_code STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	pcb STRING,
	dc_supplier_code STRING,
	ds_supplier_code STRING,
	rotation STRING,
	run_date STRING,
	first_order_date STRING,
	first_delivery_date STRING,
	regular_sales_before_dm DOUBLE,
	four_weeks_after_dm DOUBLE,
	dm_sales DOUBLE,
	order_qty INT,
	first_dm_order_qty INT,
	order_without_pcb DOUBLE
	) partitioned by (
	dm_theme_id INT)
  stored as parquet;
  
CREATE TABLE vartefact.forecast_dm_dc_orders (
	item_id INT,
	sub_id INT,
	con_holding STRING,
	theme_start_date STRING,
	theme_end_date STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	pcb STRING,
	ds_supplier_code STRING,
	rotation STRING,
	run_date STRING,
	first_order_date STRING,
	first_delivery_date STRING,
	regular_sales_before_dm DOUBLE,
	four_weeks_after_dm DOUBLE,
	dm_sales DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) partitioned by (
	dm_theme_id INT)
  stored as parquet;
  
CREATE TABLE vartefact.t_forecast_daily_sales_prediction (
    item_id INT,
    sub_id INT,
    store_code STRING,
    week_key STRING,
    daily_sales_pred DOUBLE,
    daily_sales_pred_original DOUBLE,
    daily_order_pred DOUBLE,
    dm_to_daily_pred DOUBLE,
    dm_to_daily_pred_original DOUBLE,
    dm_to_daily_order_pred DOUBLE,
    daily_sales_prediction DOUBLE,
    daily_sales_prediction_original DOUBLE
) PARTITIONED BY (date_key STRING) STORED AS PARQUET 

CREATE TABLE vartefact.forecast_store_daily_order_files (
    store_code STRING,
    dept_code STRING,
    supplier_code STRING,
    item_code STRING,
    sub_code STRING,
    order_qty STRING,
    free_goods_qty STRING,
    delv_yyyymmdd STRING,
    order_qty_in_pieces STRING,
    order_by STRING,
    qty_per_pack STRING,
    pack_per_box STRING,
    regular_order STRING,
    regular_order_without_pcb STRING,
    dm_order STRING,
    dm_order_without_pcb STRING,
    ppp STRING,
    npp STRING,
    4_weeks_after_dm_order STRING
) PARTITIONED BY (date_key STRING) 
  STORED AS PARQUET

CREATE TABLE vartefact.forecast_dc_daily_order_files (
    supplier_code STRING,
    warehouse STRING,
    delivery_date STRING,
    item_code STRING,
    item_name STRING,
    item_subcode_name_local STRING,
    poq_quantity STRING,
    purchase_quantity STRING,
    unit STRING,
    purchase_price STRING,
    purchase_amount STRING,
    unit_dc_discount STRING,
    unit_percent_discount STRING,
    additional_free_goods STRING,
    npp STRING,
    main_barcode STRING,
	order_in_pieces STRING,
    service_level STRING,
    regualr_order_in_pieces STRING,
    dm_order_in_pieces STRING
) PARTITIONED BY (date_key STRING) 
  STORED AS PARQUET

CREATE TABLE vartefact.foreacst_store_monitor (
    store_code STRING,
    rotation STRING, 
    con_holding STRING,
    ds_supplier_code STRING,
    in_dm INT,
    date_key STRING,
    n_items INT,
    n_not_oos_items INT,
    total_stock DECIMAL(15, 4),
    total_stock_value DECIMAL(15, 4)
	) PARTITIONED BY (
	run_date STRING) 
	STORED AS parquet


CREATE TABLE vartefact.foreacst_dc_monitor (
    dc_site STRING,
    rotation STRING, 
    holding_code STRING,
    ds_supplier_code STRING,
    in_dm INT,
    date_key STRING,
    n_items INT,
    n_not_oos_items INT,
    total_stock DECIMAL(15, 4),
    total_stock_value DECIMAL(15, 4)
	) PARTITIONED BY (
	run_date STRING) 
	STORED AS parquet

CREATE TABLE vartefact.forecast_monitor_dc_order_diff (
    dept_code STRING,
    item_code STRING,
    sub_code STRING,
    qty_per_box STRING,
    holding_code STRING,
    risk_item_unilever STRING,
    rotation STRING,
    supplier_code STRING,
    warehouse STRING,
    delivery_date STRING,
    cn_name STRING,
    purchase_quantity STRING,
    main_barcode STRING,
    service_level STRING,
    artefact_order_qty STRING,
    actual_order_qty STRING,
    actual_order_numbers STRING,
    order_qty_diff STRING
) PARTITIONED BY (date_key STRING) STORED AS PARQUET

CREATE TABLE vartefact.forecast_monitor_store_order_diff (
    store_code STRING,
    dept_code STRING,
    item_code STRING,
    sub_code STRING,
    cn_name STRING,
    rotation STRING,
    supplier_code STRING,
    holding_code STRING,
    risk_item_unilever STRING,
    artefact_order_qty STRING,
    actual_order_qty STRING,
    order_qty_diff STRING
) PARTITIONED BY (date_key STRING) STORED AS PARQUET 

CREATE VIEW vartefact.v_forecast_inscope_store_item_details AS
SELECT
    id.*
FROM
    vartefact.forecast_store_item_details id
WHERE
    id.store_status != 'Stop'
    AND id.item_type NOT IN ('New', 'Company Purchase', 'Seasonal')


create view vartefact.v_forecast_inscope_dc_item_details as (
    select
        distinct dc.*
    from
        vartefact.forecast_dc_item_details dc
        join vartefact.forecast_store_item_details id ON dc.dept_code = id.dept_code
        AND dc.item_code = id.item_code
        AND dc.sub_code = id.sub_code
    where
        dc.dc_status != 'Stop'
        AND dc.seasonal = 'No'
        AND dc.item_type not in ('New','Company Purchase','Seasonal')
)
    
CREATE TABLE vartefact.forecast_simulation_dm_orders (
	item_id INT,
	sub_id INT,
	store_code STRING,
	con_holding STRING,
	theme_start_date STRING,
	theme_end_date STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	city_code STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	pcb STRING,
	dc_supplier_code STRING,
	ds_supplier_code STRING,
	rotation STRING,
	run_date STRING,
	first_order_date STRING,
	first_delivery_date STRING,
	regular_sales_before_dm DOUBLE,
	four_weeks_after_dm DOUBLE,
	dm_sales DOUBLE,
	order_qty INT,
	first_dm_order_qty INT,
	order_without_pcb DOUBLE
	) partitioned by (
	dm_theme_id INT)
  stored as parquet;
  
CREATE TABLE vartefact.forecast_simulation_dm_dc_orders (
	item_id INT,
	sub_id INT,
	con_holding STRING,
	theme_start_date STRING,
	theme_end_date STRING,
	npp DECIMAL(15, 4),
	ppp DECIMAL(15, 4),
	ppp_start_date STRING,
	ppp_end_date STRING,
	dept_code STRING,
	item_code STRING,
	sub_code STRING,
	pcb STRING,
	ds_supplier_code STRING,
	rotation STRING,
	run_date STRING,
	first_order_date STRING,
	first_delivery_date STRING,
	regular_sales_before_dm DOUBLE,
	four_weeks_after_dm DOUBLE,
	dm_sales DOUBLE,
	order_qty INT,
	order_without_pcb DOUBLE
	) partitioned by (
	dm_theme_id INT)
  stored as parquet;
    
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
    day_end_stock_with_actual DOUBLE
	) PARTITIONED BY (
	run_date STRING,
    flow_type STRING
	) STORED AS PARQUET;


CREATE VIEW vartefact.v_forecast_simulation_lastest_result
AS
(
		SELECT r.*
		FROM vartefact.forecast_simulation_result r
		JOIN (
			SELECT fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				max(fsr.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_result fsr
			GROUP BY fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code
			) t ON r.item_id = t.item_id
			AND r.sub_id = t.sub_id
			AND r.store_code = t.store_code
			AND r.date_key = t.date_key
			AND r.run_date = t.max_run_date
		)


CREATE VIEW vartefact.v_forecast_simulation_stock
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
		FROM vartefact.forecast_simulation_result r
		JOIN (
			SELECT fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code,
				max(fsr.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_result fsr
			GROUP BY fsr.date_key,
				fsr.item_id,
				fsr.sub_id,
				fsr.store_code
			) t ON r.item_id = t.item_id
			AND r.sub_id = t.sub_id
			AND r.store_code = t.store_code
			AND r.date_key = t.date_key
			AND r.run_date = t.max_run_date
		)


CREATE VIEW vartefact.v_forecast_simulation_orders
AS
(
		SELECT o.*
		FROM vartefact.forecast_simulation_orders_hist o
		JOIN (
			SELECT fsoh.order_day,
				fsoh.item_id,
				fsoh.sub_id,
				fsoh.store_code,
				max(fsoh.run_date) AS max_run_date
			FROM vartefact.forecast_simulation_orders_hist fsoh
			GROUP BY fsoh.order_day,
				fsoh.item_id,
				fsoh.sub_id,
				fsoh.store_code
			) t ON o.item_id = t.item_id
			AND o.sub_id = t.sub_id
			AND o.store_code = t.store_code
			AND o.order_day = t.order_day
			AND o.run_date = t.max_run_date
		)
        
CREATE view vartefact.v_forecast_daily_onstock_order_items AS
SELECT
    *
from
    (
        select
            distinct id.dept_code,
            id.item_code,
            id.sub_code,
			id.cn_name,
            id.con_holding,
            id.store_code,
            id.rotation,
            id.ds_supplier_code,
            id.dc_supplier_code,
            ord.date_key as order_day,
            trim(coalesce(stp.item_stop_start_date, '')) as item_stop_start_date,
            trim(coalesce(stp.item_stop_end_date, '')) as item_stop_end_date
        from
            vartefact.v_forecast_inscope_store_item_details id
            join vartefact.forecast_onstock_order_delivery_mapping mp
              on id.dept_code = mp.dept_code
              and id.rotation = mp.rotation
              and id.store_code = mp.store_code
            join vartefact.forecast_calendar ord 
              on ord.iso_weekday = mp.order_iso_weekday
            left join vartefact.forecast_p4cm_store_item stp 
              on id.item_code = stp.item_code
              and id.sub_code = stp.sub_code
              and id.store_code = stp.store_code
              and id.dept_code = stp.dept_code
              and ord.date_key = stp.date_key
    ) t
where
    (
        item_stop_start_date = ''
        and item_stop_end_date = ''
    )
    OR (
        item_stop_start_date != ''
        and to_timestamp(order_day, 'yyyyMMdd') < to_timestamp(item_stop_start_date, 'dd/MM/yyyy')
    )
    OR (
        item_stop_end_date != ''
        and to_timestamp(order_day, 'yyyyMMdd') > to_timestamp(item_stop_end_date, 'dd/MM/yyyy')
    )
                 
                 
CREATE view vartefact.v_forecast_daily_xdock_order_items AS
SELECT
    *
from
    (
        select
            distinct id.dept_code,
            id.item_code,
            id.sub_code,
			id.cn_name,
            id.con_holding,
            id.store_code,
            id.rotation,
            id.ds_supplier_code,
            id.dc_supplier_code,
            ord.date_key as order_day,
            trim(coalesce(stp.item_stop_start_date, '')) as item_stop_start_date,
            trim(coalesce(stp.item_stop_end_date, '')) as item_stop_end_date
        from
            vartefact.v_forecast_inscope_store_item_details id
        join vartefact.forecast_xdock_order_mapping xo
            on xo.item_code = id.item_code
            and xo.sub_code = id.sub_code
            and xo.dept_code = id.dept_code
            and xo.store_code = id.store_code
        join vartefact.forecast_calendar ord 
            on ord.iso_weekday = xo.order_iso_weekday
		join vartefact.forecast_dc_order_delivery_mapping dodm
			on dodm.con_holding = id.con_holding
			and dodm.order_date = ord.date_key
			and dodm.risk_item_unilever = id.risk_item_unilever
        left join vartefact.forecast_p4cm_store_item stp 
            on id.item_code = stp.item_code
            and id.sub_code = stp.sub_code
            and id.store_code = stp.store_code
            and id.dept_code = stp.dept_code
            and ord.date_key = stp.date_key
    ) t
where
    (
        item_stop_start_date = ''
        and item_stop_end_date = ''
    )
    OR (
        item_stop_start_date != ''
        and to_timestamp(order_day, 'yyyyMMdd') < to_timestamp(item_stop_start_date, 'dd/MM/yyyy')
    )
    OR (
        item_stop_end_date != ''
        and to_timestamp(order_day, 'yyyyMMdd') > to_timestamp(item_stop_end_date, 'dd/MM/yyyy')
    )

create view vartefact.v_forecast_weekly_xdock_order_forecast as
SELECT
    dc.holding_code,
    dc.primary_barcode,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.item_name_english,
    wst.date_key as week_start_day,
    sum(
        ceil(
            coalesce(ord.order_qty, 0) * (2 - coalesce(sl.service_level, 1)) / dc.qty_per_box
        )
    ) as order_qty,
    sum(ceil(coalesce(dm.order_qty, 0) / dc.qty_per_box)) as dm_qty
FROM
    vartefact.forecast_calendar cal
    JOIN vartefact.forecast_calendar wst ON wst.week_index = cal.week_index
    AND wst.weekday_short = 'Mon'
    JOIN vartefact.v_forecast_inscope_dc_item_details dc
	on dc.rotation = 'X'
    LEFT JOIN vartefact.forecast_xdock_orders ord ON ord.order_day = cal.date_key
    AND ord.item_code = dc.item_code
    AND ord.sub_code = dc.sub_code
    AND ord.dept_code = dc.dept_code
    LEFT JOIN vartefact.forecast_dm_orders dm ON ord.store_code = dm.store_code
    AND ord.dept_code = dm.dept_code
    AND ord.item_code = dm.item_code
    AND ord.sub_code = dm.sub_code
    AND ord.order_day = dm.first_order_date
    LEFT JOIN vartefact.service_level_safety2_vinc sl on ord.item_code = sl.item_code
    and ord.sub_code = sl.sub_code
    and ord.dept_code = sl.dept_code
group by
    dc.holding_code,
    dc.primary_barcode,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.item_name_english,
    wst.date_key

create view vartefact.v_forecast_weekly_dc_order_forecast as
SELECT
    dc.holding_code,
    dc.primary_barcode,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.item_name_english,
    wst.date_key as week_start_day,
    sum(
        ceil(
            coalesce(ord.order_qty, 0) * (2 - coalesce(sl.service_level, 1)) / dc.qty_per_box
        )
    ) as order_qty,
    sum(ceil(coalesce(dm.order_qty, 0) / dc.qty_per_box)) as dm_qty
FROM
    vartefact.forecast_calendar cal
    JOIN vartefact.forecast_calendar wst ON wst.week_index = cal.week_index
    AND wst.weekday_short = 'Mon'
    JOIN vartefact.v_forecast_inscope_dc_item_details dc
	on dc.rotation != 'X'
    LEFT JOIN vartefact.forecast_dc_orders ord ON ord.order_day = cal.date_key
    AND ord.item_code = dc.item_code
    AND ord.sub_code = dc.sub_code
    AND ord.dept_code = dc.dept_code
    LEFT JOIN vartefact.forecast_dm_dc_orders dm ON ord.dept_code = dm.dept_code
    AND ord.item_code = dm.item_code
    AND ord.sub_code = dm.sub_code
    AND ord.order_day = dm.first_order_date
    LEFT JOIN vartefact.service_level_safety2_vinc sl on ord.item_code = sl.item_code
    and ord.sub_code = sl.sub_code
    and ord.dept_code = sl.dept_code
group by
    dc.holding_code,
    dc.primary_barcode,
    dc.dept_code,
    dc.item_code,
    dc.sub_code,
    dc.item_name_local,
    dc.item_name_english,
    wst.date_key