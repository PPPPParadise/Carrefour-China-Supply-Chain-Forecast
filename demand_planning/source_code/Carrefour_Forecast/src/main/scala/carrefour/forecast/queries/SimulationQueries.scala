package carrefour.forecast.queries

import carrefour.forecast.config.SimulationTables

object SimulationQueries {

  def getActualSalesSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    select
        acts.item_id,
        acts.sub_id,
        acts.store_code as entity_code,
        acts.date_key,
        cast(acts.daily_sales_sum as double) as daily_sales_sum
    from vartefact.forecast_sprint4_add_dm_to_daily acts
      join ${viewName} isxi
        on acts.item_id = isxi.item_id
        and acts.sub_id = isxi.sub_id
        and acts.store_code = isxi.store_code
        where acts.date_Key >='${startDateStr}' and acts.date_Key <='${endDateStr}'
    """
  }

  def getSimulationActualStockLevelSql(stockDateStr: String, viewName: String): String = {
    s"""
		SELECT icis.item_id,
			icis.sub_id,
			icis.store_code AS entity_code,
			CASE
				WHEN fss.day_end_stock_with_actual IS NULL
					THEN cast(icis.balance_qty AS DOUBLE)
				ELSE fss.day_end_stock_with_actual
				END AS stock_level
		FROM vartefact.forecast_item_code_id_stock icis
		LEFT OUTER JOIN vartefact.forecast_simulation_stock fss
		ON icis.item_id = fss.item_id
			AND icis.sub_id = fss.sub_id
			AND icis.store_code = fss.store_code
			AND fss.date_key = '${stockDateStr}'
		JOIN ${viewName} itmd ON icis.item_id = itmd.item_id
			AND icis.sub_id = itmd.sub_id
			AND icis.store_code = itmd.store_code
		WHERE icis.date_key = '${stockDateStr}'
    """
  }

  def getSimulationOnTheWayStockSql(startDateStr: String, endDateStr: String, viewName: String,
                                    orderTableName: String, isDcFlow: Boolean): String = {

    if (isDcFlow) {
      s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.store_code as entity_code,
        ord.delivery_day,
        cast(ord.order_qty as double) order_qty
    FROM ${orderTableName} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.store_code = itmd.store_code
    WHERE ord.flow_type = 'DC'
        AND ord.delivery_day >= '${startDateStr}'
        AND ord.delivery_day <= '${endDateStr}'
        AND ord.order_day < '${startDateStr}'
    """

    } else {
      s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.store_code as entity_code,
        ord.delivery_day,
        cast(ord.order_qty as double) order_qty
    FROM ${orderTableName} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.store_code = itmd.store_code
    WHERE ord.flow_type != 'DC'
        AND ord.delivery_day >= '${startDateStr}'
        AND ord.delivery_day <= '${endDateStr}'
        AND ord.order_day < '${startDateStr}'
    """

    }

  }


  def getSimulationDcActualStockLevelSql(stockDateStr: String, viewName: String): String = {
    s"""
		SELECT ldd.item_id,
			ldd.sub_id,
			ldd.holding_code AS entity_code,
			CASE
				WHEN fss.day_end_stock_with_actual IS NULL
					THEN cast(ldd.stock_available_sku AS DOUBLE)
				ELSE cast(fss.day_end_stock_with_actual AS DOUBLE)
				END AS stock_level
		FROM vartefact.forecast_lfms_daily_dcstock ldd
		LEFT OUTER JOIN vartefact.forecast_simulation_stock fss
		ON ldd.item_id = fss.item_id
			AND ldd.sub_id = fss.sub_id
			AND ldd.holding_code = fss.store_code
			AND fss.date_key = '${stockDateStr}'
      AND fss.flow_type='DC'
		JOIN ${viewName} itmd ON ldd.item_id = itmd.item_id
			AND ldd.sub_id = itmd.sub_id
			AND ldd.holding_code = itmd.con_holding
		WHERE ldd.date_key = '${stockDateStr}'
      AND ldd.dc_site='DC1'
      AND ldd.warehouse_code='KS01'
    """
  }

  def getSimulationStoreOrderToDcSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
      SELECT t.item_id,
        t.sub_id,
        t.entity_code,
        t.date_key,
        cast(sum(t.order_qty) AS DOUBLE) AS daily_sales_prediction
      FROM (
        SELECT fcst.item_id,
          fcst.sub_id,
          fcst.con_holding as entity_code,
          fcst.order_day as date_key,
          fcst.order_qty
        FROM ${SimulationTables.simulationOrdersHistTable} fcst
        JOIN ${viewName} itmd ON fcst.item_id = itmd.item_id
          AND fcst.sub_id = itmd.sub_id
        WHERE fcst.flow_type = 'OnStock'
          AND fcst.order_day >= '${startDateStr}'
          AND fcst.order_day <= '${endDateStr}'
          AND fcst.run_date = '${startDateStr}'

        UNION

        SELECT fdo.item_id,
          fdo.sub_id,
          itmd.con_holding AS entity_code,
          fdo.first_order_date AS date_key,
          fdo.dm_order_qty_with_pcb AS order_qty
        FROM vartefact.forecast_dm_orders fdo
        JOIN ${viewName} itmd ON fdo.item_id = itmd.item_id
          AND fdo.sub_id = itmd.sub_id
        WHERE fdo.first_order_date >= '${startDateStr}'
          AND fdo.first_order_date <= '${endDateStr}'
          AND fdo.rotation != 'X'
        ) t
      GROUP BY t.item_id,
        t.sub_id,
        t.entity_code,
        t.date_key
    """
  }

  def getSimulationDcActualSales(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
      SELECT t.item_id,
        t.sub_id,
        t.entity_code,
        t.date_key,
        cast(sum(t.order_qty) AS DOUBLE) AS daily_sales_sum
      FROM (
        SELECT fcst.item_id,
          fcst.sub_id,
          fcst.con_holding as entity_code,
          fcst.order_day as date_key,
          fcst.order_qty
        FROM ${SimulationTables.simulationOrdersTable} fcst
        JOIN ${viewName} itmd ON fcst.item_id = itmd.item_id
          AND fcst.sub_id = itmd.sub_id
        WHERE fcst.flow_type = 'OnStock'
          AND fcst.order_day >= '${startDateStr}'
          AND fcst.order_day <= '${endDateStr}'

        UNION

        SELECT fdo.item_id,
          fdo.sub_id,
          itmd.con_holding AS entity_code,
          fdo.first_order_date AS date_key,
          fdo.dm_order_qty_with_pcb AS order_qty
        FROM vartefact.forecast_dm_orders fdo
        JOIN ${viewName} itmd ON fdo.item_id = itmd.item_id
          AND fdo.sub_id = itmd.sub_id
        WHERE fdo.first_order_date >= '${startDateStr}'
          AND fdo.first_order_date <= '${endDateStr}'
          AND fdo.rotation != 'X'
        ) t
      GROUP BY t.item_id,
        t.sub_id,
        t.entity_code,
        t.date_key
    """
  }

  def getSimulationDcPastOrdersSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.con_holding as entity_code,
        ord.order_day,
        cast(ord.order_qty as double) order_qty
    FROM ${SimulationTables.simulationOrdersTable} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.con_holding = itmd.con_holding
    WHERE  ord.flow_type ='DC'
        AND ord.order_day >= '${startDateStr}'
        AND ord.order_day <= '${endDateStr}'
    """
  }

}
