package carrefour.forecast.queries

import carrefour.forecast.config.SimulationTables
import carrefour.forecast.model.EnumFlowType.FlowType

object SimulationQueries {

  /**
    * SQL query to get actual sales
    * 查询真实历史销量的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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

  /**
    * SQL query to get store stock level generated by simulation process
    * 查询模拟运行计算的门店库存水平的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
		LEFT OUTER JOIN ${SimulationTables.simulationStockTable} fss
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

  /**
    * SQL query to get on the way order quantity and delivery date from simulation process
    * 查询模拟运行生成的在途订单订货量及其抵达日期的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @return SQL with variables filled 拼装好的SQL
    */
  def getSimulationOnTheWayStockSql(startDateStr: String, endDateStr: String, viewName: String,
                                    isDcFlow: Boolean): String = {

    if (isDcFlow) {
      s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.store_code as entity_code,
        ord.delivery_day,
        cast(ord.order_qty as double) order_qty
    FROM ${SimulationTables.simulationOrdersTable} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.store_code = itmd.store_code
    WHERE ord.flow_type = '${FlowType.DC}'
        AND ord.delivery_day >= '${startDateStr}'
        AND ord.delivery_day <= '${endDateStr}'
        AND ord.order_day < '${startDateStr}'
        AND ord.order_qty > 0
    """

    } else {
      s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.store_code as entity_code,
        ord.delivery_day,
        cast(ord.order_qty as double) order_qty
    FROM ${SimulationTables.simulationOrdersTable} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.store_code = itmd.store_code
    WHERE ord.flow_type != '${FlowType.DC}'
        AND ord.delivery_day >= '${startDateStr}'
        AND ord.delivery_day <= '${endDateStr}'
        AND ord.order_day < '${startDateStr}'
        AND ord.order_qty > 0
    """

    }

  }

  /**
    * SQL query to get DC stock level generated by simulation process
    * 查询模拟运行计算的DC/货仓库存水平的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getSimulationDcActualStockLevelSql(startDateStr: String,
                                         stockDateStr: String, viewName: String): String = {
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
		LEFT OUTER JOIN ${SimulationTables.simulationStockTable} fss
		ON ldd.item_id = fss.item_id
			AND ldd.sub_id = fss.sub_id
			AND ldd.holding_code = fss.store_code
			AND fss.date_key = '${stockDateStr}'
      AND fss.flow_type = '${FlowType.DC}'
		JOIN ${viewName} itmd ON ldd.item_id = itmd.item_id
			AND ldd.sub_id = itmd.sub_id
			AND ldd.holding_code = itmd.con_holding
		WHERE ldd.date_key = '${startDateStr}'
      AND ldd.dc_site='DC1'
      AND ldd.warehouse_code='KS01'
    """
  }


  /**
    * SQL query to get future store orders to DC generated by simulation process
    * 查询模拟运行计算的门店向DC/货仓未来订货量的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
        WHERE fcst.flow_type = '${FlowType.OnStockStore}'
          AND fcst.order_day >= '${startDateStr}'
          AND fcst.order_day <= '${endDateStr}'
          AND fcst.run_date = '${startDateStr}'

        UNION

        SELECT fdo.item_id,
          fdo.sub_id,
          itmd.con_holding AS entity_code,
          fdo.first_order_date AS date_key,
          fdo.order_qty AS order_qty
        FROM ${SimulationTables.simulationDmOrdersTable} fdo
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

  /**
    * SQL query to get store actual orders to DC generated by simulation process
    * 查询模拟运行计算的门店向DC/货仓真实订货量的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
        WHERE fcst.flow_type = '${FlowType.OnStockStore}'
          AND fcst.order_day >= '${startDateStr}'
          AND fcst.order_day <= '${endDateStr}'

        UNION

        SELECT fdo.item_id,
          fdo.sub_id,
          itmd.con_holding AS entity_code,
          fdo.first_order_date AS date_key,
          fdo.order_qty AS order_qty
        FROM ${SimulationTables.simulationDmOrdersTable} fdo
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

  /**
    * SQL query to get past generated orders for DC generated by simulation process
    * 查询模拟运行计算的过去生成的DC订单规划的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
    WHERE  ord.flow_type = '${FlowType.DC}'
        AND ord.order_day >= '${startDateStr}'
        AND ord.order_day <= '${endDateStr}'
    """
  }

}
