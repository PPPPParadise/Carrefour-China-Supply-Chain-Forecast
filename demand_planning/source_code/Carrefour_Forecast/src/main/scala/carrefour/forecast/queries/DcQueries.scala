package carrefour.forecast.queries

/**
  * SQL queries for DC order
  */
object DcQueries {

  /**
    * SQL query to get in scope items for this job run for DC flow
    * 查询货仓订单流程中应包括的商品的SQL
    *
    * @param orderDateStr Order date in yyyyMMdd String format 文本格式的订单日期，为yyyyMMdd格式
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @return SQL with variables filled 拼装好的SQL
    */
  def getOnStockDcItemsSql(orderDateStr: String, stockDateStr: String): String = {
    s"""
      SELECT DISTINCT icis.item_id,
        icis.sub_id,
        fdls.dept_code,
        fdls.item_code,
        fdls.sub_code,
        fdls.con_holding,
        fdls.con_holding AS entity_code,
        fdls.con_holding AS store_code,
        fdls.flow_type,
        fdls.rotation,
        cast(fdls.pcb AS DOUBLE) pcb,
        fdls.ds_supplier_code AS supplier_code,
        dodm.order_date AS run_date,
        'AfterStoreOpen' AS delivery_time,
        fdls.avg_sales_qty AS average_sales
      FROM vartefact.forecast_dc_latest_sales fdls
      JOIN vartefact.forecast_dc_order_deliver_mapping dodm ON dodm.con_holding = fdls.con_holding
        AND dodm.order_date = '${orderDateStr}'
      JOIN vartefact.forecast_item_code_id_stock icis ON icis.date_key = '${stockDateStr}'
        AND fdls.item_code = icis.item_code
        AND fdls.sub_code = icis.sub_code
        AND fdls.dept_code = icis.dept_code
      WHERE fdls.date_key = '${orderDateStr}'
  """
  }

  /**
    * SQL query to get all order days for DC job run
    * DC/货仓脚本查询周期中包括的全部订单日的SQL
    *
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getOnStockDcInScopeOrderDaysSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT
        itmd.item_id,
        itmd.sub_id,
        itmd.dept_code,
        itmd.item_code,
        itmd.sub_code,
        itmd.con_holding,
        itmd.entity_code,
        itmd.store_code,
        itmd.supplier_code,
        itmd.rotation,
        itmd.pcb,
        itmd.delivery_time,
        ord.date_key,
        ord.date_key AS order_date,
        dev.date_key AS delivery_date,
        ord.weekday_short as order_weekday,
        dev.weekday_short as delivery_weekday,
        "" as item_stop_start_date,
        "" as item_stop_end_date,
        cast(itmd.average_sales as double) average_sales
    from ${viewName} itmd
    join vartefact.forecast_dc_order_deliver_mapping dodm
        on itmd.con_holding = dodm.con_holding
    join vartefact.forecast_calendar ord
        on ord.date_key = dodm.order_date
    join vartefact.forecast_calendar dev
        on dev.weekday_short = dodm.delivery_weekday and dev.week_index = ord.week_index + dodm.week_shift
    where ord.date_key>='${startDateStr}' and dev.date_key <='${endDateStr}'
    """
  }

  /**
    * SQL query to get future store orders to DC
    * 查询门店向DC/货仓未来订货量的SQL
    *
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getStoreOrderToDcSql(startDateStr: String, endDateStr: String, viewName: String): String = {
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
        FROM vartefact.forecast_onstock_orders fcst
        JOIN ${viewName} itmd ON fcst.item_id = itmd.item_id
          AND fcst.sub_id = itmd.sub_id
        WHERE fcst.order_day >= '${startDateStr}'
          AND fcst.order_day <= '${endDateStr}'

        UNION

        SELECT fdo.item_id,
          fdo.sub_id,
          itmd.con_holding AS entity_code,
          fdo.first_order_date AS date_key,
          fdo.order_qty AS order_qty
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

  /**
    * SQL query to get current stock level for DC
    * 查询DC/货仓当前库存的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getDcActualStockLevelSql(stockDateStr: String, viewName: String): String = {
    s"""
    SELECT ldd.item_id,
     ldd.sub_id,
     itmd.con_holding as entity_code,
     cast(ldd.stock_available_sku AS DOUBLE) as stock_level
    FROM vartefact.forecast_lfms_daily_dcstock ldd
    join ${viewName} itmd
       on ldd.item_id = itmd.item_id
       and ldd.sub_id = itmd.sub_id
    WHERE
      ldd.date_key='${stockDateStr}'
      and ldd.dc_site='DC1'
      and ldd.warehouse_code='KS01'
    """
  }

  /**
    * SQL query to get past generated orders for DC
    * 查询过去生成的DC订单规划的SQL
    *
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param orderTableName
    * @return SQL with variables filled 拼装好的SQL
    */
  def getDcPastOrdersSql(startDateStr: String, endDateStr: String, viewName: String, orderTableName: String): String = {
    s"""
    SELECT ord.item_id,
        ord.sub_id,
        ord.con_holding as entity_code,
        ord.order_day,
        cast(ord.order_qty as double) order_qty
    FROM ${orderTableName} ord
    join ${viewName} itmd
        on ord.item_id = itmd.item_id
        and ord.sub_id = itmd.sub_id
        and ord.con_holding = itmd.con_holding
    WHERE ord.order_day >= '${startDateStr}'
        AND ord.order_day <= '${endDateStr}'
    """
  }

}
