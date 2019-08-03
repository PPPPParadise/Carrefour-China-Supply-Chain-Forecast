package carrefour.forecast.queries

object DcQueries {

  def getOnStockDcItemsSql(startDateStr: String, stockDateStr: String): String = {
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
        AND dodm.order_date = '${startDateStr}'
      JOIN vartefact.forecast_item_code_id_stock icis ON icis.date_key = '${stockDateStr}'
        AND fdls.item_code = icis.item_code
        AND fdls.sub_code = icis.sub_code
        AND fdls.dept_code = icis.dept_code
        AND fdls.date_key = icis.date_key
      WHERE fdls.date_key = '${stockDateStr}'
  """
  }

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

  def getDcActualStockLevelSql(stockDateStr: String, viewName: String): String = {
    s"""
    SELECT ldd.item_id,
     ldd.sub_id,
     ldd.holding_code as entity_code,
     cast(ldd.stock_available_sku AS DOUBLE) as stock_level
    FROM vartefact.forecast_lfms_daily_dcstock ldd
    join ${viewName} itmd
       on ldd.item_id = itmd.item_id
       and ldd.sub_id = itmd.sub_id
       and ldd.holding_code = itmd.con_holding
    WHERE
      ldd.date_key='${stockDateStr}'
      and ldd.dc_site='DC1'
      and ldd.warehouse_code='KS01'
    """
  }

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
