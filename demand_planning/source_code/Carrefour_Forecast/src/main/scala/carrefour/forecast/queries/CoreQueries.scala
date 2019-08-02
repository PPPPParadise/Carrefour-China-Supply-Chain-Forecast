package carrefour.forecast.queries

object CoreQueries {

  def getOnStockStoreInScopeItemsSql(startDateStr: String, stockDateStr: String): String = {
    s"""
   SELECT distinct
          icis.item_id,
          icis.sub_id,
          icis.dept_code,
          icis.item_code,
          icis.sub_code,
          id.con_holding,
          mp.store_code,
          mp.store_code as entity_code,
          id.flow_type,
          id.rotation,
          cast(id.pcb as double) pcb,
          id.dc_supplier_code as supplier_code,
          ord.date_key AS run_date,
          dsdt.delivery_time,
          fsd.dept
      from vartefact.forecast_item_details id
      join vartefact.forecast_stores_dept fsd
          on fsd.dept_code = id.dept_code
      join vartefact.onstock_order_delivery_mapping mp
          on mp.dept_code = fsd.dept_code and id.rotation = mp.`class`
          and mp.store_code = fsd.store_code
      join vartefact.forecast_stores_delv_time  dsdt
          on fsd.store_code = dsdt.store_code
      join vartefact.forecast_calendar ord
          on ord.date_key = '${startDateStr}'
          and ord.weekday_short = mp.order_weekday
      join vartefact.forecast_item_code_id_stock icis
          on icis.date_key = '${stockDateStr}'
          and id.item_code = icis.item_code
          and id.sub_code = icis.sub_code
          and id.dept_code = icis.dept_code
          and fsd.store_code = icis.store_code
  """
  }

  def getOnStockStoreInScopeOrderDaysSql(stockDateStr: String,
                                         startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT
      itmd.item_id,
      itmd.sub_id,
      itmd.dept_code,
      itmd.item_code,
      itmd.sub_code,
      itmd.con_holding,
      itmd.store_code,
      itmd.entity_code,
      itmd.supplier_code,
      itmd.rotation,
      itmd.pcb,
      itmd.delivery_time,
      ord.date_key,
      ord.date_key AS order_date,
      dev.date_key AS delivery_date,
      ord.weekday_short as order_weekday,
      dev.weekday_short as delivery_weekday,
      trim(coalesce(stp.item_stop_start_date, '')) as item_stop_start_date,
      trim(coalesce(stp.item_stop_end_date, '')) as item_stop_end_date,
      stp.shelf_capacity,
      opi.ittreplentyp,
      opi.ittminunit
  from ${viewName} itmd
  join vartefact.onstock_order_delivery_mapping mp
      on mp.dept_code = itmd.dept_code
      and itmd.rotation = mp.`class`
      and mp.store_code = itmd.store_code
  join vartefact.forecast_calendar ord
      on ord.weekday_short = mp.order_weekday
  join  vartefact.forecast_calendar dev
      on dev.weekday_short = mp.delivery_weekday and dev.week_index = ord.week_index + mp.week_shift
  join ods.p4md_itmsto opi
      on cast(opi.ittitmid as INT) = itmd.item_id
      and opi.ittstocd = itmd.store_code
  join vartefact.forecast_p4cm_store_item stp
      on itmd.item_code = stp.item_code
      and itmd.sub_code = stp.sub_code
      and itmd.store_code = stp.store_code
      and itmd.dept_code = stp.dept_code
      and stp.date_key = '${stockDateStr}'
  where ord.date_key>='${startDateStr}' and dev.date_key <='${endDateStr}'
      """
  }

  def getXdockInScopeItemsSql(startDateStr: String, stockDateStr: String): String = {
    s"""
     SELECT distinct
        icis.item_id,
        icis.sub_id,
        id.dept_code,
        id.item_code,
        id.sub_code,
        id.con_holding,
        fsd.store_code,
        fsd.store_code as entity_code,
        id.flow_type,
        id.rotation,
        cast(id.pcb as double) pcb,
        id.ds_supplier_code as supplier_code,
        ord.date_key AS run_date,
        dsdt.delivery_time
    from vartefact.forecast_item_details id
    join vartefact.xdock_order_delivery_mapping xo
        on id.flow_type = 'Xdock'
        and xo.item_code = id.item_code
        and xo.sub_code = id.sub_code
        and xo.dept_code = id.dept_code
    join vartefact.forecast_stores_dept fsd
        on fsd.dept_code = id.dept_code
    join vartefact.forecast_stores_delv_time dsdt
        on fsd.store_code = dsdt.store_code
    join vartefact.forecast_calendar ord
        on ord.date_key = '${startDateStr}'
        and ord.iso_weekday = xo.order_weekday
    join vartefact.forecast_item_code_id_stock icis
        on icis.date_key = '${stockDateStr}'
        and id.item_code = icis.item_code
        and id.sub_code = icis.sub_code
        and id.dept_code = icis.dept_code
        and fsd.store_code = icis.store_code
    """
  }

  def getXDockingInScopeOrderDaysSql(stockDateStr: String,
                                     startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT
        itmd.item_id,
        itmd.sub_id,
        itmd.dept_code,
        itmd.item_code,
        itmd.sub_code,
        itmd.con_holding,
        itmd.store_code,
        itmd.entity_code,
        itmd.supplier_code,
        itmd.rotation,
        itmd.pcb,
        itmd.delivery_time,
        ord.date_key,
        ord.date_key AS order_date,
        dev.date_key AS delivery_date,
        ord.weekday_short as order_weekday,
        dev.weekday_short as delivery_weekday,
        trim(coalesce(stp.item_stop_start_date, '')) as item_stop_start_date,
        trim(coalesce(stp.item_stop_end_date, '')) as item_stop_end_date,
        stp.shelf_capacity,
        opi.ittreplentyp,
        opi.ittminunit
    from ${viewName} itmd
    join vartefact.xdock_order_delivery_mapping xo
        on itmd.item_code = xo.item_code
        and itmd.sub_code = xo.sub_code
        and itmd.dept_code = xo.dept_code
    join vartefact.forecast_calendar ord
        on ord.iso_weekday = xo.order_weekday
    join vartefact.forecast_calendar dev
        on dev.iso_weekday = xo.delivery_weekday and dev.week_index = ord.week_index + xo.week_shift
    join ods.p4md_itmsto opi
        on cast(opi.ittitmid as INT) = itmd.item_id
        and opi.ittstocd = itmd.store_code
    join vartefact.forecast_p4cm_store_item stp
        on itmd.item_code = stp.item_code
        and itmd.sub_code = stp.sub_code
        and itmd.dept_code = stp.dept_code
        and itmd.store_code = stp.store_code
        and stp.date_key = '${stockDateStr}'
    where ord.date_key>='${startDateStr}' and dev.date_key <='${endDateStr}'
    """
  }


  def getActualStockLevelSql(stockDateStr: String, viewName: String): String = {
    s"""
    SELECT icis.item_id,
     icis.sub_id,
     icis.store_code as entity_code,
     cast(icis.balance_qty AS DOUBLE) as stock_level
    FROM vartefact.forecast_item_code_id_stock icis
    join ${viewName} itmd
       on icis.item_id = itmd.item_id
       and icis.sub_id = itmd.sub_id
       and icis.store_code = itmd.store_code
    WHERE
      icis.date_key='${stockDateStr}'
    """
  }

  def getCalendarSql(startDateStr: String, endDateStr: String): String = {
    s"select date_key from ods.dim_calendar where date_key >='${startDateStr}' and date_key <='${endDateStr}'"
  }

  def getSalesPredictionSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT fcst.item_id,
        fcst.sub_id,
        fcst.store_code as entity_code,
        fcst.date_key,
        fcst.daily_sales_prediction
    FROM vartefact.v_forecast_daily_sales_prediction fcst
    join ${viewName} itmd
        on fcst.item_id = itmd.item_id
        and fcst.sub_id = itmd.sub_id
        and fcst.store_code = itmd.store_code
    WHERE fcst.date_key >= '${startDateStr}'
        AND fcst.date_key <= '${endDateStr}'
    """
  }

  def getDmOrdersSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT dm.item_id,
        dm.sub_id,
        dm.store_code as entity_code,
        dm.first_delivery_date,
        sum(dm.dm_order_qty_with_pcb) as dm_order_qty
    FROM vartefact.forecast_dm_orders dm
    join ${viewName} itmd
        on dm.item_id = itmd.item_id
        and dm.sub_id = itmd.sub_id
        and dm.store_code = itmd.store_code
    WHERE dm.first_delivery_date >= '${startDateStr}'
        AND dm.first_delivery_date <= '${endDateStr}'
    GROUP BY dm.item_id,
       dm.sub_id,
       dm.store_code,
       dm.first_delivery_date
    """
  }

  def getOnTheWayStockSql(startDateStr: String, endDateStr: String, viewName: String, orderTableName: String): String = {
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
    WHERE ord.delivery_day >= '${startDateStr}'
        AND ord.delivery_day <= '${endDateStr}'
        AND ord.order_day < '${startDateStr}'
    """
  }

  def getOnStockDcItemsSql(startDateStr: String, stockDateStr: String): String = {
    s"""
      SELECT DISTINCT icis.item_id,
        icis.sub_id,
        fdls.dept_code,
        fdls.item_code,
        fdls.sub_code,
        fdls.con_holding,
        fdls.con_holding AS entity_code,
        'DC_FLOW' AS store_code,
        fdls.flow_type,
        fdls.rotation,
        cast(fdls.pcb AS DOUBLE) pcb,
        fdls.dc_supplier_code AS supplier_code,
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

  def getOnStockDcInScopeOrderDaysSql(defaultMinimumStock: Double,
                                      startDateStr: String, endDateStr: String, viewName: String): String = {
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
    FROM lfms.daily_dcstock ldd
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
