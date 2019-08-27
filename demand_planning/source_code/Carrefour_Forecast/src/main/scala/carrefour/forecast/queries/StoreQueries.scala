package carrefour.forecast.queries

object StoreQueries {

  /**
    * SQL query to get in scope items for this job run for on stock items store flow
    * 查询on stock商品门店订单流程中应包括的商品的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @return SQL with variables filled 拼装好的SQL
    */
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
          id.rotation,
          cast(id.qty_per_unit as double) as pcb,
          id.dc_supplier_code as supplier_code,
          ord.date_key AS run_date,
          dsdt.delivery_time,
          id.repl_type,
          id.min_stock
      from vartefact.v_forecast_inscope_store_item_details id
      join vartefact.forecast_onstock_order_delivery_mapping mp
          on id.dept_code = mp.dept_code
          and id.rotation = mp.rotation
          and id.store_code = mp.store_code
      join vartefact.forecast_stores_delv_time  dsdt
          on id.store_code = dsdt.store_code
      join vartefact.forecast_calendar ord
          on ord.date_key = '${startDateStr}'
          and ord.iso_weekday = mp.order_iso_weekday
      join vartefact.forecast_item_code_id_stock icis
          on icis.date_key = '${stockDateStr}'
          and id.item_code = icis.item_code
          and id.sub_code = icis.sub_code
          and id.dept_code = icis.dept_code
          and id.store_code = icis.store_code
  """
  }

  /**
    * SQL query to get in scope items for on stock items store flow
    * 查询on stock商品门店订单脚本中应包括的商品的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
        cast(itmd.repl_type as INT) as ittreplentyp,
        cast(itmd.min_stock as INT) as ittminunit
    from ${viewName} itmd
    join vartefact.forecast_onstock_order_delivery_mapping mp
        on mp.dept_code = itmd.dept_code
        and mp.rotation = itmd.rotation
        and mp.store_code = itmd.store_code
    join vartefact.forecast_calendar ord
        on ord.iso_weekday = mp.order_iso_weekday
    join  vartefact.forecast_calendar dev
        on dev.iso_weekday = mp.delivery_iso_weekday and dev.week_index = ord.week_index + mp.week_shift
    join vartefact.forecast_p4cm_store_item stp
        on itmd.item_code = stp.item_code
        and itmd.sub_code = stp.sub_code
        and itmd.store_code = stp.store_code
        and itmd.dept_code = stp.dept_code
        and stp.date_key = '${startDateStr}'
    where ord.date_key>='${startDateStr}' and dev.date_key <='${endDateStr}'
      """
  }

  /**
    * SQL query to get in scope items for this job run for cross docking flow
    * 查询cross docking商品订单流程中应包括的商品的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @return SQL with variables filled 拼装好的SQL
    */
  def getXdockInScopeItemsSql(startDateStr: String, stockDateStr: String): String = {
    s"""
     SELECT distinct
        icis.item_id,
        icis.sub_id,
        id.dept_code,
        id.item_code,
        id.sub_code,
        id.con_holding,
        id.risk_item_unilever,
        id.store_code,
        id.store_code as entity_code,
        id.rotation,
        cast(id.qty_per_unit as double) as pcb,
        id.dc_supplier_code as supplier_code,
        ord.date_key AS run_date,
        dsdt.delivery_time,
        id.repl_type,
        id.min_stock
    from vartefact.v_forecast_inscope_store_item_details id
    join vartefact.forecast_xdock_order_mapping xo
        on xo.item_code = id.item_code
        and xo.sub_code = id.sub_code
        and xo.dept_code = id.dept_code
        and xo.store_code = id.store_code
    join vartefact.forecast_stores_delv_time dsdt
        on id.store_code = dsdt.store_code
    join vartefact.forecast_calendar ord
        on ord.date_key = '${startDateStr}'
        and ord.iso_weekday = xo.order_iso_weekday
    join vartefact.forecast_item_code_id_stock icis
        on icis.date_key = '${stockDateStr}'
        and id.item_code = icis.item_code
        and id.sub_code = icis.sub_code
        and id.dept_code = icis.dept_code
        and id.store_code = icis.store_code
    """
  }

  /**
    * SQL query to get in scope items for cross docking flow
    * 查询cross docking商品订单脚本中应包括的商品的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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
        date_format(
            date_add(
                to_timestamp(dodm.delivery_date, 'yyyyMMdd'), xo.dc_to_store_time
                ),
            'yyyyMMdd'
        ) AS delivery_date,
        ord.weekday_short as order_weekday,
        "" as delivery_weekday,
        trim(coalesce(stp.item_stop_start_date, '')) as item_stop_start_date,
        trim(coalesce(stp.item_stop_end_date, '')) as item_stop_end_date,
        stp.shelf_capacity,
        cast(itmd.repl_type as INT) as ittreplentyp,
        cast(itmd.min_stock as INT) as ittminunit
    from ${viewName} itmd
    join vartefact.forecast_xdock_order_mapping xo
        on itmd.item_code = xo.item_code
        and itmd.sub_code = xo.sub_code
        and itmd.dept_code = xo.dept_code
        and itmd.store_code = xo.store_code
    join vartefact.forecast_calendar ord
        on ord.iso_weekday = xo.order_iso_weekday
    join vartefact.forecast_dc_order_delivery_mapping dodm
        on dodm.con_holding = itmd.con_holding
        and dodm.order_date = ord.date_key
        and dodm.risk_item_unilever = itmd.risk_item_unilever
    join vartefact.forecast_p4cm_store_item stp
        on itmd.item_code = stp.item_code
        and itmd.sub_code = stp.sub_code
        and itmd.dept_code = stp.dept_code
        and itmd.store_code = stp.store_code
        and stp.date_key = '${startDateStr}'
    where ord.date_key>='${startDateStr}'
        and date_add(to_timestamp(dodm.delivery_date, 'yyyyMMdd'), xo.dc_to_store_time)
         <= to_timestamp('${endDateStr}', 'yyyyMMdd')
    """
  }


  /**
    * SQL query to get current stock level for store
    * 查询门店当前库存的SQL
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
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

  /**
    * SQL query to get all dates in current job
    * 查询当前脚本对应的全部日期的SQL
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @return SQL with variables filled 拼装好的SQL
    */
  def getCalendarSql(startDateStr: String, endDateStr: String): String = {
    s"select date_key from vartefact.forecast_calendar where date_key >='${startDateStr}' and date_key <='${endDateStr}'"
  }

  /**
    * SQL query to get sales predictions
    * 查询销量预测的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getSalesPredictionSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT fcst.item_id,
        fcst.sub_id,
        fcst.store_code as entity_code,
        fcst.date_key,
        sum (
        case when
          fcst.daily_sales_prediction < 0.2 and itmd.rotation != 'A'
        then 0
        else fcst.daily_sales_prediction
        end ) as max_predict_sales,
        sum (
        case when
          fcst.daily_sales_prediction_original < 0.2 and itmd.rotation != 'A'
        then 0
        else fcst.daily_sales_prediction_original
        end ) as daily_sales_prediction
    FROM vartefact.t_forecast_daily_sales_prediction fcst
    join ${viewName} itmd
        on fcst.item_id = itmd.item_id
        and fcst.sub_id = itmd.sub_id
        and fcst.store_code = itmd.store_code
    WHERE fcst.date_key >= '${startDateStr}'
        AND fcst.date_key <= '${endDateStr}'
    GROUP BY fcst.item_id,
        fcst.sub_id,
        fcst.store_code,
        fcst.date_key
    """
  }

  /**
    * SQL query to get orders from DM process
    * 查询DM订单系统生成的DM订单的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @return SQL with variables filled 拼装好的SQL
    */
  def getDmOrdersSql(startDateStr: String, endDateStr: String, viewName: String): String = {
    s"""
    SELECT dm.item_id,
        dm.sub_id,
        dm.store_code as entity_code,
        dm.first_delivery_date,
        cast(sum(dm.first_dm_order_qty) as DOUBLE) as dm_order_qty
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

  /**
    * SQL query to get on the way order quantity and delivery date
    * 查询在途订单订货量及其抵达日期的SQL
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param orderTableName Database and name for order table 订单表的数据库名及表名
    * @return SQL with variables filled 拼装好的SQL
    */
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
        AND ord.order_qty > 0
    """
  }
}
