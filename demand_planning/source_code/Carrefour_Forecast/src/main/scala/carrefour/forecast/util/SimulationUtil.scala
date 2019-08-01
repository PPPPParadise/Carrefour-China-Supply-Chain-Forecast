package carrefour.forecast.util

import carrefour.forecast.config.SimulationTables
import carrefour.forecast.model.{DateRow, ItemEntity, ModelRun}
import carrefour.forecast.queries.SimulationQueries
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}

object SimulationUtil {

  def insertSimulationResultToDatalake(resDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Unit = {
    val simulationDf = resDf.filter("error_info == ''")

    val simulationView = modelRun.flowType + "_simulation_df"

    simulationDf.createOrReplaceTempView(simulationView)

    val simulationSql =
      s"""
       insert overwrite table ${SimulationTables.simulationResultTable}
         partition(run_date, flow_type)
         select date_key, item_id, sub_id, dept_code, item_code, sub_code,
         con_holding, store_code, supplier_code, rotation,
         order_day, delivery_day, order_qty, order_without_pcb,
         is_order_day, matched_sales_start_date, matched_sales_end_date,
         start_stock, future_stock, minimum_stock_required, dm_delivery, order_delivery,
         predict_sales, day_end_stock_with_predict, actual_sales, day_end_stock_with_actual,
         ittreplentyp, shelf_capacity, ittminunit,
         '${modelRun.runDateStr}' as run_date, '${modelRun.flowType}' as flow_type
         from ${simulationView}
        """

    sqlc.sql(simulationSql)

    sqlc.sql(s"refresh ${SimulationTables.simulationResultTable} ")
  }


  def insertSimulationStockToDatalake(resDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Unit = {
    val simulationStockDf = resDf.filter("error_info == ''")
    val simulationStockView = modelRun.flowType + "_simulation_stock_df"

    simulationStockDf.createOrReplaceTempView(simulationStockView)

    val stockSql =
      s"""
        insert overwrite table ${SimulationTables.simulationStockTable}
          partition(date_key, flow_type)
          SELECT
          item_id,
          sub_id,
          store_code,
          rotation,
          dept_code,
          day_end_stock_with_actual,
          date_key,
          '${modelRun.flowType}' as flow_type
          FROM ${simulationStockView}
      """

    sqlc.sql(stockSql)

    sqlc.sql(s"refresh ${SimulationTables.simulationStockTable}")
  }


  def insertSimulationOrderToDatalake(orderDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Unit = {

    val finalOrderDf = orderDf.filter("error_info == ''")

    val orderDfView = modelRun.flowType + "_result_df"

    finalOrderDf.createOrReplaceTempView(orderDfView)

    val order_sql =
      s"""
        insert overwrite table ${SimulationTables.simulationOrdersTable}
        partition(order_day, flow_type)
         select item_id, sub_id, dept_code, item_code, sub_code,
         con_holding, store_code, supplier_code, rotation,
         delivery_day, order_qty, minimum_stock_required, order_without_pcb,
         order_day, '${modelRun.flowType}' as flow_type
         from ${orderDfView}
        """

    sqlc.sql(order_sql)

    sqlc.sql(s"refresh ${SimulationTables.simulationOrdersTable} ")

    val order_hist_sql =
      s"""
        insert overwrite table ${SimulationTables.simulationOrdersHistTable}
        partition(run_date, flow_type)
         select item_id, sub_id, dept_code, item_code, sub_code,
         con_holding, store_code, supplier_code, rotation,
         order_day, delivery_day, minimum_stock_required, order_qty, order_without_pcb,
         ${modelRun.runDateStr}, '${modelRun.flowType}' as flow_type
         from ${orderDfView}
        """

    sqlc.sql(order_hist_sql)

    sqlc.sql(s"refresh ${SimulationTables.simulationOrdersHistTable} ")
  }

  def insertSimulationItemStatusToDatalake(resDf: DataFrame, modelRun: ModelRun, sqlc: SQLContext): Unit = {
    val simulationItemStatuskView = modelRun.flowType + "_simulation_item_status_df"

    resDf.createOrReplaceTempView(simulationItemStatuskView)

    val stockSql =
      s"""
        insert overwrite table ${SimulationTables.simulationItemStatusTable}
          partition(delivery_date, flow_type)
          SELECT
          item_id,
          sub_id,
          store_code,
          rotation,
          dept_code,
          item_stop_start_date,
          item_stop_end_date,
          order_date,
          shelf_capacity,
          ittreplentyp,
          ittminunit,
          delivery_date,
          '${modelRun.flowType}' as flow_type
          FROM ${simulationItemStatuskView}
      """

    sqlc.sql(stockSql)

    sqlc.sql(s"refresh ${SimulationTables.simulationItemStatusTable}")
  }


  def getSimulationOnTheWayStockMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                                    spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val onTheWayStockSql = SimulationQueries.getSimulationOnTheWayStockSql(startDateStr, endDateStr,
      viewName, SimulationTables.simulationOrdersTable)

    val onTheWayStockDf = spark.sqlContext.sql(onTheWayStockSql)

    val grouppedDf = onTheWayStockDf.groupByKey(row => ItemEntity(row.getAs[Integer]("item_id"),
      row.getAs[Integer]("sub_id"),
      row.getAs[String]("entity_code"),
      isDcFlow))

    val onTheWayStockMap = grouppedDf.mapGroups((itemEntity, rows) => {
      var onTheWayStockList: List[Tuple2[String, Double]] = List()
      for (row <- rows) {
        onTheWayStockList = (row.getAs[String]("delivery_day"),
          row.getAs[Double]("order_qty")) :: onTheWayStockList
      }
      itemEntity -> onTheWayStockList
    }

    ).collect().toMap

    return onTheWayStockMap
  }

  def getSimulationStoreOrderToDc(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                                  sqlc: SQLContext): DataFrame = {

    // Get sales prediction
    val storeOrderToDcSql = SimulationQueries.getSimulationStoreOrderToDcSql(startDateStr, endDateStr, viewName)
    var storeOrderToDcDf = sqlc.sql(storeOrderToDcSql)

    storeOrderToDcDf = dateMapDf.join(storeOrderToDcDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    storeOrderToDcDf = storeOrderToDcDf.withColumn("predict_sales",
      when(col("daily_sales_prediction").isNull, lit(0.0)).otherwise(col("daily_sales_prediction")))
    storeOrderToDcDf = storeOrderToDcDf.drop("daily_sales_prediction")

    return storeOrderToDcDf
  }

  def getActualSales(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                     sqlc: SQLContext): DataFrame = {

    // Get actual sales
    val actualSalesSql = SimulationQueries.getActualSalesSql(startDateStr, endDateStr, viewName)
    var actualSalesDf = sqlc.sql(actualSalesSql)

    actualSalesDf = dateMapDf.join(actualSalesDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    actualSalesDf = actualSalesDf.withColumn("actual_sales",
      when(col("daily_sales_sum").isNull, lit(0.0)).otherwise(col("daily_sales_sum")))
    actualSalesDf = actualSalesDf.drop("daily_sales_sum")

    return actualSalesDf
  }

  def getDCActualSales(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                       sqlc: SQLContext): DataFrame = {

    // Get actual sales
    val actualSalesSql = SimulationQueries.getDcActualSalesSql(startDateStr, endDateStr, viewName)
    var actualSalesDf = sqlc.sql(actualSalesSql)

    actualSalesDf = dateMapDf.join(actualSalesDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    actualSalesDf = actualSalesDf.withColumn("actual_sales",
      when(col("daily_sales_sum").isNull, lit(0.0)).otherwise(col("daily_sales_sum")))
    actualSalesDf = actualSalesDf.drop("daily_sales_sum")

    return actualSalesDf
  }
}
