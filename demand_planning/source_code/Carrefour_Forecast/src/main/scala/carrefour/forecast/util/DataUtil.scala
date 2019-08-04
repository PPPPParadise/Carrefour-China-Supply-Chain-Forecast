package carrefour.forecast.util

import carrefour.forecast.model.{DateRow, ModelRun}
import org.apache.spark.sql.{DataFrame, _}

/**
  * Utility used for processing dataframe
  */
object DataUtil {

  /**
    * Filter dataframe by using item ID, sub ID, and Entity ID (store code or con holding code).
    * One or more of these three conditions can be used
    * 从dataframe中筛选对应的记录。可以使用三种条件中的一种或多种。
    * 三种条件为item ID, sub ID, 和 Entity ID (store code 或者 con holding code).
    *
    * @param inputDf Dataframe
    * @param item_id
    * @param sub_id
    * @param entity_code
    * @return
    */
  def filterDateFrame(inputDf: DataFrame, item_id: Integer, sub_id: Integer, entity_code: String): DataFrame = {

    var newDf = inputDf

    if (item_id != 0) {
      newDf = newDf.filter(s"item_id=${item_id}")
      print(s"filter on item_id=${item_id} with ${newDf.count()} rows left \n")
    }

    if (sub_id != 0) {
      newDf = newDf.filter(s"sub_id=${sub_id}")
      print(s"filter on sub_id=${sub_id} with ${newDf.count()} rows left \n")
    }

    if (!entity_code.equals("")) {
      newDf = newDf.filter(f"entity_code='${entity_code}'")
      print(s"filter on entity_code='${entity_code}' with ${newDf.count()} rows left \n")
    }

    return newDf
  }

  /**
    * Insert debug result to datalake
    * 将debug运行结果写入datalake
    *
    * @param debugDf debug result. debug运行结果
    * @param debugTableName Database and table name for debug process 调试结果表的数据库名及表名
    * @param sqlc Spark SQLContext
    */
  def insertDebugInfoToDatalake(debugDf: Dataset[DateRow], debugTableName: String, sqlc: SQLContext): Unit = {

    debugDf.write.format("parquet").mode("overwrite").saveAsTable(debugTableName)

    sqlc.sql(s"refresh ${debugTableName}")

    LogUtil.info(s"Number of debug lines: ${debugDf.count()}")
  }


  /**
    * Insert generated store order to datalake
    * 将门店订单结果写入datalake
    *
    * @param orderDf
    * @param modelRun Job run information 脚本运行信息
    * @param sqlc Spark SQLContext
    */
  def insertOrderToDatalake(orderDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Unit = {

    val finalOrderDf = orderDf.filter("error_info == ''")

    val orderDfView = modelRun.flowType + "_result_df"

    finalOrderDf.createOrReplaceTempView(orderDfView)

    val order_sql =
      s"""
        insert overwrite table ${modelRun.orderTableName} partition(order_day)
         select item_id, sub_id, dept_code, item_code, sub_code, con_holding,
         store_code, supplier_code, delivery_day, minimum_stock_required, order_qty, order_without_pcb,
         order_day
         from ${orderDfView}
        """

    sqlc.sql(order_sql)

    sqlc.sql(s"refresh ${modelRun.orderTableName} ")

    val order_hist_sql =
      s"""
        insert overwrite table ${modelRun.orderHistTableName} partition(run_date, item_id, sub_id)
         select dept_code, item_code, sub_code, con_holding,
         store_code, supplier_code, order_day, delivery_day, minimum_stock_required, order_qty, order_without_pcb,
         ${modelRun.runDateStr}, item_id, sub_id
         from ${orderDfView}
        """

    sqlc.sql(order_hist_sql)

    sqlc.sql(s"refresh ${modelRun.orderHistTableName} ")

    LogUtil.info(s"Number of order lines: ${finalOrderDf.count()}")
  }

  /**
    * Insert generated DC order to datalake
    * 将DC/货仓订单结果写入datalake
    *
    * @param resDf
    * @param modelRun Job run information 脚本运行信息
    * @param sqlc Spark SQLContext
    */
  def insertDcOrderToDatalake(resDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Unit = {

    val orderDf = resDf.filter("error_info == ''")

    val orderDfView = modelRun.flowType + "_result_df"

    orderDf.createOrReplaceTempView(orderDfView)

    val order_sql =
      s"""
        insert overwrite table ${modelRun.orderTableName} partition(order_day)
         select item_id, sub_id, dept_code, item_code, sub_code, con_holding,
         supplier_code, delivery_day, average_sales, order_qty, order_without_pcb,
         order_day
         from ${orderDfView}
        """

    sqlc.sql(order_sql)

    sqlc.sql(s"refresh ${modelRun.orderTableName} ")

    val order_hist_sql =
      s"""
        insert overwrite table ${modelRun.orderHistTableName} partition(run_date, item_id, sub_id)
         select dept_code, item_code, sub_code, con_holding,
         supplier_code, order_day, delivery_day, average_sales, order_qty, order_without_pcb,
         ${modelRun.runDateStr}, item_id, sub_id
         from ${orderDfView}
        """

    sqlc.sql(order_hist_sql)

    sqlc.sql(s"refresh ${modelRun.orderHistTableName} ")

    LogUtil.info(s"Number of order lines: ${orderDf.count()}")
  }

}
