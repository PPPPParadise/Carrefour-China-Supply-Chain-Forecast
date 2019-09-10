package carrefour.forecast.util

import carrefour.forecast.model.{DateRow, ModelRun}
import org.apache.spark.sql.{DataFrame, _}

/**
  * Utility used for processing dataframe<br />
  * 用于处理分布式数据集的工具函数
  */
object DataUtil {

  /**
    * Filter dataframe by using item ID, sub ID, and Entity ID (store code or con holding code).
    * One or more of these three conditions can be used<br />
    * 从分布式数据集中筛选对应的记录。可以使用三种条件中的一种或多种。
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
    * Insert job run information to datalake<br />
    * 将脚本运行信息写入数据池
    *
    * @param modelRun Job run information 脚本运行信息
    * @param output Job run output 脚本运行输出
    * @param info Job run other information 脚本运行其它信息
    * @param sqlc Spark SQLContext
    */
  def insertJobRunInfoToDatalake(modelRun: ModelRun, status: String,
                                 output: String, info: String, error: String, sqlc: SQLContext): Unit = {

    var scriptName = "Forecast process for " + modelRun.flowType
    if (modelRun.isSimulation) {
      scriptName = "Forecast simulation process for " + modelRun.flowType
    }

    var scriptType = "Order run"

    if (modelRun.isSimulation) {
      scriptType = "Order simulation run"
    }

    if (modelRun.isDebug) {
      scriptType = "Order Debug run"
    }

    var encodeError = error.replace("\'", "").replace("\"", "")

    sqlc.sql(s"insert into vartefact.forecast_script_runs values (now(), " +
      s"'${modelRun.runDateStr}', '${status}' , '${scriptName}', '${scriptType}'," +
      s"'${modelRun.getScriptParameter}', '${output}', '${info}', '${encodeError}')")

  }

  /**
    * Insert debug result to datalake<br />
    * 将调试运行结果写入数据池
    *
    * @param debugDf debug result. debug运行结果
    * @param debugTableName Database and table name for debug process 调试结果表的数据库名及表名
    * @param sqlc Spark SQLContext
    */
  def insertDebugInfoToDatalake(debugDf: Dataset[DateRow], debugTableName: String, sqlc: SQLContext): Unit = {

    debugDf.write.format("parquet").mode("overwrite").saveAsTable(debugTableName)

    sqlc.sql(s"refresh ${debugTableName}")
  }


  /**
    * Insert generated store order to datalake<br />
    * 将门店订单结果写入数据池
    *
    * @param orderDf
    * @param modelRun Job run information 脚本运行信息
    * @param sqlc Spark SQLContext
    * @return Number of orders 订单的数量
    */
  def insertOrderToDatalake(orderDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Long = {

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

    orderDf.count()
  }

  /**
    * Insert generated DC order to datalake<br />
    * 将大仓订单结果写入数据池
    *
    * @param resDf
    * @param modelRun Job run information 脚本运行信息
    * @param sqlc Spark SQLContext
    * @return Number of orders 订单的数量
    */
  def insertDcOrderToDatalake(resDf: Dataset[DateRow], modelRun: ModelRun, sqlc: SQLContext): Long = {

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

    orderDf.count()
  }

}
