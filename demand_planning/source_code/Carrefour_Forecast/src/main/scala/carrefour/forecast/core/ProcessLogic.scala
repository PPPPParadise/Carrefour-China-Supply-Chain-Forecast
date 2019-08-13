package carrefour.forecast.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.{ItemEntity, ModelRun}
import carrefour.forecast.queries.{DcQueries, StoreQueries}
import carrefour.forecast.util._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object ProcessLogic {

  /**
    * Job run logic
    * 脚本运行逻辑
    * @param modelRun Job run information 脚本运行信息
    */
  def process(modelRun: ModelRun): Unit = {
    val outputSb = new StringBuilder
    val infoSb = new StringBuilder

    val spark = SparkSession
      .builder
      .appName("Forecast process for " + modelRun.flowType)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val sqlc = spark.sqlContext

    try {
      LogUtil.info(s"\n\nRun for ${modelRun.runDateStr}\n " +
        s"Forecast process for ${modelRun.flowType} start with input parameter" +
        s" \n ${modelRun.getScriptParameter} \n")
      infoSb.append("Job Start:")
        .append(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
        .append(",")

      // initialize run
      val item_id = modelRun.itemId
      val sub_id = modelRun.subId
      val entity_code = modelRun.storeCode

      val runDateStr = modelRun.runDateStr
      val dateKeyFormat = new SimpleDateFormat("yyyyMMdd")
      val cal = Calendar.getInstance()
      val runDate = dateKeyFormat.parse(runDateStr)

      val startDate = runDate
      val startDateStr = dateKeyFormat.format(startDate)

      cal.setTime(runDate)
      cal.add(Calendar.DATE, 63)
      val endDate = cal.getTime
      val endDateStr = dateKeyFormat.format(endDate)

      cal.setTime(runDate)
      cal.add(Calendar.DATE, -1)
      val stockDateStr = dateKeyFormat.format(cal.getTime)

      sqlc.setConf("hive.support.concurrency", "true")
      sqlc.setConf("hive.exec.parallel", "true")
      sqlc.setConf("hive.exec.dynamic.partition", "true")
      sqlc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      sqlc.setConf("hive.exec.max.dynamic.partitions", "4096")
      sqlc.setConf("hive.exec.max.dynamic.partitions.pernode", "4096")

      // Get in item store that valid for logic
      val inScopeItemEntitySql = getInScopeItemEntitySql(modelRun, startDateStr, stockDateStr)
      var inScopeItemEntityDf = sqlc.sql(inScopeItemEntitySql)
      inScopeItemEntityDf = DataUtil.filterDateFrame(inScopeItemEntityDf, item_id, sub_id, entity_code)
      val itemEntityCnt = inScopeItemEntityDf.count
      var outputLine = s"Number of item store/holding combinations in scope: ${itemEntityCnt}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")
      if (itemEntityCnt == 0) {
        LogUtil.info(s"skip date ${runDateStr} cause no in scope item for today")
        infoSb.append(s"skip date ${runDateStr} cause no in scope item for today")
        infoSb.append("Job Finish:")
          .append(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
        DataUtil.insertJobRunInfoToDatalake(modelRun, "Success",
          outputSb.toString(), infoSb.toString(), "" ,sqlc)
        return
      }

      inScopeItemEntityDf.createOrReplaceTempView(modelRun.viewName)

      // Get all order and delivery days between start and end date
      val orderDeliverySql = getOrderDeliverySql(modelRun, stockDateStr, startDateStr, endDateStr)
      val orderDeliveryDf = sqlc.sql(orderDeliverySql)

      outputLine = s"Number of total order opportunities: ${orderDeliveryDf.count()}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      val activeOrderDeliveryDf = extractActiveOrderOpportunities(orderDeliveryDf, modelRun, sqlc)

      val activeOrderOpportunitiesCnt = activeOrderDeliveryDf.count()

      outputLine = s"Number of active order opportunities: ${activeOrderOpportunitiesCnt}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      if (activeOrderOpportunitiesCnt == 0) {
        LogUtil.info(s"skip date ${runDateStr} cause no active order opportunity for today")
        infoSb.append(s"skip date ${runDateStr} cause no active order opportunity for today")
        infoSb.append("Job Finish:")
          .append(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
        DataUtil.insertJobRunInfoToDatalake(modelRun, "Success",
          outputSb.toString(), infoSb.toString(), "", sqlc)
        return
      }

      val activeItemEntities = extractActiveItem(activeOrderDeliveryDf)

      outputLine = s"Number of active items store/holding combinations: ${activeItemEntities.count()}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      activeItemEntities.cache()
      activeItemEntities.createOrReplaceTempView(modelRun.viewName)

      activeItemEntities.write.format("parquet")
        .mode("overwrite")
        .saveAsTable("vartefact." + modelRun.viewName)

      // Find the actual stock level
      val stockLevelMap = getActualStockMap(startDateStr, stockDateStr, modelRun.isDcFlow, modelRun.viewName, spark, modelRun.isSimulation)

      outputLine = s"Number of current stock level found: ${stockLevelMap.size}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      // Get DM orders to be delivered
      val dmOrdersMap = getDmOrderMap(modelRun, startDateStr, endDateStr, spark)

      // Get orders to be delivered
      val onTheWayStockMap = getOnTheWayStockMap(modelRun, startDateStr, endDateStr, spark)

      // Get item stores that having sales prediction and valid for logic
      val orderItemStore = activeItemEntities
        .select("item_id", "sub_id", "entity_code")
        .distinct()

      // Get all days between logic start and end date
      val calendarDf = sqlc.sql(StoreQueries.getCalendarSql(startDateStr, endDateStr))
      val dateMapDf = calendarDf.crossJoin(orderItemStore)

      // Find the sales prediction
      val salesPredictionDf = getSalesPrediction(modelRun, dateMapDf, startDateStr, endDateStr, sqlc)

      var predictionWithSalesDf = salesPredictionDf

      outputLine = s"Number of sales predictions: ${predictionWithSalesDf.count()}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      if (modelRun.isSimulation) {
        // Find the actual sales
        val actualSalesDf = SimulationUtil.getActualSales(dateMapDf, modelRun, startDateStr, endDateStr, sqlc)
        // For simulation process add actual sales in past
        predictionWithSalesDf = salesPredictionDf.join(actualSalesDf,
          Seq("item_id", "sub_id", "entity_code", "date_key"))
      }

      // Group records by item and store
      val unionedDf = predictionWithSalesDf.join(activeOrderDeliveryDf,
        Seq("item_id", "sub_id", "entity_code", "date_key"), "left")

      unionedDf.cache()

      outputLine = s"Count of unioned input dataframe: ${unionedDf.count()}"
      LogUtil.info(outputLine)
      outputSb.append(outputLine).append(",")

      val grouppedDf = unionedDf.groupByKey(row => ItemEntity(row.getAs[Integer]("item_id"),
        row.getAs[Integer]("sub_id"),
        row.getAs[String]("entity_code"),
        modelRun.isDcFlow))

      var errorDf: DataFrame = sqlc.emptyDataFrame

      // Perform logic for every item store combination
      val resDf = grouppedDf.flatMapGroups((ist, rows) => {
        OrderLogic.generateOrder(ist, rows, runDateStr,
          modelRun, stockLevelMap, dmOrdersMap, onTheWayStockMap)
      })

      if (modelRun.isSimulation && modelRun.isDebug) {
        DataUtil.insertDebugInfoToDatalake(resDf, modelRun.debugTableName, sqlc)

      } else if (modelRun.isSimulation) {
        SimulationUtil.insertSimulationResultToDatalake(resDf, modelRun, sqlc)

        outputLine = s"Number of simulation lines: ${resDf.count()}"
        LogUtil.info(outputLine)
        outputSb.append(outputLine).append(",")

        val orderDf = resDf.filter(row => row.is_order_day)
        SimulationUtil.insertSimulationOrderToDatalake(orderDf, modelRun, sqlc)

        outputLine = s"Number of simulation order lines: ${orderDf.count()}"
        LogUtil.info(outputLine)
        outputSb.append(outputLine).append(",")

      } else if (modelRun.isDebug) {
        DataUtil.insertDebugInfoToDatalake(resDf, modelRun.debugTableName, sqlc)

        outputLine = s"Number of debug lines: ${resDf.count()}"
        LogUtil.info(outputLine)
        outputSb.append(outputLine).append(",")

      } else if (modelRun.flowType == FlowType.DC) {
        val orderDf = resDf.filter(row => row.is_order_day)
        val orderDfCnt = DataUtil.insertDcOrderToDatalake(orderDf, modelRun, sqlc)

        outputLine = s"Number of order lines: ${orderDfCnt}"
        LogUtil.info(outputLine)
        outputSb.append(outputLine).append(",")

      } else {
        val orderDf = resDf.filter(row => row.is_order_day)
        val orderDfCnt = DataUtil.insertOrderToDatalake(orderDf, modelRun, sqlc)

        outputLine = s"Number of order lines: ${orderDfCnt}"
        LogUtil.info(outputLine)
        outputSb.append(outputLine).append(",")
      }

      errorDf = resDf.filter("error_info != ''").toDF()

      val errorCnt = errorDf.count()

      if (errorCnt > 0) {
        LogUtil.info(s"Error item store count: ${errorCnt}")
        for (row <- errorDf.collect()) {
          LogUtil.info(List("item_id:",
            row.getAs[Integer]("item_id"),
            "sub_id:",
            row.getAs[Integer]("sub_id"),
            "store_code:",
            row.getAs[String]("store_code"),
            "con_holding:",
            row.getAs[String]("con_holding"),
            row.getAs[String]("error_info")).mkString(" "))
        }

        throw new RuntimeException(s"Not all items successfully processed")
      }

      infoSb.append("Job Finish:")
        .append(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))

      DataUtil.insertJobRunInfoToDatalake(modelRun, "Success",
        outputSb.toString(), infoSb.toString() ,"", sqlc)

      LogUtil.info("Finish")

    } catch {
      case ex: Exception => {
        LogUtil.error(ex.getMessage, ex)
        infoSb.append("Job Finish:")
          .append(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))

        val errorSb = new StringBuilder

        for (te <- ex.getStackTrace) {
          errorSb.append(te).append("|")
        }
        DataUtil.insertJobRunInfoToDatalake(modelRun, "Fail", outputSb.toString(),
          infoSb.toString(), errorSb.toString(), sqlc)
        throw ex
      }
    }

  }

  /**
    * SQL query to get in scope items for this job run
    * 生成查询运行中应包括的商品的SQL
    *
    * @param modelRun Job run information 脚本运行信息
    * @param orderDateStr Order date in yyyyMMdd String format 文本格式的订单日期，为yyyyMMdd格式
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @return SQL query
    */
  private def getInScopeItemEntitySql(modelRun: ModelRun, orderDateStr: String,
                                      stockDateStr: String): String = {
    val inScopeItemEntitySql = modelRun.flowType match {
      case FlowType.XDocking => StoreQueries.getXdockInScopeItemsSql(orderDateStr, stockDateStr)
      case FlowType.OnStockStore => StoreQueries.getOnStockStoreInScopeItemsSql(orderDateStr, stockDateStr)
      case FlowType.DC => DcQueries.getOnStockDcItemsSql(orderDateStr, stockDateStr)
    }

    inScopeItemEntitySql
  }


  /**
    * SQL query to get all order days for this job run
    * 生成查询周期中包括的全部订单日的SQL
    *
    * @param modelRun Job run information 脚本运行信息
    * @param stockDateStr Stock level in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param startDateStr Query start date in yyyyMMdd String format 文本格式的查询开始日期，为yyyyMMdd格式
    * @param endDateStr Query end date in yyyyMMdd String format 文本格式的查询截止日期，为yyyyMMdd格式
    * @return SQL query
    */
  private def getOrderDeliverySql(modelRun: ModelRun, stockDateStr: String, startDateStr: String,
                                  endDateStr: String): String = {
    val orderDeliverySql = modelRun.flowType match {
      case FlowType.XDocking => StoreQueries.getXDockingInScopeOrderDaysSql(stockDateStr,
        startDateStr, endDateStr, modelRun.viewName)
      case FlowType.OnStockStore => StoreQueries.getOnStockStoreInScopeOrderDaysSql(stockDateStr,
        startDateStr, endDateStr, modelRun.viewName)
      case FlowType.DC => DcQueries.getOnStockDcInScopeOrderDaysSql(startDateStr, endDateStr, modelRun.viewName)
    }

    orderDeliverySql
  }

  /**
    * Get order days that not in stop period
    * 获取不在停止订货日期内的订单日
    *
    * @param df All order days 全部订单日期
    * @param modelRun Job run information 脚本运行信息
    * @param sqlc Spark SQL context
    * @return Order days that not in stop period 不在停止订货日期内的订单日
    */
  private def extractActiveOrderOpportunities(df: DataFrame, modelRun: ModelRun, sqlc: SQLContext): DataFrame = {

    val newDf = df.where("(item_stop_start_date='' and item_stop_start_date='') " +
      "or ( item_stop_start_date!='' and to_timestamp(delivery_date, 'yyyyMMdd') < to_timestamp(item_stop_start_date, 'dd/MM/yyyy')) " +
      "or ( item_stop_end_date!='' and to_timestamp(order_date, 'yyyyMMdd') > to_timestamp(item_stop_end_date, 'dd/MM/yyyy')) ")

    newDf
  }

  /**
    * Get information for items that can be ordered
    * 获取可以订货的商品的信息
    *
    * @param df Order days 订单日期
    * @return Item information 商品信息
    */
  private def extractActiveItem(df: DataFrame): DataFrame = {
    df.select("item_id", "sub_id", "dept_code", "item_code",
      "sub_code", "con_holding", "store_code", "entity_code",
      "supplier_code", "rotation", "pcb", "delivery_time").distinct()
  }

  // Find the actual stock level
  /**
    * Get Actual stock level
    * 获取当前库存
    *
    * @param stockDateStr Stock level date in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param spark Spark session
    * @param isSimulation Whether it is simulation process 是否为模拟运行
    * @return Current stock level 当前库存
    */
  def getActualStockMap(startDateStr: String, stockDateStr: String, isDcFlow: Boolean, viewName: String,
                        spark: SparkSession, isSimulation: Boolean): Map[ItemEntity, Double] = {
    if (isDcFlow) {
      QueryUtil.getDCActualStockMap(startDateStr, stockDateStr, isDcFlow, viewName, spark, isSimulation)
    } else {
      QueryUtil.
        getActualStockMap(stockDateStr, isDcFlow, viewName, spark, isSimulation)
    }
  }


  /**
    * Get sales predictions
    * 获取销量预测
    *
    * @param modelRun Job run information 脚本运行信息
    * @param dateMapDf Item information and date 商品及日期信息
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param sqlc Spark SQL context
    * @return Sales prediction 销量预测
    */
  private def getSalesPrediction(modelRun: ModelRun, dateMapDf: DataFrame, startDateStr: String, endDateStr: String,
                                 sqlc: SQLContext): DataFrame = {

    if (modelRun.flowType == FlowType.DC && modelRun.isSimulation) {
      SimulationUtil
        .getSimulationStoreOrderToDc(dateMapDf, startDateStr, endDateStr, modelRun.viewName, sqlc)

    } else if (modelRun.flowType == FlowType.DC) {
      QueryUtil
        .getStoreOrderToDc(dateMapDf, startDateStr, endDateStr, modelRun.viewName, sqlc)

    } else {
      QueryUtil
        .getSalesPrediction(dateMapDf, startDateStr, endDateStr, modelRun.viewName, sqlc)
    }
  }

  /**
    * Get orders from DM process
    * 获取DM订单系统生成的DM订单
    *
    * @param modelRun Job run information 脚本运行信息
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param spark Spark session
    * @return DM orders DM 订单
    */
  private def getDmOrderMap(modelRun: ModelRun, startDateStr: String, endDateStr: String,
                            spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    modelRun.flowType match {
      case FlowType.XDocking | FlowType.OnStockStore =>
        QueryUtil.getDmOrderMap(startDateStr, endDateStr, modelRun.isDcFlow, modelRun.viewName, spark)
      case FlowType.DC =>
        Map.empty[ItemEntity, List[Tuple2[String, Double]]]
    }
  }

  /**
    * Get on the way order quantity and delivery date
    * 获取在途订单订货量及其抵达日期
    *
    * @param modelRun Job run information 脚本运行信息
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param spark Spark session
    * @return On the way order 在途订单
    */
  private def getOnTheWayStockMap(modelRun: ModelRun, startDateStr: String, endDateStr: String,
                                  spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    modelRun.flowType match {
      case FlowType.XDocking | FlowType.OnStockStore => {
        if (modelRun.isSimulation) {
          SimulationUtil.getSimulationOnTheWayStockMap(startDateStr, endDateStr,
            modelRun.isDcFlow, modelRun.viewName, spark)
        } else {
          QueryUtil.getOnTheWayStockMap(startDateStr, endDateStr, modelRun.isDcFlow, modelRun.viewName,
            modelRun.orderTableName, spark)
        }
      }

      case FlowType.DC => {

        val dateKeyFormat = new SimpleDateFormat("yyyyMMdd")
        val cal = Calendar.getInstance()
        val startDate = dateKeyFormat.parse(startDateStr)

        cal.setTime(startDate)
        cal.add(Calendar.DATE, 15)
        val endDate = cal.getTime
        val newEndDateStr = dateKeyFormat.format(endDate)

        if (modelRun.isSimulation) {
          SimulationUtil.getSimulationDcPastOrdersMap(startDateStr, newEndDateStr,
            modelRun.isDcFlow, modelRun.viewName, spark)
        } else {
          QueryUtil.getDcPastOrdersMap(startDateStr, newEndDateStr, modelRun.isDcFlow, modelRun.viewName,
            modelRun.orderTableName, spark)
        }
      }
    }
  }



}
