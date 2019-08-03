package carrefour.forecast.core

import java.text.SimpleDateFormat
import java.util.Calendar

import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.{ItemEntity, ModelRun}
import carrefour.forecast.queries.{DcQueries, StoreQueries}
import carrefour.forecast.util._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object ProcessLogic {

  def process(modelRun: ModelRun): Unit = {
    try {
      LogUtil.info("\n\n\n" + modelRun.toString)

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

      LogUtil.info(s"Flow type is:${modelRun.flowType}, " +
        s"Run date is: ${runDateStr}, " +
        s"Forecast start date: ${startDateStr}, " +
        s"Forecast end date: ${endDateStr}, " +
        s"Stock level date: ${stockDateStr}, " +
        s"is simulation run: ${modelRun.isSimulation}")

      val spark = SparkSession
        .builder
        .appName("Forecast process for " + modelRun.flowType)
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._

      val sc = spark.sparkContext
      val sqlc = spark.sqlContext

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
      LogUtil.info(s"Number of item store/holding combinations in scope: ${itemEntityCnt}")
      if (itemEntityCnt == 0) {
        LogUtil.info(s"skip date ${runDateStr} ca fuse no in scope item for today")
        return
      }

      inScopeItemEntityDf.createOrReplaceTempView(modelRun.viewName)

      // Get all order and delivery days between start and end date
      val orderDeliverySql = getOrderDeliverySql(modelRun, stockDateStr, startDateStr, endDateStr)
      val orderDeliveryDf = sqlc.sql(orderDeliverySql)

      LogUtil.info(s"Number of total order opportunities: ${orderDeliveryDf.count()}")

      val activeOrderDeliveryDf = extractActiveOrderOpportunities(orderDeliveryDf, modelRun, sqlc)

      val activeOrderOpportunitiesCnt = activeOrderDeliveryDf.count()
      LogUtil.info(s"Number of active order opportunities: ${activeOrderOpportunitiesCnt}")
      if (activeOrderOpportunitiesCnt == 0) {
        LogUtil.info(s"skip date ${runDateStr} cause no active order opportunity for today")
        return
      }

      val activeItemEntities = extractActiveItem(activeOrderDeliveryDf)
      LogUtil.info(s"Number of active items store/holding combinations: ${activeItemEntities.count()}")

      activeItemEntities.cache()
      activeItemEntities.createOrReplaceTempView(modelRun.viewName)

      activeItemEntities.write.format("parquet")
        .mode("overwrite")
        .saveAsTable("vartefact." + modelRun.viewName)

      // Find the actual stock level in each store for each item
      val stockLevelMap = QueryUtil.
        getActualStockMap(stockDateStr, modelRun.isDcFlow, modelRun.viewName, spark, modelRun.isSimulation)
      LogUtil.info(s"Number of stockLevelMap: ${stockLevelMap.size}")

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

      LogUtil.info(s"Number of sales predictions: ${predictionWithSalesDf.count()}")

      if (modelRun.isSimulation) {
        // Find the actual sales
        val actualSalesDf = getActualSales(dateMapDf, modelRun, startDateStr, endDateStr, sqlc)
        // For simulation process add actual sales in past
        predictionWithSalesDf = salesPredictionDf.join(actualSalesDf,
          Seq("item_id", "sub_id", "entity_code", "date_key"))
      }

      // Group records by item and store
      val unionedDf = predictionWithSalesDf.join(activeOrderDeliveryDf,
        Seq("item_id", "sub_id", "entity_code", "date_key"), "left")

      unionedDf.cache()

      LogUtil.info(s"Count of unioned input dataframe: ${unionedDf.count()}")

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

      if (modelRun.isSimulation) {
        SimulationUtil.insertSimulationStockToDatalake(resDf, modelRun, sqlc)
        SimulationUtil.insertSimulationResultToDatalake(resDf, modelRun, sqlc)
        LogUtil.info(s"Number of simulation lines: ${resDf.count()}")

        val orderDf = resDf.filter(row => row.is_order_day)
        SimulationUtil.insertSimulationOrderToDatalake(orderDf, modelRun, sqlc)
        LogUtil.info(s"Number of simulation order lines: ${orderDf.count()}")

      } else if (modelRun.isDebug) {
        DataUtil.insertDebugInfoToDatalake(resDf, modelRun, sqlc)

      } else if (modelRun.flowType == FlowType.DC) {
        val orderDf = resDf.filter(row => row.is_order_day)
        DataUtil.insertDcOrderToDatalake(orderDf, modelRun, sqlc)

      } else {
        val orderDf = resDf.filter(row => row.is_order_day)
        DataUtil.insertOrderToDatalake(orderDf, modelRun, sqlc)
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
      LogUtil.info("Finish")

    } catch {
      case ex: Exception => {
        LogUtil.error(ex.getMessage, ex)
        throw ex
      }
    }

  }

  private def getInScopeItemEntitySql(modelRun: ModelRun, startDateStr: String,
                                      stockDateStr: String): String = {
    val inScopeItemEntitySql = modelRun.flowType match {
      case FlowType.XDocking => StoreQueries.getXdockInScopeItemsSql(startDateStr, stockDateStr)
      case FlowType.OnStockStore => StoreQueries.getOnStockStoreInScopeItemsSql(startDateStr, stockDateStr)
      case FlowType.DC => DcQueries.getOnStockDcItemsSql(startDateStr, stockDateStr)
    }

    inScopeItemEntitySql
  }

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

  private def extractActiveOrderOpportunities(df: DataFrame, modelRun: ModelRun, sqlc: SQLContext): DataFrame = {

    if (modelRun.isSimulation && modelRun.flowType != FlowType.DC) {
      SimulationUtil.insertSimulationItemStatusToDatalake(df, modelRun, sqlc)
    }

    val newDf = df.where("(item_stop_start_date='' and item_stop_start_date='') " +
      "or ( item_stop_start_date!='' and to_timestamp(delivery_date, 'yyyyMMdd') < to_timestamp(item_stop_start_date, 'dd/MM/yyyy')) " +
      "or ( item_stop_end_date!='' and to_timestamp(order_date, 'yyyyMMdd') > to_timestamp(item_stop_end_date, 'dd/MM/yyyy')) ")

    newDf
  }

  private def extractActiveItem(df: DataFrame): DataFrame = {
    df.select("item_id", "sub_id", "dept_code", "item_code",
      "sub_code", "con_holding", "store_code", "entity_code",
      "supplier_code", "rotation", "pcb", "delivery_time").distinct()
  }

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

  private def getDmOrderMap(modelRun: ModelRun, startDateStr: String, endDateStr: String,
                            spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    modelRun.flowType match {
      case FlowType.XDocking | FlowType.OnStockStore =>
        QueryUtil.getDmOrderMap(startDateStr, endDateStr, modelRun.isDcFlow, modelRun.viewName, spark)
      case FlowType.DC =>
        Map.empty[ItemEntity, List[Tuple2[String, Double]]]
    }
  }

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

  private def getActualSales(dateMapDf: DataFrame, modelRun: ModelRun, startDateStr: String, endDateStr: String,
                             sqlc: SQLContext): DataFrame = {

    modelRun.flowType match {
      case FlowType.XDocking | FlowType.OnStockStore => {
        SimulationUtil
          .getActualSales(dateMapDf, startDateStr, endDateStr, modelRun.viewName, sqlc)
      }

      case FlowType.DC => {
        SimulationUtil
          .getDCActualSales(dateMapDf, startDateStr, endDateStr, modelRun.viewName, sqlc)
      }
    }

  }

}
