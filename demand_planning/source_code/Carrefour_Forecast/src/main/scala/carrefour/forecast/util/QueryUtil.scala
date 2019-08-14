package carrefour.forecast.util

import carrefour.forecast.model.ItemEntity
import carrefour.forecast.queries.{DcQueries, SimulationQueries, StoreQueries}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import carrefour.forecast.model.EnumFlowType.FlowType

/**
  * Utility used to query data
  */
object QueryUtil {

  /**
    * Get current store stock level
    * 获取当前门店库存
    *
    * @param stockDateStr Stock level in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param flowType Flow Type
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param spark Spark session
    * @param isSimulation Whether it is simulation process 是否为模拟运行
    * @return Current store stock level 当前门店库存
    */
  def getActualStockMap(stockDateStr: String, flowType:FlowType.Value, isDcFlow: Boolean, viewName: String,
                        spark: SparkSession, isSimulation: Boolean): Map[ItemEntity, Double] = {
    import spark.implicits._

    var stockSql = StoreQueries.getActualStockLevelSql(stockDateStr, viewName)

    if (isSimulation) {
      stockSql = SimulationQueries.getSimulationActualStockLevelSql(stockDateStr, flowType, viewName)
    }

    val stockDf = spark.sqlContext.sql(stockSql)

    val stockLevelMap = stockDf.distinct().map(row => {
      val itemStore = ItemEntity(row.getAs[Integer]("item_id"),
        row.getAs[Integer]("sub_id"),
        row.getAs[String]("entity_code"),
        isDcFlow)

      itemStore -> row.getAs[Double]("stock_level")
    }).collect().toMap

    return stockLevelMap
  }

  /**
    * Get current DC stock level
    * 获取当前DC/货仓库存
    *
    * @param stockDateStr Stock level in yyyyMMdd String format 文本格式的库存日期，为yyyyMMdd格式
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param spark Spark session
    * @return Current DC stock level 当前DC/货仓库存
    */
  def getDCActualStockMap(startDateStr: String, stockDateStr: String, isDcFlow: Boolean, viewName: String,
                          spark: SparkSession , isSimulation: Boolean): Map[ItemEntity, Double] = {
    import spark.implicits._


    var stockSql = DcQueries.getDcActualStockLevelSql(stockDateStr, viewName)
    if (isSimulation) {
      stockSql = SimulationQueries.getSimulationDcActualStockLevelSql(startDateStr, stockDateStr, viewName)
    }

    val stockDf = spark.sqlContext.sql(stockSql)
    val stockLevelMap = stockDf.distinct().map(row => {
      val itemStore = ItemEntity(row.getAs[Integer]("item_id"),
        row.getAs[Integer]("sub_id"),
        row.getAs[String]("entity_code"),
        isDcFlow)

      itemStore -> row.getAs[Double]("stock_level")
    }).collect().toMap

    return stockLevelMap
  }

  /**
    * Get DM orders
    * 获取DM订单
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param spark Spark session
    * @return DM orders. DM订单
    */
  def getDmOrderMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                    spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val dmOrderSql = StoreQueries.getDmOrdersSql(startDateStr, endDateStr, viewName)
    val dmOrderDf = spark.sqlContext.sql(dmOrderSql)

    val grouppedDmDf = dmOrderDf.groupByKey(row => ItemEntity(row.getAs[Integer]("item_id"),
      row.getAs[Integer]("sub_id"),
      row.getAs[String]("entity_code"),
      isDcFlow))

    val dmOrderMap = grouppedDmDf.mapGroups((itemEntity, rows) => {
      var dmOrderList: List[Tuple2[String, Double]] = List()
      for (row <- rows) {
        dmOrderList = (row.getAs[String]("first_delivery_date"),
          row.getAs[Double]("dm_order_qty")) :: dmOrderList
      }
      itemEntity -> dmOrderList
    }

    ).collect().toMap

    return dmOrderMap
  }

  /**
    * Get store on the way order quantity and delivery date
    * 获取门店在途订单订货量及其抵达日期
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param orderTableName Database and name for order table 订单表的数据库名及表名
    * @param spark Spark session
    * @return On the way order 在途订单
    */
  def getOnTheWayStockMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                          orderTableName: String, spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val onTheWayStockSql = StoreQueries.getOnTheWayStockSql(startDateStr, endDateStr, viewName, orderTableName)
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

  /**
    * Get sales predictions
    * 获取销量预测
    *
    * @param dateMapDf All combinations of item, store, and dates 全部商品，门店，及日期的组合
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param sqlc Spark SQLContext
    * @return Sales predictions 销量预测
    */
  def getSalesPrediction(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                         sqlc: SQLContext): DataFrame = {

    // Get sales prediction
    val salesPredictionSql = StoreQueries.getSalesPredictionSql(startDateStr, endDateStr, viewName)
    var salesPredictionDf = sqlc.sql(salesPredictionSql)

    salesPredictionDf = dateMapDf.join(salesPredictionDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    salesPredictionDf = salesPredictionDf.withColumn("predict_sales",
      when(col("daily_sales_prediction").isNull, lit(0.0)).otherwise(col("daily_sales_prediction")))
    salesPredictionDf = salesPredictionDf.drop("daily_sales_prediction")

    return salesPredictionDf
  }

  /**
    * Get future store orders to DC
    * 获取门店向DC/货仓未来订货量
    *
    * @param dateMapDf All combinations of item, store, and dates 全部商品，门店，及日期的组合
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param sqlc Spark SQLContext
    * @return Future store orders to DC 门店向DC/货仓未来订货量
    */
  def getStoreOrderToDc(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                        sqlc: SQLContext): DataFrame = {

    // Get sales prediction
    val storeOrderToDcSql = DcQueries.getStoreOrderToDcSql(startDateStr, endDateStr, viewName)
    var storeOrderToDcDf = sqlc.sql(storeOrderToDcSql)

    storeOrderToDcDf = dateMapDf.join(storeOrderToDcDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    storeOrderToDcDf = storeOrderToDcDf.withColumn("predict_sales",
      when(col("daily_sales_prediction").isNull, lit(0.0)).otherwise(col("daily_sales_prediction")))
    storeOrderToDcDf = storeOrderToDcDf.drop("daily_sales_prediction")

    return storeOrderToDcDf
  }

  /**
    * Get past generated orders for DC
    * 获取过去生成的DC订单规划
    *
    * @param startDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param endDateStr Start date in yyyyMMdd String format 文本格式的起始日期，为yyyyMMdd格式
    * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
    * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
    * @param orderTableName Database and name for order table 订单表的数据库名及表名
    * @param spark Spark session
    * @return Past generated orders for DC过去生成的DC订单规划
    */
  def getDcPastOrdersMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                         orderTableName: String, spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val pastOrdersSql = DcQueries.getDcPastOrdersSql(startDateStr, endDateStr, viewName, orderTableName)
    val pastOrdersDf = spark.sqlContext.sql(pastOrdersSql)

    val grouppedDf = pastOrdersDf.groupByKey(row => ItemEntity(row.getAs[Integer]("item_id"),
      row.getAs[Integer]("sub_id"),
      row.getAs[String]("entity_code"),
      isDcFlow))

    val pastOrdersMap = grouppedDf.mapGroups((itemEntity, rows) => {
      var pastOrdersList: List[Tuple2[String, Double]] = List()
      for (row <- rows) {
        pastOrdersList = (row.getAs[String]("order_day"),
          row.getAs[Double]("order_qty")) :: pastOrdersList
      }
      itemEntity -> pastOrdersList
    }

    ).collect().toMap

    return pastOrdersMap
  }


}
