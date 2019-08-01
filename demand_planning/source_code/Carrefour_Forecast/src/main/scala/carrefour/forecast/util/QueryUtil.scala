package carrefour.forecast.util

import carrefour.forecast.model.ItemEntity
import carrefour.forecast.queries.{CoreQueries, SimulationQueries}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object QueryUtil {

  def getActualStockMap(stockDateStr: String, isDcFlow: Boolean, viewName: String,
                        spark: SparkSession, isSimulation: Boolean): Map[ItemEntity, Double] = {
    import spark.implicits._

    var stockSql = CoreQueries.getActualStockLevelSql(stockDateStr, viewName)

    if (isSimulation) {
      stockSql = SimulationQueries.getSimulationActualStockLevelSql(stockDateStr, viewName)
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

  def getDCActualStockMap(stockDateStr: String, isDcFlow: Boolean, viewName: String,
                        spark: SparkSession): Map[ItemEntity, Double] = {
    import spark.implicits._

    val stockSql = CoreQueries.getDcActualStockLevelSql(stockDateStr, viewName)
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

  def getDmOrderMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                    spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val dmOrderSql = CoreQueries.getDmOrdersSql(startDateStr, endDateStr, viewName)
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

  def getOnTheWayStockMap(startDateStr: String, endDateStr: String, isDcFlow: Boolean, viewName: String,
                          orderTableName: String, spark: SparkSession): Map[ItemEntity, List[Tuple2[String, Double]]] = {
    import spark.implicits._

    val onTheWayStockSql = CoreQueries.getOnTheWayStockSql(startDateStr, endDateStr, viewName, orderTableName)
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

  def getSalesPrediction(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                         sqlc: SQLContext): DataFrame = {

    // Get sales prediction
    val salesPredictionSql = CoreQueries.getSalesPredictionSql(startDateStr, endDateStr, viewName)
    var salesPredictionDf = sqlc.sql(salesPredictionSql)

    salesPredictionDf = dateMapDf.join(salesPredictionDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    salesPredictionDf = salesPredictionDf.withColumn("predict_sales",
      when(col("daily_sales_prediction").isNull, lit(0.0)).otherwise(col("daily_sales_prediction")))
    salesPredictionDf = salesPredictionDf.drop("daily_sales_prediction")

    return salesPredictionDf
  }

  def getStoreOrderToDc(dateMapDf: DataFrame, startDateStr: String, endDateStr: String, viewName: String,
                         sqlc: SQLContext): DataFrame = {

    // Get sales prediction
    val storeOrderToDcSql = CoreQueries.getStoreOrderToDcSql(startDateStr, endDateStr, viewName)
    var storeOrderToDcDf = sqlc.sql(storeOrderToDcSql)

    storeOrderToDcDf = dateMapDf.join(storeOrderToDcDf, Seq("item_id", "sub_id", "entity_code", "date_key"), "left")
    storeOrderToDcDf = storeOrderToDcDf.withColumn("predict_sales",
      when(col("daily_sales_prediction").isNull, lit(0.0)).otherwise(col("daily_sales_prediction")))
    storeOrderToDcDf = storeOrderToDcDf.drop("daily_sales_prediction")

    return storeOrderToDcDf
  }


}
