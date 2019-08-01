package carrefour.forecast.process

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession


object BpTrxnGroup {

  case class Transaction(item_id: Integer,
                         sub_id: Integer,
                         line_num: Integer,
                         store_code: String,
                         trxn_date: Timestamp,
                         ticket_id: String,
                         var group_id: String)

  def main(args: Array[String]) {

    try {
      val databaseName = args(0)
      val inputTableName = args(1)
      val outputTableName = args(2)

      val spark = SparkSession.builder.appName("BP group trades").getOrCreate()
      import spark.implicits._

      val sqlc = spark.sqlContext

      val trxnDf = sqlc.sql("select item_id, " +
        "sub_id, line_num, store_code, trxn_date, ticket_id " +
        s"from ${databaseName}.${inputTableName} " +
        "where trxn_date >'2016-12-31'" +
        "group by item_id, sub_id, store_code, trxn_date, ticket_id, line_num")

      val trxns = trxnDf.map(row => Transaction(
        row.getAs[Integer]("item_id"), row.getAs[Integer]("sub_id"),
        row.getAs[Integer]("line_num"), row.getAs[String]("store_code"),
        row.getAs[Timestamp]("trxn_date"), row.getAs[String]("ticket_id"),
        row.getAs[String]("ticket_id")
      ))

      val grpTrxns = trxns.groupByKey(t => (t.item_id, t.sub_id, t.store_code, t.trxn_date))

      val resDf = grpTrxns.flatMapGroups((tuple, ts) => {
        var tradeList = ts.toList
        tradeList = tradeList.sortBy(t => t.ticket_id)
        var tradeOne = tradeList.head
        var group = tradeOne.group_id
        var i = 1;
        while (i < tradeList.size) {
          val tradeTwo = tradeList(i)
          if (tradeTwo.ticket_id.toLong - tradeOne.ticket_id.toLong > 5) {
            group = tradeTwo.ticket_id
          }
          tradeTwo.group_id = group
          tradeOne = tradeTwo
          i = i + 1
        }

        tradeList
      })

      resDf.write.mode("overwrite")
        .format("parquet")
        .saveAsTable(s"${databaseName}.${outputTableName}")

      spark.stop()
    } catch {
      case ex: Exception => {
        print("Please check input format. Should be DatabaseName InputTableName outputTableName")
        throw ex
      }

    }

  }
}
