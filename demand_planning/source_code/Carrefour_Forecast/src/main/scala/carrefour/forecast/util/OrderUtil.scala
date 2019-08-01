package carrefour.forecast.util

import carrefour.forecast.model._
import org.apache.spark.sql.Row

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

object OrderUtil {

  def generateOrder(ist: ItemEntity, rows: Iterator[Row], runDateStr: String,
                    modelRun: ModelRun,
                    stockLevelMap: Map[ItemEntity, Double],
                    dmOrdersMap: Map[ItemEntity, List[Tuple2[String, Double]]],
                    onTheWayStockMap: Map[ItemEntity, List[Tuple2[String, Double]]]): List[DateRow] = {

    var dateRowList = List.empty[DateRow]

    // For debug and tracking purpose
    var orderD = 0
    var futureD = 0
    var deliveryDay = ""
    var futureStock = 0.0
    var lasti = 0
    var lastj = 0



    var order: DateRow = DateRow(runDateStr, "", ist.item_id, ist.sub_id, "", "", ""
      , "", ist.entity_code, "", "", 0.0, "", 0.0, "", "", 0.0
    )

    if (modelRun.isDcFlow) {
      order = DateRow(runDateStr, "", ist.item_id, ist.sub_id, "", "", ""
        , ist.entity_code,  "", "", "", 0.0, "", 0.0, "", "", 0.0
      )
    }

    try {

      var dateRowListBuffer = new ListBuffer[DateRow]()

      for (row <- rows) {
        val dateRow = DateRow(
          runDateStr,
          row.getAs[String]("date_key"),

          ist.item_id,
          ist.sub_id,
          row.getAs[String]("dept_code"),
          row.getAs[String]("item_code"),
          row.getAs[String]("sub_code"),

          row.getAs[String]("con_holding"),
          row.getAs[String]("store_code"),
          row.getAs[String]("supplier_code"),
          row.getAs[String]("rotation"),

          row.getAs[Double]("pcb"),
          row.getAs[String]("delivery_time"),
          0.0,

          row.getAs[String]("order_date"),
          row.getAs[String]("delivery_date"),

          row.getAs[Double]("predict_sales")
        )

        if (null != dateRow.order_day) {
          dateRow.is_order_day = true
          dateRow.minimum_stock_required = getMinimumStock(row, ist.is_dc_flow)
          dateRow.ittreplentyp = row.getAs[Integer]("ittreplentyp")
          dateRow.ittminunit = row.getAs[Integer]("ittminunit")
          dateRow.shelf_capacity = row.getAs[String]("shelf_capacity")
        }

        if (modelRun.isSimulation) {
          dateRow.actual_sales = row.getAs[Double]("actual_sales")
        }

        dateRowListBuffer += dateRow

      }

      dateRowList = dateRowListBuffer.toList.sortBy(_.date_key)(Ordering[String])

      var currentStock = stockLevelMap.getOrElse(ist, modelRun.defaultStockLevel)
      var dmDeliveryMap: Map[String, Double] = Map()
      var deliveryMap: Map[String, Double] = Map()

      if (dmOrdersMap.contains(ist)) {
        for (dmOrder <- dmOrdersMap(ist)) {
          var deliveryQty = dmOrder._2

          if (dmDeliveryMap.contains(dmOrder._1)) {
            deliveryQty = deliveryQty + dmDeliveryMap(dmOrder._1)
          }

          dmDeliveryMap = dmDeliveryMap + (dmOrder._1 -> deliveryQty)
        }
      }

      if (onTheWayStockMap.contains(ist)) {
        for (onTheWayStock <- onTheWayStockMap(ist)) {
          var deliveryQty = onTheWayStock._2

          if (deliveryMap.contains(onTheWayStock._1)) {
            deliveryQty = deliveryQty + deliveryMap(onTheWayStock._1)
          }

          deliveryMap = deliveryMap + (onTheWayStock._1 -> deliveryQty)
        }
      }

      var i = 0
      orderD = 0
      futureD = 0
      order = dateRowList.head

      while (i < dateRowList.size) {

        // find the next order day
        while (i < dateRowList.size && !dateRowList(i).is_order_day) {
          lasti = i
          i = i + 1
        }
        if (i < dateRowList.size) {
          lasti = i
          order = dateRowList(i)
        }
        i = i + 1

        // actual stock on order day
        while (dateRowList(orderD).date_key < order.order_day) {

          dateRowList(orderD).dm_delivery = dmDeliveryMap.getOrElse(dateRowList(orderD).date_key, 0.0)
          dateRowList(orderD).order_delivery = deliveryMap.getOrElse(dateRowList(orderD).date_key, 0.0)

          currentStock = currentStock - dateRowList(orderD).predict_sales
          currentStock = currentStock + dateRowList(orderD).dm_delivery
          currentStock = currentStock + dateRowList(orderD).order_delivery

          dateRowList(orderD).day_end_stock_with_predict = currentStock

          dateRowList(orderD).dept_code = order.dept_code
          dateRowList(orderD).item_code = order.item_code
          dateRowList(orderD).sub_code = order.sub_code
          dateRowList(orderD).store_code = order.store_code
          dateRowList(orderD).con_holding = order.con_holding
          dateRowList(orderD).supplier_code = order.supplier_code
          dateRowList(orderD).rotation = order.rotation

          orderD = orderD + 1
        }

        // find delivery date of next order day
        var j = i + 1
        lastj = j
        while (j < dateRowList.size && !dateRowList(j).is_order_day) {
          lastj = j
          j = j + 1
        }

        lastj = j
        if (j < dateRowList.size) {
          val nextOderDeliver = dateRowList(j)
          deliveryDay = nextOderDeliver.delivery_day
          futureStock = currentStock
          order.start_stock = currentStock

          futureD = orderD
          order.matched_sales_start_date = dateRowList(futureD).date_key
          // expected stock on delivery day of next order
          while (dateRowList(futureD).date_key < deliveryDay) {
            futureStock = futureStock - dateRowList(futureD).predict_sales
            futureStock = futureStock + dmDeliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureStock = futureStock + deliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            order.matched_sales_end_date = dateRowList(futureD).date_key
            futureD = futureD + 1
          }

          if ("AfterStoreOpen".equalsIgnoreCase(order.delivery_time)) {
            futureStock = futureStock - dateRowList(futureD).predict_sales
            futureStock = futureStock + dmDeliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureStock = futureStock + deliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            order.matched_sales_end_date = dateRowList(futureD).date_key
            futureD = futureD + 1
          }

          order.future_stock = futureStock

          if (futureStock < 0) {
            futureStock = 0.0
          }


          if (futureStock < nextOderDeliver.minimum_stock_required) {
            var orderQty = nextOderDeliver.minimum_stock_required - futureStock
            order.order_without_pcb = orderQty

            var pcb = order.pcb
            if (order.supplier_code.equalsIgnoreCase("KSSE")
              || order.supplier_code.equalsIgnoreCase("KXS1")) {
              pcb = 1
            }

            if (orderQty < pcb) {
              orderQty = pcb
            } else if (orderQty % pcb > 0) {
              orderQty = math.ceil(orderQty / pcb).intValue() * pcb
            }

            order.order_qty = orderQty.intValue()
            val deliveryQty = orderQty + deliveryMap.getOrElse(order.delivery_day, 0.0)

            deliveryMap = deliveryMap + (order.delivery_day -> deliveryQty)
          }

        }

        j = j + 1
      }

      if (modelRun.isSimulation) {
        i = 0
        currentStock = stockLevelMap.getOrElse(ist, modelRun.defaultStockLevel)
        while (i < dateRowList.size) {

          currentStock = currentStock - dateRowList(i).actual_sales
          currentStock = currentStock + dmDeliveryMap.getOrElse(dateRowList(i).date_key, 0.0)
          currentStock = currentStock + deliveryMap.getOrElse(dateRowList(i).date_key, 0.0)

          if (currentStock < 0.0) {
            currentStock = 0.0
          }

          dateRowList(i).day_end_stock_with_actual = currentStock

          i = i + 1
        }
      }

    } catch {
      case ex: Exception => {

        val sb = new StringBuilder(ex.toString)
        sb.append("|")

        var cnt = 10
        for (te <- ex.getStackTrace) {
          sb.append(te).append("|")
          cnt = cnt - 1
        }

        order.error_info = List("futureD:", futureD.toString,
          "orderD:", orderD.toString,
          "deliveryDay:", deliveryDay,
          "futureStock:", futureStock,
          "lasti:", lasti,
          "lastj:", lastj,
          sb.toString()).mkString(" ")

        if (dateRowList.isEmpty) {
          dateRowList = order :: dateRowList
        }
      }
    }

    dateRowList
  }

  private def getMinimumStock(row: Row, isDcFlow: Boolean): Double = {

    if (isDcFlow) {
      return row.getAs[Double]("average_sales") * 15
    }

    var minumumStock: Double = 0

    try {
      val ittreplentyp = row.getAs[Integer]("ittreplentyp")

      if (ittreplentyp == 3) {
        minumumStock = row.getAs[Int]("ittminunit").doubleValue()

      } else if (ittreplentyp == 2 || ittreplentyp == 4) {
        val shelf_capacity = row.getAs[String]("shelf_capacity")
        if (null != shelf_capacity) {
          minumumStock = shelf_capacity.toDouble
        }
      }
    } catch {
      case ex: Exception => LogUtil.error("Fallback to use default stock", ex)

    } finally {
      if (minumumStock == 0) {
        val rotation = row.getAs[String]("rotation").trim()

        if (rotation.equalsIgnoreCase("A")) {
          minumumStock = 8
        } else if (rotation.equalsIgnoreCase("B")) {
          minumumStock = 6
        } else if (rotation.equalsIgnoreCase("X")) {
          minumumStock = 2
        }

      }

    }

    minumumStock
  }


}