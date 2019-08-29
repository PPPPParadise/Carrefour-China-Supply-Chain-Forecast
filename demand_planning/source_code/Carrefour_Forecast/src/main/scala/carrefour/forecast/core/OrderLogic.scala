package carrefour.forecast.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import carrefour.forecast.model.{DateRow, ItemEntity, ModelRun}
import org.apache.spark.sql.Row

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

/**
  * Logic to calculate order quantity for each order day
  * 计算每个订单日订货量的逻辑
  */
object OrderLogic {

  /**
    * Generate orders
    * 生成订单
    *
    * @param ist                   Item and store information 商品及门店信息
    * @param rows                  Input data rows. One row per day. 输入数据列。每个自然日对应一行
    * @param runDateStr            Run date in yyyyMMdd String format 文本格式的运行日期，为yyyyMMdd格式
    * @param modelRun              Job run information 脚本运行信息
    * @param stockLevelMap         Day end stock level 门店盘点库存量
    * @param dmOrdersMap           DM order quantity and delivery date / DM 订单订货量及其抵达日期
    * @param onTheWayStockMap      On the way order quantity and delivery date 在途订单订货量及其抵达日期
    * @param pastOrderForecastkMap Previously generated order forecast 过去生成的订单规划
    * @return Order information 订单信息
    */
  def generateOrder(ist: ItemEntity, rows: Iterator[Row], runDateStr: String,
                    modelRun: ModelRun,
                    stockLevelMap: Map[ItemEntity, Double],
                    dmOrdersMap: Map[ItemEntity, List[Tuple2[String, Double]]],
                    onTheWayStockMap: Map[ItemEntity, List[Tuple2[String, Double]]],
                    pastOrderForecastkMap: Map[ItemEntity, List[Tuple2[String, Double]]]): List[DateRow] = {

    var dateRowList = List.empty[DateRow]

    // For debug and tracking purpose
    var orderD = 0
    var futureD = 0
    var deliveryDay = ""
    var futureStock = 0.0
    var lasti = 0
    var lastj = 0

    val dateKeyFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(dateKeyFormat.parse(runDateStr))
    cal.add(Calendar.DATE, 14)
    val twoWeeksAfterRunDate = cal.getTime

    var order = getEmptyOrder(runDateStr, ist, modelRun.isDcFlow)

    try {

      var dateRowListBuffer = new ListBuffer[DateRow]()

      for (row <- rows) {
        dateRowListBuffer += mapDateRow(runDateStr, ist, row, modelRun.isSimulation, modelRun.isDcFlow)
      }
      dateRowList = dateRowListBuffer.toList.sortBy(_.date_key)(Ordering[String])

      var currentStock = stockLevelMap.getOrElse(ist, modelRun.defaultStockLevel)
      val dmDeliveryMap = getDmDeliveryMap(ist, dmOrdersMap)

      var deliveryMap: Map[String, Double] = getDeliveryMap(ist, onTheWayStockMap)

      val orderMap: Map[String, Double] = processPastDcOrders(ist, pastOrderForecastkMap)

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
          // expected stock on delivery day of order
          while (dateRowList(futureD).date_key < order.delivery_day) {
            futureStock = futureStock - dateRowList(futureD).max_predict_sales
            futureStock = futureStock + dmDeliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureStock = futureStock + deliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureD = futureD + 1
          }

          if (futureStock < 0) {
            futureStock = 0.0
          }

          // expected stock on delivery day of next order
          while (dateRowList(futureD).date_key < deliveryDay) {
            futureStock = futureStock - dateRowList(futureD).max_predict_sales
            futureStock = futureStock + dmDeliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureStock = futureStock + deliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            order.matched_sales_end_date = dateRowList(futureD).date_key
            futureD = futureD + 1
          }

          if ("AfterStoreOpen".equalsIgnoreCase(order.delivery_time)) {
            futureStock = futureStock - dateRowList(futureD).max_predict_sales
            futureStock = futureStock + dmDeliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            futureStock = futureStock + deliveryMap.getOrElse(dateRowList(futureD).date_key, 0.0)
            order.matched_sales_end_date = dateRowList(futureD).date_key
            futureD = futureD + 1
          }

          order.future_stock = futureStock

          var orderQty: Double = 0

          if (modelRun.isDcFlow) {
            val maxStockLevel = nextOderDeliver.average_sales * 24
            val minStockLevel = nextOderDeliver.average_sales * 12
            orderQty = order.order_qty.doubleValue()

            order.minStock = minStockLevel
            order.maxStock = maxStockLevel

            if (futureStock < minStockLevel) {
              // Below minimum. Need to order more
                orderQty = (maxStockLevel + minStockLevel) / 2 - futureStock
            }

            if (!isAfterTwoWeeks(order.order_day, twoWeeksAfterRunDate)
              && orderMap.contains(order.order_day)) {
              // Within 2 weeks and has previous forecast
              // Try to keep consistency
              orderQty = orderMap(order.order_day)

              if (futureStock + orderQty < minStockLevel) {
                // Past order quantity is not enough
                // Increase 5% box
                orderQty = changeOrderQtyWithConsistency(orderQty, 1.05, order)
              }

              if (futureStock + orderQty > maxStockLevel) {
                // Past order quantity is too much
                // Decrease 5% box
                orderQty = changeOrderQtyWithConsistency(orderQty, 0.95, order)
              }
            }

            order.order_without_pcb = orderQty
          } else {
            if (futureStock < nextOderDeliver.minimum_stock_required) {
              orderQty = nextOderDeliver.minimum_stock_required - futureStock
              order.order_without_pcb = orderQty
            }
          }

          if (orderQty > 0) {
            if (orderQty < order.pcb) {
              orderQty = order.pcb
            } else if (orderQty % order.pcb > 0) {
              orderQty = math.ceil(orderQty / order.pcb).intValue() * order.pcb
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
        var currentStockWithActual = stockLevelMap.getOrElse(ist, modelRun.defaultStockLevel)
        var currentStockWithPred = stockLevelMap.getOrElse(ist, modelRun.defaultStockLevel)

        while (i < dateRowList.size) {

          val dmDelivery = dmDeliveryMap.getOrElse(dateRowList(i).date_key, 0.0)
          val orderDelivery = deliveryMap.getOrElse(dateRowList(i).date_key, 0.0)

          currentStockWithActual = currentStockWithActual - dateRowList(i).actual_sales + dmDelivery + orderDelivery
          if (currentStockWithActual < 0.0) {
            currentStockWithActual = 0.0
          }
          dateRowList(i).day_end_stock_with_actual = currentStockWithActual

          currentStockWithPred = currentStockWithPred - dateRowList(i).predict_sales + dmDelivery + orderDelivery
          if (currentStockWithPred < 0.0) {
            currentStockWithPred = 0.0
          }
          dateRowList(i).day_end_stock_with_predict = currentStockWithPred

          dateRowList(i).dept_code = order.dept_code
          dateRowList(i).item_code = order.item_code
          dateRowList(i).sub_code = order.sub_code
          dateRowList(i).store_code = order.store_code
          dateRowList(i).con_holding = order.con_holding
          dateRowList(i).supplier_code = order.supplier_code
          dateRowList(i).rotation = order.rotation
          dateRowList(i).ittreplentyp = order.ittreplentyp

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


  /**
    * Generate empty orer
    * 生成空订单
    *
    * @param runDateStr Run date in yyyyMMdd String format 文本格式的运行日期，为yyyyMMdd格式
    * @param ist        Item and store information 商品及门店信息
    * @param isDcFlow   Whether it is DC flow 是否为计算DC/货仓订单
    * @return Empty order 空订单
    */
  private def getEmptyOrder(runDateStr: String, ist: ItemEntity, isDcFlow: Boolean): DateRow = {

    if (isDcFlow) {
      DateRow(runDateStr, "", ist.item_id, ist.sub_id, "", "", ""
        , ist.entity_code, "", "", "", 0.0, "", "", "", 0.0, 0.0, 1
      )
    } else {
      DateRow(runDateStr, "", ist.item_id, ist.sub_id, "", "", ""
        , "", ist.entity_code, "", "", 0.0, "", "", "", 0.0, 0.0, 1
      )
    }
  }

  /**
    * Read information from input data row
    * 从输入数据列中读取数据
    *
    * @param runDateStr   Run date in yyyyMMdd String format 文本格式的运行日期，为yyyyMMdd格式
    * @param ist          Item and store information 商品及门店信息
    * @param row          Input data row  输入数据列
    * @param isSimulation Whether it is simulation run 是否为模拟运行
    * @param isDcFlow     Whether it is DC flow 是否为计算DC/货仓订单
    * @return Daily information for order logic 用于订单逻辑的每日数据
    */
  private def mapDateRow(runDateStr: String, ist: ItemEntity, row: Row, isSimulation: Boolean,
                         isDcFlow: Boolean): DateRow = {

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

      row.getAs[String]("order_date"),
      row.getAs[String]("delivery_date"),

      row.getAs[Double]("max_predict_sales"),
      row.getAs[Double]("predict_sales"),
      row.getAs[Integer]("qty_per_box")
    )

    if (null != dateRow.order_day) {
      dateRow.is_order_day = true
      if (isDcFlow) {
        dateRow.average_sales = row.getAs[Double]("average_sales")
        dateRow.minimum_stock_required = dateRow.average_sales
      } else {
        dateRow.ittreplentyp = row.getAs[Integer]("ittreplentyp")
        dateRow.ittminunit = row.getAs[Integer]("ittminunit")
        dateRow.shelf_capacity = row.getAs[String]("shelf_capacity")
        dateRow.minimum_stock_required = getMinimumStoreStock(row)
      }
    }

    if (isDcFlow) {
      dateRow.con_holding = ist.entity_code
    } else {
      dateRow.store_code = ist.entity_code
    }

    if (isSimulation) {
      dateRow.actual_sales = row.getAs[Double]("actual_sales")
    }

    dateRow
  }

  /**
    * Get minimum required store stock level
    * 获取最低门店库存要求
    *
    * @param row Input data row.  输入数据列
    * @return Minimum required stock level 最低门店库存要求
    */
  private def getMinimumStoreStock(row: Row): Double = {
    var minumumStock: Double = 1

    val ittreplentyp = row.getAs[Integer]("ittreplentyp")
    if (ittreplentyp == 3) {
      minumumStock = row.getAs[Int]("ittminunit").doubleValue()
    } else if (ittreplentyp == 2 || ittreplentyp == 4) {
      val shelf_capacity = row.getAs[String]("shelf_capacity")
    }

    minumumStock
  }


  /**
    * Get DM orders for this item and store
    * 获取当前商品和门店的DM订单
    *
    * @param ist         Item and store information 商品及门店信息
    * @param dmOrdersMap DM order quantity and delivery date / DM 订单订货量及其抵达日期
    * @return DM order quantity and delivery date / DM 订单订货量及其抵达日期
    */
  private def getDmDeliveryMap(ist: ItemEntity,
                               dmOrdersMap: Map[ItemEntity, List[Tuple2[String, Double]]]): Map[String, Double] = {
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

    dmDeliveryMap
  }

  /**
    * Get on the way order for this item and store
    * 获取当前商品和门店的在途订单
    *
    * @param ist              Item and store information 商品及门店信息
    * @param onTheWayStockMap On the way order quantity and delivery date 在途订单订货量及其抵达日期
    * @return On the way order quantity and delivery date 在途订单订货量及其抵达日期
    */
  private def getDeliveryMap(ist: ItemEntity,
                             onTheWayStockMap: Map[ItemEntity, List[Tuple2[String, Double]]]): Map[String, Double] = {
    var deliveryMap: Map[String, Double] = Map()

    if (onTheWayStockMap.contains(ist)) {
      for (onTheWayStock <- onTheWayStockMap(ist)) {
        var deliveryQty = onTheWayStock._2

        if (deliveryMap.contains(onTheWayStock._1)) {
          deliveryQty = deliveryQty + deliveryMap(onTheWayStock._1)
        }

        deliveryMap = deliveryMap + (onTheWayStock._1 -> deliveryQty)
      }
    }
    deliveryMap
  }

  /**
    * Get past DC order for this item and con holding
    * 获取当前商品及供应商的过去生成的订单
    *
    * @param ist             Item and con holding information 商品及供应商信息
    * @param pastDcOrdersMap Past DC order results 过去生成的货仓订单
    * @return Past DC order information 过去生成的货仓订单信息
    */
  private def processPastDcOrders(ist: ItemEntity,
                                  pastDcOrdersMap: Map[ItemEntity, List[Tuple2[String, Double]]]): Map[String, Double] = {
    var orderMap: Map[String, Double] = Map()

    if (pastDcOrdersMap.contains(ist)) {
      for (pastDcOrder <- pastDcOrdersMap(ist)) {
        orderMap = orderMap + (pastDcOrder._1 -> pastDcOrder._2)
      }
    }

    orderMap
  }

  /**
    * Check if order day is within 2 weeks of run date
    * 
    *
    * @param orderDayStr Order day
    * @param twoWeeksAfterRunDate 2 weeks date in future
    * @return
    */
  private def isAfterTwoWeeks(orderDayStr: String, twoWeeksAfterRunDate: Date): Boolean = {
    val dateKeyFormat = new SimpleDateFormat("yyyyMMdd")
    val orderDate = dateKeyFormat.parse(orderDayStr)

    orderDate.after(twoWeeksAfterRunDate)
  }

  /**
    *
    * @param orderQty
    * @param factor
    * @param order
    * @return
    */
  private def changeOrderQtyWithConsistency(orderQty: Double, factor: Double, order: DateRow): Double = {

    val pastOrderBox = math.ceil(orderQty / order.qty_per_box)

    val newOrderBox = math.ceil(pastOrderBox * factor)

    if (newOrderBox < pastOrderBox) {
      if (order.qty_per_box == order.pcb) {
        // Already order per box
        return newOrderBox * order.qty_per_box
      } else if ( math.ceil(orderQty / order.pcb) == math.ceil(newOrderBox * order.qty_per_box / order.pcb)) {
        // New order qty is still in same pallet or layer
        return newOrderBox * order.qty_per_box
      }
    }
    orderQty
  }


}
