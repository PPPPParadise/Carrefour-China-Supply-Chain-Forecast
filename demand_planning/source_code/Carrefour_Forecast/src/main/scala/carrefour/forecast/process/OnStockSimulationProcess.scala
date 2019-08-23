package carrefour.forecast.process

import carrefour.forecast.core.ProcessLogic
import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.ModelRun
import carrefour.forecast.util.Utils

/**
  * Simulation process for on stock store order
  * On Stock 商品门店订单模拟运行
  */
object OnStockSimulationProcess {

  def main(args: Array[String]): Unit = {
    var runDate = args(0)
    val flowType = FlowType.OnStockStore
    val defaultStockLevel = 0.0
    val defaultDeliveryTime = "AfterStoreOpen"

    var item_id = 0
    var sub_id = 0
    var isDebug = false
    var debugTable = ""
    var store_code = ""

    for (i <- 1 until args.length) {
      val argPair = args(i).split("=")

      if (argPair(0).equalsIgnoreCase("day_shift")) {
        val dayShift = Integer.valueOf(argPair(1))
        runDate = Utils.getRunDate(runDate, dayShift)
      }

      if (argPair(0).equalsIgnoreCase("item_id")) {
        item_id = Integer.valueOf(argPair(1))
      }

      if (argPair(0).equalsIgnoreCase("sub_id")) {
        sub_id = Integer.valueOf(argPair(1))
      }

      if (argPair(0).equalsIgnoreCase("store_code")) {
        store_code = argPair(1)
      }

      if (argPair(0).equalsIgnoreCase("debug_table")) {
        isDebug = true
        debugTable = argPair(1)
      }

      if (argPair(0).equalsIgnoreCase("help")) {
        throw new IllegalArgumentException("Sample input:  item_id=6690 sub_id=5324 store_code=101 ")
      }
    }

    val onstockRun = ModelRun(runDate,
      flowType,
      "vartefact.forecast_onstock_orders",
      "vartefact.forecast_onstock_orders_hist",
      "tmp_in_scope_onstock_item_store",
      defaultStockLevel,
      defaultDeliveryTime,
      item_id,
      sub_id,
      store_code,
      false,
      isDebug,
      debugTable,
      true)

    ProcessLogic.process(onstockRun)
  }

}
