package carrefour.forecast.process

import carrefour.forecast.core.ProcessLogic
import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.ModelRun
import carrefour.forecast.util.Utils

/**
  * Simulation process
  * 订单模拟运行
  */
object SimulationProcess {

  def main(args: Array[String]): Unit = {
    var runDate = args(0)
    val defaultStockLevel = 0.0
    val defaultDeliveryTime = "AfterStoreOpen"
    var item_id = 0
    var sub_id = 0
    var store_code = ""
    var isDebug = false
    var debugTable = ""

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
        throw new IllegalArgumentException("Sample input:  item_id=6690 sub_id=5324 " +
          "con_holding=101 debugTable=vartefact.forecast_debug")
      }
    }

    val onstockRun = ModelRun(runDate,
      FlowType.OnStockStore,
      "vartefact.forecast_onstock_orders",
      "vartefact.forecast_onstock_orders_hist",
      "tmp_in_scope_onstock_item_store",
      defaultStockLevel,
      defaultDeliveryTime,
      item_id,
      sub_id,
      store_code,
      false,
      false,
      "",
      true)

    ProcessLogic.process(onstockRun)

    val xdockRun = ModelRun(runDate,
      FlowType.XDocking,
      "vartefact.forecast_xdock_orders",
      "vartefact.forecast_xdock_orders_hist",
      "tmp_in_scope_xdock_item_store",
      defaultStockLevel,
      defaultDeliveryTime,
      item_id,
      sub_id,
      store_code,
      false,
      false,
      "",
      true)

    ProcessLogic.process(xdockRun)

    val dcRun = ModelRun(runDate,
      FlowType.DC,
      "vartefact.forecast_dc_orders",
      "vartefact.forecast_dc_orders_hist",
      "tmp_in_scope_dc_item_store",
      defaultStockLevel,
      defaultDeliveryTime,
      item_id,
      sub_id,
      "",
      true,
      false,
      "",
      true)

    ProcessLogic.process(dcRun)
  }

}
