package carrefour.forecast.process

import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.ModelRun

object XDockingSimulationProcess {

  def main(args: Array[String]): Unit = {

    val runDate = args(0)
    val flowType = FlowType.XDocking
    val defaultStockLevel = 0.0
    val defaultSafetyStockLevel = 2.0
    val defaultDeliveryTime = "AfterStoreOpen"

    var item_id = 0
    var sub_id = 0
    var store_code = ""
    var isDebug = false
    var debugTable = ""

    for (i <- 1 until args.length) {
      val argPair = args(i).split("=")

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
          "store_code=101 debugTable=vartefact.forecast_debug")
      }
    }

    val onstockRun = ModelRun(runDate,
      flowType,
      "vartefact.forecast_xdock_orders",
      "vartefact.forecast_xdock_orders_hist",
      "in_scope_xdock_item_store",
      defaultStockLevel,
      defaultSafetyStockLevel,
      defaultDeliveryTime,
      item_id,
      sub_id,
      store_code,
      false,
      false,
      "",
      true)

    CoreProcess.process(onstockRun)
  }
}
