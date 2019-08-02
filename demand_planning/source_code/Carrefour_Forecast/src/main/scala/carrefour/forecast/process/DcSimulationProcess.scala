package carrefour.forecast.process

import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.ModelRun

object DcSimulationProcess {

  def main(args: Array[String]): Unit = {
    val runDate = args(0)
    val flowType = FlowType.DC
    val defaultStockLevel = 0.0
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

      if (argPair(0).equalsIgnoreCase("con_holding")) {
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
      flowType,
      "vartefact.forecast_dc_orders",
      "vartefact.forecast_dc_orders_hist",
      "in_scope_dc_item_store",
      defaultStockLevel,
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
