package carrefour.forecast.process

import carrefour.forecast.model.EnumFlowType.FlowType
import carrefour.forecast.model.ModelRun

object OnStockSimulationProcess {

  def main(args: Array[String]): Unit = {
    val runDate = args(0)
    val flowType = FlowType.OnStockStore
    val defaultStockLevel = 0.0
    var defaultSafetyStockLevel = 8.0
    val defaultDeliveryTime = "AfterStoreOpen"

    var item_id = 0
    var sub_id = 0
    var store_code = ""

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

      if (argPair(0).equalsIgnoreCase("safetyStockLevel")) {
        defaultSafetyStockLevel = argPair(1).toDouble
      }

      if (argPair(0).equalsIgnoreCase("help")) {
        throw new IllegalArgumentException("Sample input:  item_id=6690 sub_id=5324 store_code=101 safetyStockLevel=3")
      }
    }

    val onstockRun = ModelRun(runDate,
      flowType,
      "vartefact.forecast_onstock_orders",
      "vartefact.forecast_onstock_orders_hist",
      "in_scope_onstock_item_store",
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
