package carrefour.forecast.model

import carrefour.forecast.model.EnumFlowType.FlowType

case class ModelRun(
                     runDateStr: String,
                     flowType: FlowType.Value,
                     orderTableName: String,
                     orderHistTableName: String,
                     viewName: String,
                     defaultStockLevel: Double,
                     defaultDeliveryTime: String,
                     itemId: Integer,
                     subId: Integer,
                     storeCode: String,
                     isDcFlow: Boolean,
                     isDebug: Boolean,
                     debugTableName: String,
                     isSimulation: Boolean = false
                   ) extends Serializable


