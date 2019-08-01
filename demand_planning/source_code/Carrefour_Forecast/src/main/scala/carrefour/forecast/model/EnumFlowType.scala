package carrefour.forecast.model

object EnumFlowType {

  object FlowType extends Enumeration {
    type FlowType = Value

    val OnStockStore = Value("OnStock")
    val XDocking = Value("XDocking")
    val DC = Value("DC")
  }

}
