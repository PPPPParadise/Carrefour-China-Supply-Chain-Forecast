package carrefour.forecast.model

object EnumFlowType {

  /**
    * Flow type of job run. OnStock, XDocking, Or DC
    * 脚本运行对应的Flow Type。
    */
  object FlowType extends Enumeration {
    type FlowType = Value

    val OnStockStore = Value("OnStock")
    val XDocking = Value("XDocking")
    val DC = Value("DC")
  }

}
