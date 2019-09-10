package carrefour.forecast.model

/**
  * Data model used for flow type<br />
  * 用于储存物流模式的数据模型
  */

object EnumFlowType {

  /**
    * Flow type of job run. There are 3 possible values.<br />
    * 1. OnStock - Store order for on stock items<br />
    * 2. XDocking - Store order for cross docking items<br />
    * 3. DC - DC order<br />
    * 脚本运行对应的物流模式。有如下三种可能值<br />
    * 1. OnStock - 存库商品门店订单<br />
    * 2. XDocking - 越库商品门店订单<br />
    * 3. DC - 大仓订单
    */
  object FlowType extends Enumeration {
    type FlowType = Value

    val OnStockStore = Value("OnStock")
    val XDocking = Value("XDocking")
    val DC = Value("DC")
  }

}
