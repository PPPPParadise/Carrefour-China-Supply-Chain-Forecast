package carrefour.forecast.model

import carrefour.forecast.model.EnumFlowType.FlowType

/**
  *
  * @param runDateStr Job run date 脚本运行时间
  * @param flowType Flow Type
  * @param orderTableName Database and name for order table 订单表的数据库名及表名
  * @param orderHistTableName Database and name for order history table 历史订单预测表的数据库名及表名
  * @param viewName Temp view name used by job run 脚本运行时使用的临时数据库视图名
  * @param defaultStockLevel Default value for initial stock level 缺省当前库存值
  * @param defaultDeliveryTime Default value for delivery arrival time 缺省订单送达门店的时间
  * @param itemId Item ID
  * @param subId Sub ID
  * @param storeCode Store code
  * @param isDcFlow Whether it is DC flow 是否为计算DC/货仓订单
  * @param isDebug Whether it is debug process 是否为调试运行
  * @param debugTableName Database and table name for debug process 调试结果表的数据库名及表名
  * @param isSimulation Whether it is simulation process 是否为模拟运行
  */
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


