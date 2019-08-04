package carrefour.forecast.config

/**
  * Database and table names for simulation
  * 用于模拟运行的数据库及表名
  */
object SimulationTables {

  /**
    * For latest orders from simulation process
    * 用于储存模拟运行生成的最新订单
    */
  val simulationOrdersTable = "vartefact.forecast_simulation_orders"

  /**
    * For all versions of orders for an order day
    * 用于储存对每个订货日生成的全部历史预测
    */
  val simulationOrdersHistTable = "vartefact.forecast_simulation_orders_hist"

  /**
    * For results of simulation
    * 用于储存模拟运行结果
    */
  val simulationResultTable = "vartefact.forecast_simulation_result"

  /**
    * For day end stock level calculated by simulation
    * 用于储存模拟运行过程中计算的每日门店盘点后库存
    */
  val simulationStockTable = "vartefact.forecast_simulation_stock"

  /**
    * For tracking item start and stop period
    * 用于储存商品停止订货日期
    */
  val simulationItemStatusTable = "vartefact.forecast_simulation_item_status"

}
