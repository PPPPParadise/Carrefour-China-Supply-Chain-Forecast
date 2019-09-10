package carrefour.forecast.config

/**
  * This class is used to store Database and table names for simulation<br />
  * 用于储存模拟运行需要的的数据库及表名
  */
object SimulationTables {

  /**
    * For latest orders from simulation DM order process<br />
    * 用于储存模拟运行生成的DM订单
    */
  val simulationDmOrdersTable = "vartefact.forecast_simulation_dm_orders"

  /**
    * For latest orders from simulation DM order process<br />
    * 用于储存模拟运行生成的DM订单
    */
  val simulationDmDcOrdersTable = "vartefact.forecast_simulation_dm_dc_orders"

  /**
    * For latest orders from simulation process<br />
    * 用于储存模拟运行生成的最新订单
    */
  val simulationOrdersTable = "vartefact.v_forecast_simulation_orders"

  /**
    * For all versions of orders for an order day<br />
    * 用于储存对每个订货日生成的全部历史预测
    */
  val simulationOrdersHistTable = "vartefact.forecast_simulation_orders_hist"

  /**
    * For results of simulation
    * 用于储存模拟运行结果
    */
  val simulationResultTable = "vartefact.forecast_simulation_result"

  /**
    * For day end stock level calculated by simulation<br />
    * 用于储存模拟运行过程中计算的每日门店盘点后库存
    */
  val simulationStockTable = "vartefact.v_forecast_simulation_stock"

}
