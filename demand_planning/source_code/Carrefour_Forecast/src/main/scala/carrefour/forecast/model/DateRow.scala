package carrefour.forecast.model

/**
  * A data model class used for order related information per day<br />
  * 用于储存每日订单相关信息的数据模型
  *
  * @param run_date Job run date 脚本运行时间
  * @param date_key Mapped date 对应的日期
  * @param item_id Item ID
  * @param sub_id Sub ID
  * @param dept_code Department Code
  * @param item_code 6 digit Item Code 六位格式的Item Code
  * @param sub_code Sub Code
  * @param con_holding Con Holding
  * @param store_code Store Code
  * @param supplier_code Supplier COde
  * @param rotation Rotation
  * @param pcb PCB
  * @param delivery_time Stock arrival time for store 订单送达门店的时间
  * @param order_day Order day 订单日
  * @param delivery_day Delivery day for this order day 订单日对应的送货日
  * @param predict_sales Predicted sales 预计销量
  * @param order_without_pcb Order quantity without PCB logic 不考虑PCB的订单量
  * @param order_qty Order Quantity 订单量
  * @param is_order_day Whether this date is order day 本日是否为订单日
  * @param minimum_stock_required Store minimum required stock level 门店最低库存量
  * @param average_sales DC Averaqge sales / 货仓平均销量
  * @param matched_sales_start_date Start day of matched sales days
  * @param matched_sales_end_date End day of matched sales days
  * @param start_stock Initial stock level
  * @param future_stock End stock level
  * @param dm_delivery Delivery quantity from DM order
  * @param order_delivery Delivery quantity from ordinary order
  * @param day_end_stock_with_predict Day end stock level with predicted sales
  * @param minStock Minimum stock level for DC
  * @param maxStock Maximum stock level for DC
  * @param actual_sales Actual sales
  * @param day_end_stock_with_actual Day end stock level with actual sales
  * @param error_info Error log
  */
case class DateRow(
                    run_date: String,
                    date_key: String,

                    item_id: Integer,
                    sub_id: Integer,
                    var dept_code: String,
                    var item_code: String,
                    var sub_code: String,

                    var con_holding: String,
                    var store_code: String,
                    var supplier_code: String,
                    var rotation: String,

                    pcb: Double,
                    delivery_time: String,

                    order_day: String,
                    delivery_day: String,
                    max_predict_sales: Double,
                    predict_sales: Double,
                    qty_per_box: Integer,

                    var order_without_pcb: Double = 0,
                    var order_qty: Integer = 0,
                    var is_order_day: Boolean = false,

                    // For store flow
                    var minimum_stock_required: Double = 0,

                    // For DC flow
                    var average_sales: Double = 0,

                    // For debug
                    var matched_sales_start_date: String = "",
                    var matched_sales_end_date: String = "",
                    var start_stock: Double = 0.0,
                    var future_stock: Double = 0.0,
                    var dm_delivery: Double = 0,
                    var order_delivery: Double = 0,
                    var day_end_stock_with_predict: Double = 0,
                    var minStock: Double = 0,
                    var maxStock: Double = 0,


                    // For Simulation
                    var actual_sales: Double = 0.0,
                    var day_end_stock_with_actual: Double = 0,

                    var error_info: String = ""
                  ) extends Serializable
