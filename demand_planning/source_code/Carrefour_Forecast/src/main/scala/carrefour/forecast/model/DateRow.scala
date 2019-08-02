package carrefour.forecast.model

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
                    predict_sales: Double,

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
                    var ittreplentyp: Integer = 0,
                    var shelf_capacity: String = "",
                    var ittminunit: Integer = 0,
                    var minStock: Double =0,
                    var maxStock: Double =0,


                    // For Simulation
                    var actual_sales: Double = 0.0,
                    var day_end_stock_with_actual: Double = 0,

                    var error_info: String = ""
                  ) extends Serializable
