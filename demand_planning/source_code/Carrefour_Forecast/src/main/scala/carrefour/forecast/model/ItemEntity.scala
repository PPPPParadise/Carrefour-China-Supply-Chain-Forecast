package carrefour.forecast.model

/**
  * Data model used as key to group data for processing<br />
  * 用于将数据正确分组的数据模型
  *
  * @constructor create a item store key
  * @param item_id     Item ID in integer format 数字格式 Item ID
  * @param sub_id      Sub ID in integer format 数字格式 Sub ID
  * @param entity_code Store Code or Con Holding in string format 文本格式 Store Code
  * @param is_dc_flow Whether it is DC flow 是否为计算大仓订单
  */
case class ItemEntity(
                       item_id: Integer,
                       sub_id: Integer,
                       entity_code: String,
                       is_dc_flow: Boolean
                     ) extends Serializable