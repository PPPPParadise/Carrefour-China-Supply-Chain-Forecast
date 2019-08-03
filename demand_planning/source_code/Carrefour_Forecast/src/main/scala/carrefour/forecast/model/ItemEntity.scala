package carrefour.forecast.model

/**
  * A case class used as key to group data for processing
  * 数据主键，用于将数据正确分组进行运算
  *
  * @constructor create a item store key
  * @param item_id     Item ID in integer format / 数字格式 Item ID
  * @param sub_id      Sub ID in integer format / 数字格式 Sub ID
  * @param entity_code Store Code or Con Hondling in string / format 文本格式 Store Code
  */
case class ItemEntity(
                       item_id: Integer,
                       sub_id: Integer,
                       entity_code: String,
                       is_dc_flow: Boolean
                     ) extends Serializable