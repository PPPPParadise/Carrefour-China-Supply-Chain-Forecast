package carrefour.forecast.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Utility for job run<br />
  * 用于脚本运行的工具函数
  */
object Utils {

  /**
    * Calculate real job run date<br />
    * 获取真实运行时间
    * @param runDateStr Input run date 输入的运行时间
    * @param dayShift Add or remove days 需要增加或者减少的天数
    * @return Job run date 真实运行时间
    */
  def getRunDate(runDateStr: String, dayShift: Int): String ={
    val dateKeyFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    val runDate = dateKeyFormat.parse(runDateStr)
    cal.setTime(runDate)
    cal.add(Calendar.DATE, dayShift)
    val newRunDate = cal.getTime
    dateKeyFormat.format(newRunDate)
  }

}
