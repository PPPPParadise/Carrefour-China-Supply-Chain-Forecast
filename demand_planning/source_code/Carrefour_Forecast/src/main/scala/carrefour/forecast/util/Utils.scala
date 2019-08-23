package carrefour.forecast.util

import java.text.SimpleDateFormat
import java.util.Calendar

object Utils {

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
