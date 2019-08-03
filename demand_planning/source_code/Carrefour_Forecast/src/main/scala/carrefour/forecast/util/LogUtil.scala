package carrefour.forecast.util

import java.text.SimpleDateFormat
import java.util.Date

object LogUtil {
  def error(message: String, ex: Exception): Unit = {
    info(message)
    print(ex)
    print("\n")
  }

  def info(message: String): Unit = {
    print(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
    print(" ")
    print(message)
    print("\n")

  }

}
