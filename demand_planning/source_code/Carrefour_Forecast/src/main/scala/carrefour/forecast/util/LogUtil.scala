package carrefour.forecast.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Utility for logging
  */
object LogUtil {

  /**
    * Record error log
    * 将错误信息写入日志
    *
    * @param message
    * @param ex
    */
  def error(message: String, ex: Exception): Unit = {
    info(message)
    print(ex)
    print("\n")
  }

  /**
    * Record info log
    * 将信息写入日志
    *
    * @param message
    */
  def info(message: String): Unit = {
    print(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
    print(" ")
    print(message)
    print("\n")

  }

}
