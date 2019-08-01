package carrefour.forecast.util
import java.util.Date
import java.text.SimpleDateFormat

object LogUtil {
  def info(message: String): Unit = {
    print(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()))
    print(" ")
    print(message)
    print("\n")

  }

  def error(message: String, ex: Exception): Unit = {
    info(message)
    print(ex)
    print("\n")
  }

}
