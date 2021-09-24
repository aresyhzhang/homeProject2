import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object TimeUtils {
  /**
    *
    * @param timestamp 传入的时间戳，单位毫秒
    * @param format
    * @return 默认返回"yyyy-MM-dd HH:mm:ss" 的时间字符串
    */
  def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val timeFormat = new SimpleDateFormat(format)
    val timeStr = timeFormat.format(new Date(timestamp))
    timeStr
  }

  /**
    *
    * @param timeStr 传入"yyyy-MM-dd HH:mm:ss"的字符串格式
    * @param format
    * @return 返回时间戳，单位：毫秒
    */
  def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
    val timeFormat = new SimpleDateFormat(format)
    val timestamp = timeFormat.parse(timeStr).getTime
    timestamp
  }

  /**
   *
   * @param timeStr 传入"yyyy-MM-dd HH:mm:ss"的字符串格式
   * @param format
   * @return 返回时间戳，单位：毫秒
   */
  def stringToString(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss",outFormat:String="yyyy-MM-dd HH:mm:ss") = {
    val inputTimeStamp: Long = stringToTimestamp(timeStr)
    timestampToString(inputTimeStamp,outFormat)
  }

}
