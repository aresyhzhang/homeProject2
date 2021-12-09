import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TimeUtils {
  /**
   *
   * @param timestamp 传入的时间戳，单位毫秒
   * @param format 输出日期格式
   * @return 默认返回"yyyy-MM-dd HH:mm:ss" 的时间字符串
   */
  def timestampToString(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val timeFormat = new SimpleDateFormat(format)
    val timeStr: String = timeFormat.format(new Date(timestamp))
    timeStr
  }

  /**
   *
   * @param timeStr 日期
   * @param format 输入日期格式
   * @return 返回时间戳，单位：毫秒
   */
  def stringToTimestamp(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    val timeFormat = new SimpleDateFormat(format)
    val timestamp: Long = timeFormat.parse(timeStr).getTime
    timestamp
  }

  /**
   *
   * @param timeStr 日期
   * @param format 输入格式
   * @param outFormat 输出格式
   * @return 返回时间戳，单位：毫秒
   */
  def stringToString(timeStr: String, format: String = "yyyy-MM-dd HH:mm:ss",outFormat:String="yyyy-MM-dd HH:mm:ss"): String = {
    val inputTimeStamp: Long = stringToTimestamp(timeStr)
    timestampToString(inputTimeStamp,outFormat)
  }

  /**
   * 获取0点时间戳
   * @param dayAmount 日期偏移量
   * @return 0点时间戳
   */
  def getZeroTimeByAmount(dayAmount:Int=0): Long ={
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, dayAmount)
    val time = cal.getTime
    stringToTimestamp(new SimpleDateFormat("yyyy-MM-dd").format(time) + " 00:00:00")
  }



}
