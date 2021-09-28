
import java.util.Properties

import scala.io.{BufferedSource, Source}

object PropertyUtils {

  //读取resources下的文件并且返回
  /**
   * 获取配置文件
   *
   * @return
   */
  def getFileProperties(fileName: String, propertyKey: String):String = {
    val source: BufferedSource = Source.fromURL(getClass.getResource("/" + fileName))
    val lines_source: Seq[String] = source.getLines.toSeq
    val properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split: Array[String] = elem.split("==")
      if(split.length>1){
        val key: String = split(0)
        val value: String = split(1)
        properties.setProperty(key,value)
      }
    }
    source.close()
    properties.getProperty(propertyKey)
  }
}
