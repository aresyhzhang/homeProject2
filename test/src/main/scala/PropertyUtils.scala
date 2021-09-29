import java.io.InputStream
import java.util.{Objects, Properties}

object PropertyUtils {
  /**
   * 读取配置文件
   * @param fileName 文件名
   * @return Properties
   */
  def getFileProperties(fileName: String): Properties = {
    val prop = new Properties()
    val inputStream: InputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    if (Objects.isNull(inputStream)) {
      throw new Exception(s"can not read $fileName")
    }
    prop.load(inputStream)
    prop
  }
}
