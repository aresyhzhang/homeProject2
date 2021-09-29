import java.io.InputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object test {
  def main(args: Array[String]): Unit = {
//    val pattern = new Regex("/(?<=\\().*(?=\\))/g")  // 首字母可以是大写 S 或小写 s
//    val str = "Scala is scalable and cool"
//    val value = "(sdfas)"
//    println(pattern.findFirstIn(value).get)

//    val stream : InputStream = this.getClass.getResourceAsStream("es2hive/estest2hivetest.txt")
//    val lines = scala.io.Source.fromInputStream( stream ).getLines
//    while (lines.hasNext){
//      println(lines.next())
//    }
//    println(lines)

    val ints = new ArrayBuffer[Int]()
    ints.append(1)
    runByName(ints)
    runbyValue(ints)
    println(ints.mkString(","))

  }


  def runByName(arr: =>ArrayBuffer[Int])={
   arr.append(2)
  }

  def runbyValue(arr:ArrayBuffer[Int])=
  {
    arr.append(3)

  }

}
