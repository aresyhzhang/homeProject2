import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import TimeUtils.stringToTimestamp

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
//
//    val ints = new ArrayBuffer[Int]()
//    ints.append(1)
//    runByName(ints)
//    runbyValue(ints)
//    println(ints.mkString(","))

    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time: Date = cal.getTime
    println(new SimpleDateFormat("yyyy-MM-dd").format(time))
    println(stringToTimestamp(new SimpleDateFormat("yyyy-MM-dd").format(time) + " 00:00:00"))
    println(stringToTimestamp(new SimpleDateFormat("yyyy-MM-dd").format(time) + " 00:00:00")+86400000)
    println(time)

  }


  def runByName(arr: =>ArrayBuffer[Int])={
   arr.append(2)
  }

  def runbyValue(arr:ArrayBuffer[Int])=
  {
    arr.append(3)

  }

}
