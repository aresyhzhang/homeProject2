import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * @ Author     ：aresyhzhang
 * @ Date       ：Created in 14:12 2021/11/22
 * @ Description：
 */
object newEs2Hive {

  def main(args: Array[String]): Unit = {
    println("scala main args is:"+args.mkString(","))
    val sparkBuilder = SparkSession.builder()
      sparkBuilder.master("local[*]")
    val spark: SparkSession = sparkBuilder.appName(this.getClass.getName)
//      .config("hive.exec.dynamici.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nostrick")
//      .enableHiveSupport()
      .getOrCreate()
        val esPropMap = new mutable.HashMap[String, String]()
        esPropMap.put("hiveDataBase",args(0))
        esPropMap.put("esNodes",args(2).split(":").head)
        esPropMap.put("esPort",args(2).split(":").last)
        esPropMap.put("startTime",args(3).split(",").head)
        esPropMap.put("endTime",args(3).split(",").last)
        val esPropArrary: Array[String] = args(1).split(',')
       val properties: Properties = PropertyUtils.getFileProperties(esProperties)
          import scala.collection.JavaConverters._
          esPropMap.put("esIndexName",esIndexName)
          properties.putAll(esPropMap.toMap.asJava)
          println("read properties is:"+properties)

    import spark.sql
    sql(
      s"""
         |create temporary table mytest
         |using org.elasticsearch.spark.sql
         |options (pushdown 'true',
         |resource 'order_test/_doc',
         |es.nodes.wan.only 'true',
         |es.nodes '49.234.117.228',
         |es.port '9200',
         |es.mapping.id 'id'
         |)
         |""".stripMargin)

    sql("select * from mytest").show(false)

//    val df2: DataFrame = df.withColumn("maptest", explode(col("maptest")))
//    df2.printSchema()
//    df2.selectExpr("explode(maptest.a1) as a1")
//      .show(false)
//
//    df2
//      .printSchema()

//    df.printSchema()

//    df.show(false)

    spark.stop()

  }
}
