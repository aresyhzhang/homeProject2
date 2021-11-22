import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @ Author     ：aresyhzhang
 * @ Date       ：Created in 14:12 2021/11/22
 * @ Description：
 */
object newEs2Hive {

  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
      sparkBuilder.master("local[*]")
    val spark = sparkBuilder.appName(this.getClass.getName)
//      .config("hive.exec.dynamici.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nostrick")
//      .enableHiveSupport()
      .getOrCreate()

    val array_str="maptest,maptest.a1"
    val options = Map(
      "pushdown" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "192.168.126.129",
      "es.port" -> "9200",
      "es.mapping.id" -> "id",
      "es.read.field.as.array.include"->array_str
    )

    val df = spark.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load("test/_doc")

    val df2: DataFrame = df.withColumn("maptest", explode(col("maptest")))
    df2.printSchema()
    df2.selectExpr("explode(maptest.a1) as a1")
      .show(false)

    df2
      .printSchema()

//    df.printSchema()

//    df.show(false)

    spark.stop()

  }
}
