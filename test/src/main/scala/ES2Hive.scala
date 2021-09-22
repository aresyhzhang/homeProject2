import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkContextFunctions

object ES2Hive {

  def main(args: Array[String]): Unit = {



//    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
//    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
//    conf.set("cluster.name", "name")
    conf.set("es.nodes", "192.168.126.129")
    conf.set("es.port", "9200")
    conf.set("es.scroll.size", "10000")
    conf.set("spark.broadcast.compress", "true") // 设置广播压缩
    conf.set("spark.rdd.compress", "true") // 设置RDD压缩
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
    conf.set("spark.shuffle.file.buffer", "1280k")
    conf.set("spark.reducer.maxSizeInFlight", "1024m")
    conf.set("spark.es.nodes.wan.only", "true")
    conf.set("spark.reducer.maxMblnFlight", "1024m")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("index.mapper.dynamic","false")
    val sc=new SparkContext(conf)



      val spark = SparkSession.builder().appName("RddToDataFrame").master("local").getOrCreate()

    val query =
      s"""
         |{
         |    "query":{"match_all":{}},
         |    "_source":["first_name"]  //chatMessages是所需查询的字段，貌似没啥用
         |}
       """
        .stripMargin
    //这里的索引类型要与ES中的 【_index,_type】一致，不然会报错


    val data: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(sc,"test/testtype")

      val schema = StructType(
          Seq(
              StructField("name",StringType,true)
              ,StructField("age",IntegerType,true)
          )
      )

      val rowRDD = sparkSession.sparkContext
        .textFile("/tmp/people.txt",2)
        .map( x => x.split(",")).map( x => Row(x(0),x(1).trim().toInt))
      sc.createDataFrame(data,schema)

      data.foreach(println(_));
//    data.collect().foreach(println(_));

//    EsSpark.saveToEs(read,"index1/type1",Map("es.mapping.id" -> "id")) //写入
    sc.stop()

  }

}
