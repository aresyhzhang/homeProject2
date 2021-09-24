import java.util

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkContextFunctions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ES2Hive {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","E:\\myconf")

//    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
//    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("test")
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
//    conf.set("hive.metastore.uris","thrift://192.168.126.129:9083")
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder().appName("RddToDataFrame")
      .master("local")
      .config("hive.metastore.uris","thrift://192.168.126.129:9083")
      .config("fs.defaultFS","hdfs://192.168.126.129:9000/")
      .enableHiveSupport()
      .getOrCreate()






    val query =
      s"""
         |{
         |    "query":{"match_all":{}},
         |    "_source":["first_name"]  //chatMessages是所需查询的字段，貌似没啥用
         |}
       """
        .stripMargin
    //这里的索引类型要与ES中的 【_index,_type】一致，不然会报错

    //id string,buffer(payment string,name string),
//    val columnList="id string,order map(payment string,name string)"
    val columnList="first_name string,createdtime long,maptest map(id string|name string)"
    val explodeMap=true
    val hiveTableName="test_table"
    val partitionField="createdtime"
    val dateFormatter="timestamp"
    val partitionDateFormatter="yyyy-MM-dd"
    val hivePartitionName="dt"


//    if(columnList.contains("map(")){
//      if(explodeMap){
//        val array: Array[String] = mapType.split('|')
//        for (elem <- array) {
//          addStructType(elem.split(" ")(0),elem.split(" ")(1),structTypeArray)
//        }
//    }

    val columnArray: Array[String] = columnList.split(',')
    val structTypeArray: ArrayBuffer[StructField] = ArrayBuffer[StructField]()

    //用来存数据的key
    val resultMap = new mutable.LinkedHashMap[AnyRef, AnyRef]()

    for (elem <- columnArray) {
      val elemArray: Array[String] = elem.split(" ")
      val columnName: String = elemArray(0)
      val columnType: String = elemArray(1)
      //处理特殊map类型
      if(columnType.contains("map")){
        val mapType: String = elem.split('(').last.dropRight(1)
        if(explodeMap){
          val array: Array[String] = mapType.split('|')
          for (elem <- array) {
            val mapArray: Array[String] = elem.split(" ")
            val mapKey: String = mapArray(0)
            val mapValue: String = mapArray(1)
            addStructType(mapKey,mapValue,structTypeArray)
            resultMap.put(mapKey,mapValue)
          }
        }else{
          addStructType(columnName,"string",structTypeArray)
          resultMap.put(columnName,null)
        }
      }else{
        resultMap.put(columnName,null)
        addStructType(columnName,columnType,structTypeArray)
      }
    }

    def  addStructType(inputName:String,inputType:String,inputArr: ArrayBuffer[StructField],isNull:Boolean=true)={
      inputType match {
         case "string"=>inputArr.append(StructField(inputName,StringType,isNull))
         case "int"=>inputArr.append(StructField(inputName,IntegerType,isNull))
         case "long"=>inputArr.append(StructField(inputName,LongType,isNull))
         case "double"=>inputArr.append(StructField(inputName,DoubleType,isNull))
         case "float"=>inputArr.append(StructField(inputName,FloatType,isNull))
         case "boolean"=>inputArr.append(StructField(inputName,BooleanType,isNull))
         case _=>inputArr.append(StructField(inputName,StringType,isNull))
       }
    }


    //需要处理给定的字段很多，但是es部分字段缺失的情况。

    //rdd - df - ds
    val data: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(sc,"test/testtype")

    //构建schema
    structTypeArray.append(StructField(hivePartitionName,StringType,nullable = true))
    val schema: StructType = StructType(structTypeArray)

    val mapRdd = data.map({ x => {
      val id = x._1
      val value: collection.Map[String, AnyRef] = x._2

      //使用LinkedHashMap来存储
      val map1: mutable.LinkedHashMap[AnyRef, AnyRef] = resultMap.clone()

      val array = new ArrayBuffer[AnyRef]()

      var partitionValue=""
      for (elem <- value)
      {
        val columnName = elem._1
        val columnValue = elem._2

        //获取hive分区字段值
        if(columnName.equalsIgnoreCase(partitionField)){
          partitionValue = dateFormatter match {
            case "timestamp" => TimeUtils.timestampToString(columnValue.asInstanceOf[Long], partitionDateFormatter)
            case _ => TimeUtils.stringToString(columnValue.asInstanceOf[String], partitionDateFormatter)
          }
          map1.put(hivePartitionName,partitionValue)
        }
//        if(columnValue.getClass.getName.contains("collection")){
        columnValue match {
          case list: Seq[AnyRef] =>
            for (elem <- list) {
              val map: mutable.LinkedHashMap[AnyRef, AnyRef] = elem.asInstanceOf[mutable.LinkedHashMap[AnyRef, AnyRef]]
              //判断是否炸开map，否则将map转为单个字符串存入
              if(explodeMap){
                for (elem <- map) {
                  map1.put(elem._1,elem._2)
                  array.append(elem._2)
                }
              }else{
                //不炸开map会把map单独作为一个json字符串写入数据库
                println(map.toString())
//                val str: String = JSON.toJSONString(map)
//                println(str)
              }

            }

            //非map类型正常处理
          case _ => {
            map1.put(columnName,columnValue)
            array.append(columnValue)
          }

        }
      }
      //追加分区字段
      array.append(partitionValue)
      val array1: Array[AnyRef] = map1.values.toArray
      val row1: Row = Row.fromSeq(array1)
      row1
//      println()

//      val row: Row = Row.fromSeq(array)
//      row
    }
    })

  mapRdd.foreach(println(_))

    val df: DataFrame = spark
      .createDataFrame(mapRdd, schema)

   df.write.mode("append").insertInto(hiveTableName)


    df.show()


//    mapRdd.foreach(println(_))

    //      data.foreach(println(_));
    //    data.collect().foreach(println(_));

    //    EsSpark.saveToEs(read,"index1/type1",Map("es.mapping.id" -> "id")) //写入


//
//      val rowRDD = sparkSession.sparkContext
//        .textFile("/tmp/people.txt",2)
//        .map( x => x.split(",")).map( x => Row(x(0),x(1).trim().toInt))
//      sc.createDataFrame(data,schema)


//      data.foreach(println(_));
//    data.collect().foreach(println(_));

//    EsSpark.saveToEs(read,"index1/type1",Map("es.mapping.id" -> "id")) //写入
    sc.stop()

  }


}
