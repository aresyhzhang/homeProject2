import java.util

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object ES2Hive {

  def main(args: Array[String]): Unit = {
    //配置hadoop的环境变量,本地需要设置,线上不需要,在c盘的myconf目录下bin目录下放winUtils,下个hadoop.dll放到C:\Windows\System32
    System.setProperty("hadoop.home.dir","E:\\myconf")

    /**
     * 需要解决的问题：
     * 1.es的字段和hive的字段不一致，全部都不一致，还是先不读es的元数据，采用手动填写的方式吧
     * 2.es中的类型可能也和hive中的类型不一致,将会按照配置的es字段顺序和hive字段顺序去写入到es中。
     */
    //设置日志级别
    //        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    //
    //创建spark上下文
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("test")
    conf.set("es.nodes", "192.168.126.129")
    conf.set("es.port", "9200")
    conf.set("es.scroll.size", "10000") //滑动大小
    conf.set("spark.broadcast.compress", "true") // 设置广播压缩
    conf.set("spark.rdd.compress", "true") // 设置RDD压缩
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
    conf.set("spark.shuffle.file.buffer", "1280k")
    conf.set("spark.reducer.maxSizeInFlight", "1024m")
    conf.set("spark.es.nodes.wan.only", "false")
    conf.set("spark.reducer.maxMblnFlight", "1024m")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("index.mapper.dynamic","false")
    val sc=new SparkContext(conf)
    //创建spark上下文
    val spark = SparkSession.builder().appName("RddToDataFrame")
      //
      .master("local")
      /*    .config("hive.metastore.uris","thrift://192.168.1.102:9083") //设置hive元数据库的地址
          .config("fs.defaultFS","hdfs://hadoop102:8020") //设置hdfs  nameNode的地址
          .config("hive.exec.dynamici.partition",true)
          .config("hive.exec.dynamic.partition.mode","nonstrict")*/
//          .enableHiveSupport()
      .getOrCreate()

    //需要提取的es的字段
    val esColumnList="first_name string,createdtime long,maptest.id string,maptest.name string"
    //需要写入的hive的字段
    val hiveColumnList="first_name string,createdtime long,id string,name string"
    //可以忽略map
    val hiveTableName="index2"  //hive表的名称
    val es2HivePartitionName="ctime"  //分区字段
    val es2HivePartitionFormatter="timestamp"  //分区字段的日期格式
    val hivePartitionName="dt"  //hive中的分区字段
    val hivePartitionFormatter="yyyy-MM-dd"  //设置hive分区的日期格式
    val esIndexName="test"  //hive中的分区字段
    val esTypeName="testtype"  //hive中的分区字段
    //从args把参数传入
    val loadFilePath= "es2hive/estest2hivetest.txt"


    //根据给定的hive字段构建schema

    val hiveColumnArray: Array[String] = hiveColumnList.split(',')
    val esColumnArray: Array[String] = esColumnList.split(',')
    val hiveStructTypeArray: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
    //用来保存hive的字段名为一个map，后续遍历es时往里面填充数据 [esA,(hiveA,String,Integer)]
    val esMappingHiveNameMap = new mutable.LinkedHashMap[String, (String, String, String)]()
    val hiveDataMap = new mutable.LinkedHashMap[AnyRef, AnyRef]()

    //需要写入的hive的字段，两个数组的长度必须一样才行
    if(esColumnArray.length!=hiveColumnArray.length)throw new RuntimeException("esColumnList do not match hiveColumnList,Please check and try again")

    //同时开始遍历es和hive的列格式
     for(i <- hiveColumnArray.indices) {
     val hiveArray: Array[String] = hiveColumnArray(i).split(" ")
     val esArray: Array[String] = esColumnArray(i).split(" ")
     val hiveColumnName: String = hiveArray(0)
     val hiveColumnType: String = hiveArray(1)
     val esColumnName: String = esArray(0)
     val esColumnType: String = esArray(1)
     //用hive的列名和类型构建StructType,后面转成dataframe使用
     addStructType(hiveColumnName,hiveColumnType,hiveStructTypeArray)
     esMappingHiveNameMap.put(esColumnName,Tuple3(hiveColumnName,hiveColumnType,esColumnType))
       hiveDataMap.put(hiveColumnName,null)
    }

    def  addStructType(inputName:String,inputType:String,inputArr: ArrayBuffer[StructField],isNull:Boolean=true): Unit ={
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

    //构建hive表的schema
    //构建schema
    hiveStructTypeArray.append(StructField(hivePartitionName,StringType,nullable = true))
    val hiveTableSchema: StructType = StructType(hiveStructTypeArray)

    //从es中读取数据
    val esDataRDD: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(sc,esIndexName+"/"+esTypeName)


    val mapRdd = esDataRDD.map(
      { esData: (String, collection.Map[String, AnyRef]) => {
        val esValue: collection.Map[String, AnyRef] = esData._2
        //把之前准备好的数据格式clone来使用
        val es2HiveNameMap: mutable.LinkedHashMap[String, (String, String, String)] = esMappingHiveNameMap.clone()
        val hiveValueMap: mutable.LinkedHashMap[AnyRef, AnyRef] = hiveDataMap.clone()

        //针对hive的数据进行填充
        for (elem <- esValue) {

          val esColumnName: String = elem._1
          val esColumnValue: AnyRef = elem._2
          //开始处理es中集合类型的数据

          if (esColumnValue.isInstanceOf[Seq[AnyRef]]) {
            val jsonMap: mutable.LinkedHashMap[AnyRef, AnyRef] = elem.asInstanceOf[mutable.LinkedHashMap[AnyRef, AnyRef]]
            for (json <- jsonMap) {
              //es2HiveNameMap(maptest.a)
              val hiveName: String = es2HiveNameMap(esColumnName + "." + json._1)._1
              hiveValueMap.put(hiveName, json._2)
            }
          }
          //如果es的字段名=hive的分区字段名，就要去当前字段的值作为hive分区字段的值
          else if (esColumnName.equalsIgnoreCase(es2HivePartitionName)) {
            val partitionValue: String = es2HivePartitionFormatter match {
              case "timestamp" => TimeUtils.timestampToString(esColumnValue.asInstanceOf[Long], es2HivePartitionFormatter)
              case _ => TimeUtils.stringToString(esColumnValue.asInstanceOf[String], es2HivePartitionFormatter)
            }
            //获取hive分区字段值
            hiveValueMap.put(hivePartitionName, partitionValue)
          }

          else {
            //处理es中基本类型的数据
            val hiveColumnName: String = es2HiveNameMap(esColumnName)._1
            hiveValueMap.put(hiveColumnName, esColumnValue)
          }

        }

//        val array: Seq[AnyRef] = hiveValueMap.values.toArray
//        val row: Row = Row.fromSeq(array)
//        row
        hiveValueMap.values.toArray
      }

      }

    )

    mapRdd.foreach(println(_))
//    val df: DataFrame = spark
//      .createDataFrame(mapRdd, hiveTableSchema)
    //   df.write.mode("append").insertInto(hiveTableName)

//    df.show()

    sc.stop()

  }


}
