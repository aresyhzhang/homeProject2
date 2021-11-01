import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ES2Hive {
  //暂时不支持从es的list结构中选取时间字段来当做hive的分区字段
  /**
   * 需要解决的问题：
   * 1.es的字段和hive的字段不一致，全部都不一致，还是先不读es的元数据，采用手动填写的方式吧
   * 2.es中的类型可能也和hive中的类型不一致,将会按照配置的es字段顺序和hive字段顺序去写入到es中。
   * 1.做日增 ok
   * 2.要传多个配置文件 ok
   * 3.分区时间处理 ok
   * 4.处理类型不匹配 ok
   * 解决嵌套中有嵌套 ok
   * 两个es往一个hive写
   */
  //设置日志级别
          Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
          Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
  //

  def main(args: Array[String]): Unit = {
    //配置hadoop的环境变量,本地需要设置,线上不需要,在c盘的myconf目录下bin目录下放winUtils,下个hadoop.dll放到C:\Windows\System32
    System.setProperty("hadoop.home.dir", "E:\\myconf")
    println("scala main args is:"+args.mkString(","))
    val properties: Properties = PropertyUtils.getFileProperties(args.last)
    if(properties.isEmpty){
      throw new RuntimeException("parse prop error,can't find prop,the input args is:"+args.mkString(","))
    }
    println("read properties is:"+properties)
    val sc: SparkContext = init()
    //创建spark上下文
    val spark = SparkSession.builder().appName("RddToDataFrame")
      //
      .master("local")
        .config("hive.metastore.uris","thrift://192.168.126.129:9083") //设置hive元数据库的地址
//        .config("hive.metastore.uris","thrift://192.168.1.102:9083") //设置hive元数据库的地址
//          .config("fs.defaultFS","hdfs://hadoop102:8020") //设置hdfs  nameNode的地址
          .config("fs.defaultFS","hdfs://namenode:8020") //设置hdfs  nameNode的地址
          .config("hive.exec.dynamici.partition",value = true)
          .config("hive.exec.dynamic.partition.mode","nonstrict")
          .enableHiveSupport()
      .getOrCreate()

    val schemaTuple = createHiveTableSchema(properties)
    val esRDD: RDD[Row] = readDataFromES(sc, properties, schemaTuple._2, schemaTuple._3)
    spark.sql(s"use ${properties.getProperty("hiveDataBase")}")
    val df: DataFrame = spark
      .createDataFrame(esRDD, schemaTuple._1)
    df.show(10)
//    df.write.mode(properties.getProperty("saveMode").insertInto(properties.getProperty("hiveTableName"))
    sc.stop()
  }

  def init(): SparkContext = {
    //初始化，取读取resources下的配置
    //创建spark上下文
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    conf.set("es.nodes", "192.168.126.129")
    conf.set("es.port", "9200")
    conf.set("es.scroll.size", "10000") //滑动大小
    conf.set("spark.broadcast.compress", "true") // 设置广播压缩
    conf.set("spark.rdd.compress", "true") // 设置RDD压缩
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
    conf.set("spark.shuffle.file.buffer", "1280k")
    conf.set("spark.reducer.maxSizeInFlight", "1024m")
    //es.nodes.wan.only：默认为 false，设置为 true 之后，会关闭节点的自动 discovery，只使用es.nodes声明的节点进行数据读写操作；如果你需要通过域名进行数据访问，则设置该选项为 true，否则请务必设置为 false；
    conf.set("spark.es.nodes.wan.only", "true")
    conf.set("spark.reducer.maxMblnFlight", "1024m")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("index.mapper.dynamic", "false")
    new SparkContext(conf)
  }

  def createHiveTableSchema(properties: Properties): (StructType,
    mutable.LinkedHashMap[String, (String, String, String)],
    mutable.LinkedHashMap[AnyRef, AnyRef]) = {
    val esColumnList = properties.getProperty("esColumnList")
    val hiveColumnList = properties.getProperty("hiveColumnList")
    val hivePartitionName = properties.getProperty("hivePartitionName") //hive中的分区字段
    val hivePartitionType = properties.getProperty("hivePartitionType") //hive中的分区字段
    //根据给定的hive字段构建schema
    val hiveColumnArray: Array[String] = hiveColumnList.split(',')
    val esColumnArray: Array[String] = esColumnList.split(',')
    val hiveStructTypeArray: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
    //用来保存hive的字段名为一个map，后续遍历es时往里面填充数据 [esA,(hiveA,String,Integer)]
    val esMetaDataMap = new mutable.LinkedHashMap[String, (String, String, String)]()
    val hiveMetaDataMap = new mutable.LinkedHashMap[AnyRef, AnyRef]()
    //需要写入的hive的字段，两个数组的长度必须一样才行
//    if (esColumnArray.length != hiveColumnArray.length) throw new RuntimeException("esColumnList do not match hiveColumnList,Please check and try again")
    //同时开始遍历es和hive的列格式
    for (i <- hiveColumnArray.indices) {
      val hiveArray: Array[String] = hiveColumnArray(i).split(" ")
        val hiveColumnName: String = hiveArray(0)
        val hiveColumnType: String = hiveArray(1)
        //用hive的列名和类型构建StructType,后面转成dataframe使用
        addStructType(hiveColumnName, hiveColumnType, hiveStructTypeArray)
      if(esColumnArray.length>i){
        val esArray: Array[String] = esColumnArray(i).split(" ")
        val esColumnName: String = esArray(0)
        val esColumnType: String = esArray(1)
        esMetaDataMap.put(esColumnName, Tuple3(hiveColumnName, hiveColumnType, esColumnType))
      }
        hiveMetaDataMap.put(hiveColumnName, null)
    }

    //增加分区字段
    if(hivePartitionType.equalsIgnoreCase("int")){
      hiveStructTypeArray.append(StructField(hivePartitionName, IntegerType, nullable = true))
    }else{
      hiveStructTypeArray.append(StructField(hivePartitionName, StringType, nullable = true))
    }
    hiveMetaDataMap.put(hivePartitionName, null)
    //构建hive表的schema
    val hiveTableSchema: StructType = StructType(hiveStructTypeArray)
    Tuple3(hiveTableSchema, esMetaDataMap, hiveMetaDataMap)
  }

  def addStructType(inputName: String, inputType: String, inputArr: ArrayBuffer[StructField], isNull: Boolean = true): Unit = {
    inputType match {
      case "string" => inputArr.append(StructField(inputName, StringType, isNull))
      case "int" => inputArr.append(StructField(inputName, IntegerType, isNull))
      case "long" => inputArr.append(StructField(inputName, LongType, isNull))
      case "bigint" => inputArr.append(StructField(inputName, LongType, isNull))
      case "double" => inputArr.append(StructField(inputName, DoubleType, isNull))
      case "float" => inputArr.append(StructField(inputName, FloatType, isNull))
      case "boolean" => inputArr.append(StructField(inputName, BooleanType, isNull))
      case _ => throw new RuntimeException(s"hiveColumnList have a error dataType $inputName:$inputType,Please check and try again")
    }
  }

  def transEsType2HiveType(inputValue: AnyRef,esType:String,hiveType:String): AnyRef ={
    var esColumnValue: AnyRef =inputValue
    if(esType.equalsIgnoreCase("date")){
      esColumnValue =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(inputValue)
    }
    hiveType match {
      case "string"=>esColumnValue.toString.asInstanceOf[AnyRef]
      case "bigint"=>{
        //date转bigint
        if(esType.equalsIgnoreCase("date")){
          val strDate: Long =TimeUtils.stringToTimestamp(esColumnValue.toString)
          strDate.asInstanceOf[AnyRef]
        }else{
          esColumnValue.toString.toLong.asInstanceOf[AnyRef]
        }
      }
      case "int"=>esColumnValue.toString.toInt.asInstanceOf[AnyRef]
      case "float"=>esColumnValue.toString.toFloat.asInstanceOf[AnyRef]
      case "double"=>esColumnValue.toString.toDouble.asInstanceOf[AnyRef]
      case "boolean"=>esColumnValue.toString.toBoolean.asInstanceOf[AnyRef]
      case _=> throw new RuntimeException(s"tansDataType error,UnSupport hiveType:$hiveType,inputValue is $inputValue")
    }

  }

  def handleESMapData(es2HiveNameMap:mutable.LinkedHashMap[String, (String, String, String)],
                      hiveValueMap:mutable.LinkedHashMap[AnyRef, AnyRef],
                      esName:String,esValue:AnyRef):Unit ={
    if(es2HiveNameMap.contains(esName)){
      var writeValue=esValue
      val hiveName: String =es2HiveNameMap(esName) ._1
      val hiveType: String = es2HiveNameMap(esName)._2
      val esType: String = es2HiveNameMap(esName)._3
      //如果hive类型和es类型不一致，转为hive类型
      if(!hiveType.equalsIgnoreCase(esType)){
        writeValue=transEsType2HiveType(esValue,esType,hiveType)
      }
      hiveValueMap.put(hiveName, writeValue)
    }
  }

  def readDataFromES(sc: SparkContext, properties: Properties,
                     esMetaDataMap: mutable.LinkedHashMap[String, (String, String, String)],
                     hiveMetaDataMap: mutable.LinkedHashMap[AnyRef, AnyRef]): RDD[Row] = {
    val esIndexName = properties.getProperty("esIndexName")
    val esTypeName = properties.getProperty("esTypeName")
    val es2HivePartitionName = properties.getProperty("es2HivePartitionName")
    val es2HivePartitionFormatter = properties.getProperty("es2HivePartitionFormatter")
    val hivePartitionName = properties.getProperty("hivePartitionName")
    val hivePartitionFormatter = properties.getProperty("hivePartitionFormatter")
    val hivePartitionType = properties.getProperty("hivePartitionType")
    val writeDataMod = properties.getProperty("writeDataMode")
    //从es中读取数据
    var esDataRDD:RDD[(String, collection.Map[String, AnyRef])]=null
    if(writeDataMod.equalsIgnoreCase("day")){
      val beforeYesterdayZeroTime: Long = TimeUtils.getBeforeYesterdayZeroTime()
      val todayZeroTime: Long = beforeYesterdayZeroTime+(86400000*2)
      val esQuery = s"""{\"query\":{\"range\":{\"$es2HivePartitionName\":{\"gte\":\"$beforeYesterdayZeroTime\",\"lte\":\"$todayZeroTime\"}}}}"""
      println("esQuery is :"+esQuery)
       esDataRDD = EsSpark.esRDD(sc, esIndexName + "/" + esTypeName,query = esQuery)
    }else{
       esDataRDD = EsSpark.esRDD(sc, esIndexName + "/" + esTypeName)
    }
    esDataRDD.map(
      { esData: (String, collection.Map[String, AnyRef]) => {
        val esValue: collection.Map[String, AnyRef] = esData._2
        //把之前准备好的数据格式clone来使用
        val es2HiveNameMap: mutable.LinkedHashMap[String, (String, String, String)] = esMetaDataMap.clone()
        val hiveValueMap: mutable.LinkedHashMap[AnyRef, AnyRef] = hiveMetaDataMap.clone()

        //针对hive的数据进行填充
        for (elem <- esValue) {

          var esColumnName: String = elem._1
          var esColumnValue: AnyRef = elem._2
          //开始处理es中集合类型的数据

          //如果es的字段名=hive的分区字段名，就要取当前字段的值作为hive分区字段的值
          if (esColumnName.equalsIgnoreCase(es2HivePartitionName)) {
            val partitionValue: String = es2HivePartitionFormatter match {
              case "epoch_millis" =>{
                val strDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(esColumnValue)
                TimeUtils.stringToString(strDate,"yyyy-MM-dd HH:mm:ss", hivePartitionFormatter)
              }
              case _ => TimeUtils.stringToString(esColumnValue.asInstanceOf[String], hivePartitionFormatter)
            }
            //获取hive分区字段值
            if(hivePartitionType.equalsIgnoreCase("int")){
              hiveValueMap.put(hivePartitionName, partitionValue.toInt.asInstanceOf[AnyRef])
            }else{
              hiveValueMap.put(hivePartitionName, partitionValue)
            }

          }

          esColumnValue match {
            case list: Seq[AnyRef] =>
              for (elem <- list) {
                val jsonMap: mutable.LinkedHashMap[AnyRef, AnyRef] = elem.asInstanceOf[mutable.LinkedHashMap[AnyRef, AnyRef]]
                for (json <- jsonMap) {
                  val esName: String = esColumnName + "." + json._1
                  val esValue: AnyRef =json._2
                  //如果是双层嵌套类型
                  esValue match {
                    case doubleValueMap: mutable.LinkedHashMap[AnyRef, AnyRef] =>
                      for (doubleValue <- doubleValueMap) {
                        handleESMapData(es2HiveNameMap, hiveValueMap, esName + "." + doubleValue._1, doubleValue._2)
                      }
                    case _ =>
                      handleESMapData(es2HiveNameMap, hiveValueMap, esName, esValue)
                  }
                }
              }
            case _ =>
              //处理es中基本类型的数据
              if(es2HiveNameMap.contains(esColumnName)){
                //获取es对应的hive的字段名
                val hiveColumnName: String = es2HiveNameMap(esColumnName)._1
                val hiveType: String = es2HiveNameMap(esColumnName)._2
                val esType: String = es2HiveNameMap(esColumnName)._3
                //如果hive类型和es类型不一致，转为hive类型
                if(!hiveType.equalsIgnoreCase(esType)){
                  esColumnValue=transEsType2HiveType(esColumnValue,esType,hiveType)
                }
                //存入hive临时map中
                hiveValueMap.put(hiveColumnName, esColumnValue)
              }
          }

        }
        val array: Array[AnyRef] = hiveValueMap.values.toArray
        Row.fromSeq(array)
      }
      }
    )
  }




}
