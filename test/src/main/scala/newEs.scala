///**
// * @ Author     ：aresyhzhang
// * @ Date       ：Created in 11:17 2021/11/22
// * @ Description：
// */
//  package com.yx.sy
//
//  import java.text.SimpleDateFormat
//  import java.util.{Calendar, UUID}
//
//  import com.yx.sy.core.udf.DealTenantTimeUdf
//  import com.yx.sy.core.utils.{DateUtils, ResourcesUtils}
//  import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
//  import org.slf4j.{Logger, LoggerFactory}
//
//  object newEs {
//    private val Log: Logger = LoggerFactory.getLogger(this.getClass)
//
//    def main(args: Array[String]): Unit = {
//      var ds_dt: String = null
//      var ds_dt_h: String = null
//      var ds_dt_min: String = null
//      var timeType: String = null
//      if (args.length == 2 && "None" != String.valueOf(args(0))) {
//        println(args(0))
//        val tuple = DateUtils.getDateStr(args(0))
//        ds_dt = tuple._1
//        ds_dt_h = tuple._2
//        ds_dt_min = tuple._3
//        timeType = args(1)
//        println("ds_dt:", ds_dt)
//        println("ds_dt_h:", ds_dt_h)
//        println("ds_dt_min", ds_dt_min)
//        Log.warn(ds_dt + ds_dt_h + ds_dt_min)
//      } else if (args.length == 1) {
//        val tuple = DateUtils.getDateStr(args(0))
//        ds_dt = tuple._1
//        ds_dt_h = tuple._2
//        ds_dt_min = tuple._3
//        timeType = "00"
//      }
//      else {
//        val cal = Calendar.getInstance
//        cal.add(Calendar.DATE, -1)
//        ds_dt = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
//        Log.warn("ds_dt=:" + ds_dt)
//        timeType = "00"
//      }
//      val sparkBuilder = SparkSession.builder()
//      if ("local".equals(ResourcesUtils.getEnv("env"))) {
//        sparkBuilder.master("local[*]")
//      }
//      val spark = sparkBuilder.appName(this.getClass.getName)
//        .config("hive.exec.dynamici.partition", "true")
//        .config("hive.exec.dynamic.partition.mode", "nostrick")
//        .enableHiveSupport()
//        .getOrCreate()
//      val ds_dt_str = ds_dt.replace("-", "")
//      val esNodes = ResourcesUtils.getESPropValues("es_nodes")
//      val esPort = ResourcesUtils.getESPropValues("es_port")
//      //val array_str="orderitems,orderitems.itemType,orderitems.itemcode,orderitems.itemid,orderitems.itemname,"+
//      //"orderitems.skuid,orderitems.skucode,orderitems.categoryId,orderitems.quantity,orderitems.weight,"+
//      // "orderitems.price,orderitems.discountprice,orderitems.total,orderitems.payamount,orderitems.orderItemChildList,assistantInfoReqDtos,ordercoupons,orderpays,thirdActivityReqDtos"
//
//      val array_str="orderitems,orderitems.orderItemChildList,assistantInfoReqDtos,ordercoupons,orderpays,thirdActivityReqDtos"
//      val options = Map(
//        "pushdown" -> "true",
//        "es.nodes.wan.only" -> "true",
//        "es.nodes" -> esNodes,
//        "es.port" -> esPort,
//        "es.mapping.id" -> "id",
//        "es.read.field.as.array.include"->array_str
//      )
//
//      val startTime: Long = DealTenantTimeUdf.dealDateStrToLong(ds_dt_str)
//      println (startTime)
//      val endTime: Long = DealTenantTimeUdf.dealDateStrToLong(ds_dt_str)
//      println (endTime)
//      val frame = spark.read.format("org.elasticsearch.spark.sql")
//        .options(options)
//        .load("saas_stage_order/_doc")
//      //.filter(s"updatetime>=$startTime and updatetime<$endTime")
//      frame.write.mode(SaveMode.Overwrite).saveAsTable("saas.saas_stage_order")
//      import spark.sql
//      val item_str=
//        s"""
//           |select localorderid,memberid,tenantid,explode(orderitems) as orderitems,from_unixtime(cast(createdtime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as createdtime,from_unixtime(cast(updatetime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as updatetime
//           |  from saas.saas_stage_order
//           | where orderitems is not null
//           |""".stripMargin
//      println(item_str)
//      val items = sql(item_str)
//      items.printSchema()
//      val order_item = items.select( "orderitems.id","localorderid", "memberid","tenantid","orderitems.discountprice", "orderitems.itemType",
//        "orderitems.itemcode", "orderitems.itemid","orderitems.itemname", "orderitems.parentid","orderitems.payamount", "orderitems.price", "orderitems.quantity", "orderitems.skucode", "orderitems.skuid", "orderitems.total"
//        , "orderitems.weight","createdtime","updatetime")
//      order_item.printSchema()
//      order_item.write.mode(SaveMode.Overwrite).saveAsTable("saas.saas_order_item")
//      val pay_str=
//        s"""
//           |select localorderid,memberid,tenantid,explode(orderpays) as orderpays,from_unixtime(cast(createdtime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as createdtime,from_unixtime(cast(updatetime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as updatetime
//           |  from saas.saas_stage_order
//           |  where orderpays is not null
//           |""".stripMargin
//      println(pay_str)
//      val pays = sql(pay_str)
//      pays.printSchema()
//      val order_pay = pays.select("localorderid", "memberid","tenant", "orderpays.channelServiceFee", "orderpays.payamount",
//        "orderpays.paymethod", "orderpays.payname", "createdtime", "updatetime")
//      order_pay.write.mode(SaveMode.Overwrite).saveAsTable("saas.saas_order_pays")
//      /**
//       * 第三方活动
//       */
//      val third_activity_str=
//        s"""
//           |select localorderid,memberid,explode(thirdactivityreqdtos) as thirdactivity,from_unixtime(cast(createdtime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as createdtime,from_unixtime(cast(updatetime/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as updatetime
//           |  from saas.saas_stage_order
//           | where thirdactivityreqdtos is not null
//           |""".stripMargin
//      println(third_activity_str)
//      val activitys=sql(third_activity_str)
//      val third_activity = activitys.select("localorderid", "memberid", "thirdactivity.activityCode",
//        "thirdactivity.activityName", "thirdactivity.activityType", "createdtime", "updatetime")
//      third_activity.write.mode(SaveMode.Overwrite).saveAsTable("saas.saas_order_third_activity")
//      spark.stop()
//      spark.close()
//    }
//  }
//
//
//
