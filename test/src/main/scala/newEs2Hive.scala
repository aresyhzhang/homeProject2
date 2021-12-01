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
    val sparkBuilder = SparkSession.builder().config("spark.sql.caseSensitive","true")
      sparkBuilder.master("local[*]")
    val spark: SparkSession = sparkBuilder.appName(this.getClass.getName)
//      .config("hive.exec.dynamici.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nostrick")
//      .enableHiveSupport()
      .getOrCreate()


//        val esPropMap = new mutable.HashMap[String, String]()
//        esPropMap.put("hiveDataBase",args(0))
//        esPropMap.put("esNodes",args(2).split(":").head)
//        esPropMap.put("esPort",args(2).split(":").last)
//        esPropMap.put("startTime",args(3).split(",").head)
//        esPropMap.put("endTime",args(3).split(",").last)
//        val esPropArrary: Array[String] = args(1).split(',')
//       val properties: Properties = PropertyUtils.getFileProperties(esProperties)
//          import scala.collection.JavaConverters._
//          esPropMap.put("esIndexName",esIndexName)
//          properties.putAll(esPropMap.toMap.asJava)
//          println("read properties is:"+properties)

    import spark.sql
    sql(
      s"""
         |create temporary table ods_es_prod_order
         |using org.elasticsearch.spark.sql
         |options (pushdown 'true',
         |resource 'order_test2/_doc',
         |es.nodes.wan.only 'true',
         |es.nodes '49.234.117.228',
         |es.port '9200',
         |es.mapping.id 'id'
         |)
         |""".stripMargin)
    sql("desc ods_es_prod_order").show(1000,false)

    println("================")

    val ods_center_es_order_assistant_sql=
      s"""
         |select tenantId as id
         |      ,tenantId as tenant_id
         |      ,order_no as orderid
         |      ,localorderid
         |      ,aird_arr.assistant_code
         |      ,aird_arr.assistant_name
         |      ,aird_arr.assistant_phone
         |      ,createdtime as createdtime
         |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
         |      from ods_es_prod_order
         |      lateral view explode(assistantInfoReqDtos) aird_tbl as aird_arr
         |""".stripMargin

    sql(ods_center_es_order_assistant_sql).show(false)

    val ods_center_es_order_pay_sql=
      """
        |select concat(localorderid,'_',orderpays_arr.paymethod) as id
        |      ,tenantId as tenant_id
        |      ,order_no as orderid
        |      ,localorderid
        |      ,orderpays_arr.paymethod as pay_method
        |      ,orderpays_arr.payname as pay_name
        |      ,orderpays_arr.payamount as pay_amount
        |      ,createdtime as createdtime
        |      ,orderpays_arr.channelServiceFee as channel_service_fee
        |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
        |      from ods_es_prod_order
        |      lateral view explode(orderpays) orderpays_tbl as orderpays_arr
        |""".stripMargin
    sql(ods_center_es_order_pay_sql).show(false)

      val ods_center_es_order_activity_sql =
        s"""
           |select tenantId as id
           |      ,tenantId as tenant_id
           |      ,order_no as orderid
           |      ,localorderid
           |      ,thirdActivityReqDtos_arr.activityCode as local_order_id
           |      ,thirdActivityReqDtos_arr.activityName as activity_name
           |      ,thirdActivityReqDtos_arr.activityType as activity_type
           |      ,createdtime as createdtime
           |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
           |      from ods_es_prod_order
           |      lateral view explode(thirdActivityReqDtos) thirdActivityReqDtos_tbl as thirdActivityReqDtos_arr
           |""".stripMargin
    sql(ods_center_es_order_activity_sql).show(false)

    val ods_center_es_order_item_sql=
      """
        |with t1 as (
        |select tenantId as id
        |      ,orderitems_arr.uniqueKey as unique_key
        |      ,tenantId as tenant_id
        |      ,order_no as orderid
        |      ,localorderid
        |      ,orderitems_arr.itemType as item_type
        |      ,orderitems_arr.itemid as item_id
        |      ,orderitems_arr.itemcode as item_code
        |      ,orderitems_arr.itemname as item_name
        |      ,orderitems_arr.skuid as sku_id
        |      ,orderitems_arr.categoryId as categoryid
        |      ,orderitems_arr.quantity as quantity
        |      ,orderitems_arr.weight as weight
        |      ,orderitems_arr.price as price
        |      ,orderitems_arr.discountprice as discountprice
        |      ,orderitems_arr.total as total
        |      ,orderitems_arr.payamount as payamount
        |      ,createdtime as createdtime
        |      ,null as parent_id
        |      ,orderitems_arr.attribute as attribute
        |      ,orderitems_arr.orderItemChildList as orderItemChildList
        |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
        |      from ods_es_prod_order
        |      lateral view explode(orderitems) orderitems_tbl as orderitems_arr
        |    )
        |,t2 as (
        |    select id
        |    ,null as unique_key
        |    ,tenant_id
        |    ,orderid
        |    ,localorderid
        |    ,orderItemChildList_arr.itemType as item_type
        |    ,orderItemChildList_arr.itemid as item_id
        |    ,orderItemChildList_arr.itemcode as item_code
        |    ,orderItemChildList_arr.itemname as item_name
        |    ,orderItemChildList_arr.skuid as sku_id
        |    ,orderItemChildList_arr.categoryId as categoryid
        |    ,orderItemChildList_arr.quantity as quantity
        |    ,orderItemChildList_arr.weight as weight
        |    ,orderItemChildList_arr.price as price
        |    ,orderItemChildList_arr.discountprice as discountprice
        |    ,orderItemChildList_arr.total as total
        |    ,orderItemChildList_arr.payamount as payamount
        |    ,createdtime as createdtime
        |    ,unique_key as parent_id
        |    ,orderItemChildList_arr.attribute as attribute
        |    ,null as orderItemChildList
        |    ,dt
        |    from t1
        |    lateral view explode(orderItemChildList) orderItemChildList_tbl as orderItemChildList_arr
        |    )
        |,t3 as (
        |select * from t1 union all select * from t2
        |)
        |select payamount, item_code, item_id, id, localorderid, discountprice, item_name, price, orderid, quantity, createdtime, sku_id, unique_key, weight, attribute, tenant_id, parent_id, categoryid, item_type, total,dt from t3
        |""".stripMargin
    sql(ods_center_es_order_item_sql).show(false)

    val ods_center_es_order_coupon_sql=
      """
        |select tenantId as id
        |      ,tenantId as tenant_id
        |      ,order_no as orderid
        |      ,localorderid
        |      ,ordercoupons_arr.coupontempid as coupon_tempid
        |      ,ordercoupons_arr.couponcode as coupon_code
        |      ,ordercoupons_arr.couponname as coupon_name
        |      ,ordercoupons_arr.couponamount as coupon_amount
        |      ,createdtime as createdtime
        |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
        |      from ods_es_prod_order
        |      lateral view explode(ordercoupons) ordercoupons_tbl as ordercoupons_arr
        |""".stripMargin
    sql(ods_center_es_order_coupon_sql).show(false)

    val ods_center_es_order_sql=
      """
        |select tenantId as id
        |      ,tenantId as tenant_id
        |      ,order_no as orderid
        |      ,localorderid
        |      ,`shopid`
        |      ,`shopcode`
        |      ,`shopname`
        |      ,`orderchannel`
        |      ,`extuserid`
        |      ,`usermobile`
        |      ,`usercardno`
        |      ,memberid as `member_id`
        |      ,membername as `member_name`
        |      ,recipientAddress as `recipient_address`
        |      ,recipientPhone as `recipient_phone`
        |      ,recipientName as `recipient_name`
        |      ,totalamount as `total_amount`
        |      ,payamount as `pay_amount`
        |      ,freightAmount as `freight_amount`
        |      ,discountAmount as `discount_amount`
        |      ,`couponamount` as `coupon_amount`
        |      ,usepoint as `use_point`
        |      ,pointamount as `point_amount`
        |      ,accountamount as `account_amount`
        |      ,`description`
        |      ,`status`
        |      ,`invoiced`
        |      ,`invoiceType`
        |      ,`invoice`
        |      ,taxPayerId as `taxpayerid`
        |      ,`createdtime`
        |      ,`updatetime` as `updatetime`
        |      ,placeTime as `placetime`
        |      ,totalItemNum as `total_item_num`
        |      ,deliverytime as `delivery_time`
        |      ,payType as `pay_type`
        |      ,pickType as `pick_type`
        |      ,`latitude`
        |      ,`longitude`
        |      ,dinnersnumber as `dinners_number`
        |      ,returnordno as `return_ordno`
        |      ,tradeno as `trade_no`
        |      ,`other_discount_amount`
        |      ,`shop_discount_amount`
        |      ,`order_no`
        |      ,channelServiceFee as `channel_service_fee`
        |      ,memberType as `member_type`
        |      ,extFields as `ext_fields`
        |      ,`orderType` as `order_type`
        |      ,cast(from_unixtime(cast(createdtime as bigint),'yyyyMMdd') as int) as dt
        |      from ods_es_prod_order
        |""".stripMargin
    sql(ods_center_es_order_sql).show(false)

    spark.stop()
  }
}
