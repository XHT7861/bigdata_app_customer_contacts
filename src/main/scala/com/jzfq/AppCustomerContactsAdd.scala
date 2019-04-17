package com.jzfq

import com.jzfq.util.PhoneCheck
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object AppCustomerContactsAdd {
  def main(args: Array[String]): Unit = {
    // System.setProperty("HADOOP_USER_NAME", "root")
    val logger = LoggerFactory.getLogger("BlackList")
    logger.info("程序开始")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkHiveSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("spark-shell")
      .config("spark.defalut.parallelismsm", 15)
      .config("spark.sql.shuffle.partitions", 20)
      .config("spark.speculation", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.comprespress", "true")
      .config("metastore.client.capability.check", "false")
      .enableHiveSupport()
      .getOrCreate()
    sparkHiveSession.sql("use ods")

    sparkHiveSession.sqlContext.udf.register("isAuto8", (number: String) => {
      if (PhoneCheck.isAuto8(number)) true else false
    })

    /**
      * 每天的增量数据
      */
    logger.info("开始计算每天的增量数据")
    sparkHiveSession.sql(
      """
        |select id,contacts_name,substr(contacts_phone,length(contacts_phone)-11+1,11) contacts_phone,contacts_relation,is_valid,customer_id,create_time,update_time,strodsdatastatus,dtodsdatasynctime,llogfileoffset
        |from (
        |SELECT *, Row_Number() OVER (partition by contacts_phone order by id desc) rank FROM ods.ods_rms_app_customer_contacts)
        |as t where t.rank=1  and
        |length(t.contacts_phone) >10 and
        |substr(contacts_phone,length(contacts_phone)-11+1,1) =1 and
        |(substr(contacts_phone,length(contacts_phone),1)=substr(contacts_phone,length(contacts_phone)-1,1) and
        |substr(contacts_phone,length(contacts_phone)-2,1)=substr(contacts_phone,length(contacts_phone)-3,1) and
        |substr(contacts_phone,length(contacts_phone)-4,1)=substr(contacts_phone,length(contacts_phone)-5,1) and
        |substr(contacts_phone,length(contacts_phone)-6,1)=substr(contacts_phone,length(contacts_phone)-7,1)
        | )= false and
        |(
        |substr(contacts_phone,length(contacts_phone),1)=substr(contacts_phone,length(contacts_phone)-1,1)=substr(contacts_phone,length(contacts_phone)-2,1)=substr(contacts_phone,length(contacts_phone)-3,1)=substr(contacts_phone,length(contacts_phone)-4,1)=substr(contacts_phone,length(contacts_phone)-5,1)=substr(contacts_phone,length(contacts_phone)-6,1)
        |) = false and
        |to_date(create_time)=date_sub(current_date,1) and
        |isAuto8(t.contacts_phone) = false
      """.stripMargin).createOrReplaceTempView("tmp_new")
    println("tmp_new 开始计算")
    sparkHiveSession.sql("select count(1) from tmp_new").show()
    println("tmp_new 计算结束")


    /**
      * dw层的历史表
      */
    logger.info("dw层的历史表")
    sparkHiveSession.sql(
      """
        |select
        |id,contacts_name,contacts_phone,
        |contacts_relation,is_valid,customer_id,
        |create_time,update_time,
        |strodsdatastatus,dtodsdatasynctime,llogfileoffset
        |from dw.ods_rms_app_customer_contacts
      """.stripMargin).createOrReplaceTempView("tmp_old")



    /**
      * 删除dw层历史数据存在的增量数据
      */
    logger.info("开始删除dw层历史数据存在的增量数据")
    sparkHiveSession.sql(
      """
        |select
        |tmp_new.id,
        |tmp_new.contacts_name,
        |tmp_new.contacts_phone,
        |tmp_new.contacts_relation,
        |tmp_new.is_valid,
        |tmp_new.customer_id,
        |tmp_new.create_time,
        |tmp_new.update_time,
        |tmp_new.strodsdatastatus,
        |tmp_new.dtodsdatasynctime,
        |tmp_new.llogfileoffset
        | from tmp_new
        | right join tmp_old
        | on tmp_new.customer_id = tmp_old.customer_id and tmp_new.contacts_phone =tmp_old.contacts_phone
        | where tmp_new.contacts_phone is null
      """.stripMargin).createOrReplaceTempView("tmp_res")


    /**
      * 每天的增量数据和dw历史表数据union all
      */
    logger.info("每天的增量数据和dw历史表数据union all")
    sparkHiveSession.sql(
      """
        |select
        |id,
        |contacts_name,
        |contacts_phone,
        |contacts_relation,
        |is_valid,
        |customer_id,
        |create_time,
        |update_time,
        |strodsdatastatus,
        |dtodsdatasynctime,
        |llogfileoffset
        |from tmp_new
        |union all
        |select
        |id,
        |contacts_name,
        |contacts_phone,
        |contacts_relation,
        |is_valid,
        |customer_id,
        |create_time,
        |update_time,
        |strodsdatastatus,
        |dtodsdatasynctime,
        |llogfileoffset
        | from tmp_res
      """.stripMargin).createOrReplaceTempView("tmp_finall")

    /**
      * 把结果插入hive表
      */
    logger.info("把结果插入hive表 all")
    sparkHiveSession.sql(
      """
        |insert overwrite table dw.dw_rms_app_customer_contacts
        |select
        |id,
        |contacts_name,
        |contacts_phone,
        |contacts_relation,
        |is_valid,
        |customer_id,
        |create_time,
        |update_time,
        |100001 as version,
        |strodsdatastatus,
        |dtodsdatasynctime,
        |llogfileoffset
        | from tmp_finall
      """.stripMargin)

    sparkHiveSession.stop()
  }

}
