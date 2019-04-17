package com.jzfq

import com.jzfq.util.PhoneCheck
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AppCustomerContactsAll {
  def main(args: Array[String]): Unit = {

    // System.setProperty("HADOOP_USER_NAME", "root")
    Logger.getLogger("org").setLevel(Level.INFO)

    val sparkHiveSession: SparkSession = SparkSession.builder()
      //.master("local[2]")
      .appName("AppCustomerContactsAll")
      .config("spark.defalut.parallelismsm", 20)
      .config("spark.sql.shuffle.partitions", 20)
      .config("spark.speculation", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.comprespress", "true")
      .config("metastore.client.capability.check", "false")
      .enableHiveSupport()
      .getOrCreate()
    sparkHiveSession.sql("show  databases").show()
    sparkHiveSession.sql("use ods")

    sparkHiveSession.sqlContext.udf.register("isAuto8", (number: String) => {
      if (PhoneCheck.isAuto8(number)) true else false
    })
    /**
      * 全量清洗通讯录数据并且存入hive表中
      */
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
        |isAuto8(t.contacts_phone) = false
      """.stripMargin).createOrReplaceTempView("tmp")

    sparkHiveSession.sql("insert overwrite table dw.dw_rms_app_customer_contacts select id,contacts_name,contacts_phone,contacts_relation,is_valid,customer_id,create_time,update_time,10001 as version,strodsdatastatus,dtodsdatasynctime,llogfileoffset from tmp")

    sparkHiveSession.stop()
  }
}
