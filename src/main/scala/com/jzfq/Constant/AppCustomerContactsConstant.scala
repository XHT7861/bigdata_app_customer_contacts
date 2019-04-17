package com.jzfq.Constant

object AppCustomerContactsConstant {

  /**
    * spark相关配置，需要的key
    */
  val SPARK_DEFALUT_PARALLELISMSM = "spark.defalut.parallelismsm"

  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val SPARK_SERILALIZER = "spark.serializer"
  val SPARK_SPECULATION = "spark.speculation"
  val SPARK_RDD_COMPRESPRESS = "spark.rdd.comprespress"

  /**
    * spark相关配置，需要的value
    */

  val SPARK_DEFALUT_PARALLELISMSM_VALUE = 50

  val SPARK_SQL_SHUFFLE_PARTITIONS_VALUE = 20
  val SPARK_SERILALIZER_VALUE = "true"
  val SPARK_SPECULATION_VALUE = "org.apache.spark.serializer.KryoSerializer"
  val SPARK_RDD_COMPRESPRESS_VALUE = "true"


}
