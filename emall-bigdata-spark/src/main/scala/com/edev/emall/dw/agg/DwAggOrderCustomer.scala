package com.edev.emall.dw.agg

import com.edev.emall.utils.{DateUtils, PropertyFile, SaveConf, SaveUtils, SparkUtils}

object DwAggOrderCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_customer")
    spark.udf.register("getTimestamp", (data: Long)=>DateUtils.getTime(data+"00", "yyyyMMdd").toString)
    val data = spark.sql("select date_id, customer_id, count(*) cnt, sum(amount) amount, " +
      "getTimestamp(date_id) ts " +
      "from emall_dw.dw_fact_order group by date_id, customer_id").repartition(num)
    SaveUtils.saveWithPartition(data, SaveConf.build()
      .option("tableName","emall_dw.dw_agg_order_customer")
      .option("primaryKeyField","date_id,customer_id")
      .option("timestampField","ts")
      .option("partitionField","date_id"))
  }
}
