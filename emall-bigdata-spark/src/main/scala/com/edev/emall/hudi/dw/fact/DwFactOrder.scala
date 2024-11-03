package com.edev.emall.hudi.dw.fact

import com.edev.emall.utils.{PropertyFile, SaveConf, SaveUtils, SparkUtils}

object DwFactOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_fact_order")
    spark.udf.register("getDateKey", (date: String) => (date.substring(0, 4)+date.substring(5, 7)).toInt)
    val data = spark.sql("select id, getDateKey(order_time) date_id, customer_id, address_id, region_id, " +
      "status, amount, order_time, modify_time, payment_method, payment_status, payment_time " +
      "from emall_etl.etl_order").repartition(num)
    SaveUtils.saveWithPartition(data, SaveConf.build()
      .option("tableName","hudi_dw.dw_fact_order")
      .option("primaryKeyField","id")
      .option("timestampField","order_time")
      .option("partitionField","date_id"))
  }
}
