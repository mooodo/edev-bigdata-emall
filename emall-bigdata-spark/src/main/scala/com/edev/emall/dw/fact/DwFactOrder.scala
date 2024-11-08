package com.edev.emall.dw.fact

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwFactOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_fact_order")
    spark.udf.register("getDateKey", (date: String) => (date.substring(0, 4)+date.substring(5, 7)).toInt)
    val data = spark.sql("select id, getDateKey(order_time) date_id, customer_id, address_id, region_id, " +
      "status, amount, order_time, modify_time, payment_method, payment_status, payment_time " +
      "from emall_etl.etl_order").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_fact_order")
  }
}
