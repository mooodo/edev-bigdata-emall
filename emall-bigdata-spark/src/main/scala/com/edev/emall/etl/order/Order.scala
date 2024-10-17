package com.edev.emall.etl.order

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object Order {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_order")
    val data = spark.sql("select o.id, o.customer_id, o.address_id, a.district_id region_id, " +
      "o.`status`, o.amount, o.order_time, o.modify_time, " +
      "p.method payment_method, p.`status` payment_status, p.payment_time " +
      "from emall_order.t_order o join emall_customer.t_address a on o.address_id=a.id " +
      "left join emall_order.t_payment p on o.id=p.id").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_order")
  }
}
