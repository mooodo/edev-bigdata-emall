package com.edev.emall.etl.order

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object OrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_order_item")
    val data = spark.sql("select o.id, o.customer_id, o.address_id, a.district_id region_id, " +
      "o.`status`, o.order_time, o.modify_time, " +
      "i.product_id, i.quantity, i.price, i.amount " +
      "from emall_order.t_order_item i join emall_order.t_order o on i.order_id=o.id " +
      "join emall_customer.t_address a on o.address_id=a.id").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_order_item")
  }
}
