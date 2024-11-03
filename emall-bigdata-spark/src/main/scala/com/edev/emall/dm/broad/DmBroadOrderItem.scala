package com.edev.emall.dm.broad

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DmBroadOrderItem {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dm_broad_order_item")
    val data = spark.sql("select oi.*,"+
      "cu.name customer_name, cu.gender, cu.birthdate, cu.identification, cu.phone_number, cu.email, " +
      "cu.phone_number, cu.create_time customer_create_time, cu.modify_time customer_modify_time, "+
      "p.name product_name, p.description, p.brand_id, p.brand_name, p.category_id, " +
      "p.supplier_id, p.price, p.image, p.tip, p.status product_status, " +
      "p.create_time product_create_time, p.modify_time product_modify_time "+
      "from dw.dw_fact_order_item oi "+
      "join dw.dw_dim_customer cu on oi.customer_id=cu.id "+
      "join dw.dw_dim_product p on oi.product_id=p.id ")
    DataFrameUtils.saveOverwrite(data, "emall_dm", "dm_broad_order_item")
  }
}
