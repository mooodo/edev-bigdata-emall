package com.edev.emall.dm.broad

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DmBroadOrder {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dm_broad_order")
    val data = spark.sql(
      "select o.*,"+
        "cu.name customer_name,cu.gender,cu.birthdate, cu.identification,cu.phone_number," +
        "cu.email, cu.phone_number, cu.create_time, cu.modify_time "+
        "from dw.dw_fact_order o "+
        "join dw.dw_dim_customer cu on o.customer_id=cu.id ")
    DataFrameUtils.saveOverwrite(data, "emall_dm", "dm_broad_order")
  }
}
