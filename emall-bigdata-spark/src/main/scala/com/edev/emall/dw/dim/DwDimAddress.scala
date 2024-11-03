package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimAddress {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_address")
    val data = spark.sql("select id, customer_id, region_id, " +
      "detail_address, phone_number from emall_etl.etl_address").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_address")
  }
}
