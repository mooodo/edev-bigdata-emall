package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimSupplier {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_supplier")
    val data = spark.sql("select id, `name`, phone_number, email, address, " +
      "create_time, modify_time from emall_etl.etl_supplier").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_supplier")
  }
}
