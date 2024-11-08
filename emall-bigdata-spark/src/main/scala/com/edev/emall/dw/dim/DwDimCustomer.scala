package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_customer")
    val data = spark.sql("select id, name, gender, identification, birthdate, " +
      "email, phone_number, create_time, modify_time from emall_etl.etl_customer").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_customer")
  }
}
