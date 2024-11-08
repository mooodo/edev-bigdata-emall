package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_product")
    val data = spark.sql("select id, name, description, brand_id, brand_name, category_id, " +
      "supplier_id, price, image, tip, status, create_time, modify_time " +
      "from emall_etl.etl_product").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_product")
  }
}
