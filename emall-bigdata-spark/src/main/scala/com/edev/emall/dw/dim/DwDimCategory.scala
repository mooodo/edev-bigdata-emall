package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimCategory {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_category")
    val data = spark.sql("select id, `name`, description, parent_id, layer " +
      "from emall_etl.etl_category").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_category")
  }
}
