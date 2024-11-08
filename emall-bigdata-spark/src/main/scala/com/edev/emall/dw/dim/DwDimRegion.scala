package com.edev.emall.dw.dim

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}

object DwDimRegion {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_region")
    val data = spark.sql("select id, `name`, country_id, country_name, " +
      "province_id, province_name, city_id, city_name, district_id, district_name " +
      "from emall_etl.etl_region").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_dim_region")
  }
}
