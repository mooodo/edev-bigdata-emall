package com.edev.emall.hudi.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimRegion {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_region")
    val data = spark.sql("select id, `name`, country_id, country_name, " +
      "province_id, province_name, city_id, city_name, district_id, district_name " +
      "from emall_etl.etl_region").repartition(num)
    HudiUtils.saveAppend(data, "hudi_dw", "dw_dim_region",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
