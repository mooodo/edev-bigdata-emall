package com.edev.emall.hudi.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimAddress {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_address")
    val data = spark.sql("select id, customer_id, region_id, " +
      "detail_address, phone_number from emall_etl.etl_address").repartition(num)
    HudiUtils.saveAppend(data, "hudi_dw", "dw_dim_address",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
