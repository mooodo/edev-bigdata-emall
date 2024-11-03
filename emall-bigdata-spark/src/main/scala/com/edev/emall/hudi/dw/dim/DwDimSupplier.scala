package com.edev.emall.hudi.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimSupplier {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_supplier")
    val data = spark.sql("select id, `name`, phone_number, email, address, " +
      "create_time, modify_time from emall_etl.etl_supplier").repartition(num)
    HudiUtils.saveAppend(data, "hudi_dw", "dw_dim_supplier",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
