package com.edev.emall.hudi.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimCustomer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_customer")
    val data = spark.sql("select id, name, gender, identification, birthdate, " +
      "email, phone_number, create_time, modify_time from emall_etl.etl_customer").repartition(num)
    HudiUtils.saveAppend(data, "hudi_dw", "dw_dim_customer",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
