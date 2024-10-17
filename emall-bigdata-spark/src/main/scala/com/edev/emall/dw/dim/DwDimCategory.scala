package com.edev.emall.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimCategory {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_category")
    val data = spark.sql("select id, `name`, description, parent_id, layer " +
      "from emall_etl.etl_category").repartition(num)
    HudiUtils.saveAppend(data, "emall_dw", "dw_dim_category",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
