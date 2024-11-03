package com.edev.emall.hudi.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_product")
    val data = spark.sql("select id, name, description, brand_id, brand_name, category_id, " +
      "supplier_id, price, image, tip, status, create_time, modify_time " +
      "from emall_etl.etl_product").repartition(num)
    HudiUtils.saveAppend(data, "hudi_dw", "dw_dim_product",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
