package com.edev.emall.dw.dim

import com.edev.emall.utils.{HudiConf, HudiUtils, PropertyFile, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}

object DwDimCategoryHierarchy {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_dim_category_hierarchy")
    val data = spark.sql("select parent_id, parent_name, child_id, child_name, level " +
      "from emall_etl.etl_category_hierarchy").repartition(num)
    HudiUtils.saveAppend(data, "emall_dw", "dw_dim_category_hierarchy",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
