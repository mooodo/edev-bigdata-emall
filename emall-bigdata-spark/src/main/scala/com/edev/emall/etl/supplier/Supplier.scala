package com.edev.emall.etl.supplier

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Supplier {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_supplier")
    val data = spark.sql("select id, `name`, phone_number, email, address, " +
      "create_time, modify_time from emall_supplier.t_supplier").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_supplier")

    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知供应商",null,null,null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_supplier")
  }
}
