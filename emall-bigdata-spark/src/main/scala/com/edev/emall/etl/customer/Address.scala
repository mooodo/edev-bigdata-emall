package com.edev.emall.etl.customer

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Address {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_address")
    val data = spark.sql("select id, customer_id, district_id region_id, " +
      "detail_address, phone_number from emall_customer.t_address").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_address")

    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,null,null,"未知客户地址",null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_address")
  }
}
