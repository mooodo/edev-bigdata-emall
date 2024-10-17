package com.edev.emall.etl.customer

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Customer {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_customer")
    val data = spark.sql("select id, name, gender, identification, birthdate, " +
      "email, phone_number, create_time, modify_time from emall_customer.t_customer").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_customer")

    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知客户",null,null,null,null,null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_customer")
  }
}
