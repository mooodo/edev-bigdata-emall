package com.edev.emall.dw.dim

import com.edev.emall.utils.{DateUtils, HudiConf, HudiUtils, SparkUtils}
import org.apache.hudi.DataSourceWriteOptions.{RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DwDimDate {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.init("dw_dim_date")
    val sc = spark.sparkContext
    val strStart = args.apply(0)
    val dateStart = DateUtils.getTime(strStart, "yyyy-MM")
    val strEnd = args.apply(1)
    val dateEnd = DateUtils.getTime(strEnd, "yyyy-MM")
    val list = List()
    val listDate = DateUtils.getMonthsBetween(dateStart,dateEnd,list)

    val rows = listDate.map {
      x => Row(
        DateUtils.format(x, "yyyyMM").toInt,
        DateUtils.format(x, "yyyy"),
        DateUtils.format(x, "MM"),
        DateUtils.format(x, "yyyyMM").toInt,
        DateUtils.format(x, "yyyy年"),
        DateUtils.format(x, "MM月"),
        DateUtils.format(x, "yyyy年MM月")
      )}

    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("year", StringType, nullable = true),
      StructField("month", StringType, nullable = true),
      StructField("yyyymm", IntegerType, nullable = true),
      StructField("year_desc", StringType, nullable = true),
      StructField("month_desc", StringType, nullable = true),
      StructField("yyyymm_desc", StringType, nullable = true)
    ))
    val result = sc.parallelize(rows)
    val data = spark.createDataFrame(result, schema)
    HudiUtils.saveAppend(data, "emall_dw", "dw_dim_date",
      HudiConf.build()
        .option(RECORDKEY_FIELD.key(), "id")
        .option(TABLE_TYPE.key(), "COPY_ON_WRITE"))
  }
}
