package com.edev.emall.dw.agg

import com.edev.emall.utils.{DataFrameUtils, DateUtils, PropertyFile, SparkUtils}

object DwAggOrderRegion {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_region")
    spark.udf.register("getTimestamp", (data: Long)=>DateUtils.getTime(data+"00", "yyyyMMdd").toString)
    val data = spark.sql("select date_id, region_id, count(*) cnt, sum(amount) amount, " +
      "getTimestamp(date_id) ts " +
      "from emall_dw.dw_fact_order group by date_id, region_id").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_dw", "dw_agg_order_region")
  }
}
