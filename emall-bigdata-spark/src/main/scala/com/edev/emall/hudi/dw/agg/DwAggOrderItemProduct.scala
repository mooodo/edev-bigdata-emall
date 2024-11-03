package com.edev.emall.hudi.dw.agg

import com.edev.emall.utils._

object DwAggOrderItemProduct {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("dw_agg_order_item_product")
    spark.udf.register("getTimestamp", (data: Long)=>DateUtils.getTime(data+"00", "yyyyMMdd").toString)
    val data = spark.sql("select date_id, customer_id, product_id, count(*) cnt, " +
      "sum(amount) amount,  getTimestamp(date_id) ts " +
      "from emall_dw.dw_fact_order_item group by date_id, customer_id, product_id").repartition(num)
    SaveUtils.saveWithPartition(data, SaveConf.build()
      .option("tableName","hudi_dw.dw_agg_order_item_product")
      .option("primaryKeyField","date_id,customer_id,product_id")
      .option("timestampField","ts")
      .option("partitionField","date_id"))
  }
}
