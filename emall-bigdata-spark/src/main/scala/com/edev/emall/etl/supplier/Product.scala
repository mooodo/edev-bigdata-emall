package com.edev.emall.etl.supplier

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Product {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_product")
    val data = spark.sql("select p.id, p.name, p.description, p.brand_id, " +
      "b.name brand_name, p.category_id, p.supplier_id, p.price, " +
      "p.image, p.tip, p.status, p.create_time, p.modify_time " +
      "from emall_supplier.t_product p join emall_supplier.t_brand b on p.brand_id=b.id").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_product")

    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知商品",null,null,null,null,null,null
      ,null,null,null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_product")
  }
}
