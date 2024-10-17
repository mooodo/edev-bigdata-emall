package com.edev.emall.etl.supplier

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Category {
  def main(args: Array[String]): Unit = {
    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_category")
    val data = spark.sql("select id, `name`, description, parent_id, layer " +
      "from emall_supplier.t_product_category").repartition(num)
    DataFrameUtils.saveOverwrite(data, "emall_etl", "etl_category")

    val sc = spark.sparkContext
    val schema = data.schema
    val defaultList = List(Row(0,"未知产品分类",null,null,null))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_category")

    val hierarchy0 = spark.sql("select c0.id parent_id, c0.`name` parent_name, " +
      "c0.id child_id, c0.`name` child_name, 0 `level` " +
      "from emall_supplier.t_product_category c0").repartition(num)
    DataFrameUtils.saveOverwrite(hierarchy0, "emall_etl", "etl_category_hierarchy")

    val hierarchy1 = spark.sql("select c0.id parent_id, c0.`name` parent_name, " +
      "c1.id child_id, c1.`name` child_name, 1 `level` " +
      "from emall_supplier.t_product_category c0 join emall_supplier.t_product_category c1 on c0.id=c1.parent_id").repartition(num)
    DataFrameUtils.saveAppend(hierarchy1, "emall_etl", "etl_category_hierarchy")

    val hierarchy2 = spark.sql("select c0.id parent_id, c0.`name` parent_name, " +
      "c2.id child_id, c2.`name` child_name, 2 `level` " +
      "from emall_supplier.t_product_category c0 join emall_supplier.t_product_category c1 on c0.id=c1.parent_id " +
      "join emall_supplier.t_product_category c2 on c1.id=c2.parent_id")
    DataFrameUtils.saveAppend(hierarchy2, "emall_etl", "etl_category_hierarchy")
  }
}
