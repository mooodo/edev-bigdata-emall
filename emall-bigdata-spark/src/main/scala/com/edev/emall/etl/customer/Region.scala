package com.edev.emall.etl.customer

import com.edev.emall.utils.{DataFrameUtils, PropertyFile, SparkUtils}
import org.apache.spark.sql.Row

object Region {
  def main(args: Array[String]): Unit = {

    val num = PropertyFile.getProperty("numPartitions").toInt
    val spark = SparkUtils.init("etl_region")

    val district = spark.sql("select d.id, d.name, c.id country_id, c.name country_name, " +
      "p.id province_id, p.name province_name, c1.id city_id, c1.name city_name, " +
      "d.id district_id, d.name district_name " +
      "from emall_customer.t_district d join emall_customer.t_city c1 on d.city_id=c1.id " +
      "join emall_customer.t_province p on c1.province_id=p.id " +
      "join emall_customer.t_country c on p.country_id=c.id").repartition(num)
    DataFrameUtils.saveOverwrite(district, "emall_etl", "etl_region")

    val city = spark.sql("select c1.id, c1.name, c.id country_id, c.name country_name, " +
      "p.id province_id, p.name province_name, c1.id city_id, c1.name city_name, " +
      "NULL district_id, NULL district_name " +
      "from emall_customer.t_city c1 join emall_customer.t_province p on c1.province_id=p.id " +
      "join emall_customer.t_country c on p.country_id=c.id").repartition(num)
    DataFrameUtils.saveAppend(city, "emall_etl", "etl_region")

    val province = spark.sql("select p.id, p.name, c.id country_id, c.name country_name, " +
      "p.id province_id, p.name province_name, " +
      "NULL city_id, NULL city_name, NULL district_id, NULL district_name " +
      "from emall_customer.t_province p join emall_customer.t_country c on p.country_id=c.id").repartition(num)
    DataFrameUtils.saveAppend(province, "emall_etl", "etl_region")

    val country = spark.sql("select id, name, id country_id, name country_name, " +
      "NULL province_id, NULL province_name, " +
      "NULL city_id, NULL city_name, NULL district_id, NULL district_name " +
      "from emall_customer.t_country").repartition(num)
    DataFrameUtils.saveAppend(country, "emall_etl", "etl_region")

    val sc = spark.sparkContext
    val schema = district.schema
    val defaultList = List(Row(0,"未知地区",0,"未知国家",0,"未知省份",0,"未知地市",0,"未知区县"))
    val defaultRow = sc.parallelize(defaultList)
    val defaultData = spark.createDataFrame(defaultRow, schema)
    DataFrameUtils.saveAppend(defaultData, "emall_etl", "etl_region")
  }
}
