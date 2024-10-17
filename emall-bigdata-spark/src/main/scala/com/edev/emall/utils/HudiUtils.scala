package com.edev.emall.utils

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{Dataset, Row}

object HudiUtils {

  def saveOverwrite(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      options(conf.getAllConfigs).
      option("hoodie.table.name", table).
      mode(Overwrite).
      saveAsTable(database+"."+table)
  }

  def saveAppend(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      options(conf.getAllConfigs).
      option("hoodie.table.name", table).
      mode(Append).
      saveAsTable(database+"."+table)
  }

  def delete(data: Dataset[Row], database: String, table: String, conf: HudiConf): Unit = {
    data.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION.key(), "delete").
      options(conf.getAllConfigs).
      option("hoodie.table.name", table).
      mode(Append).
      saveAsTable(database+"."+table)
  }

  def saveWithPartition(data: Dataset[Row], conf: SaveConf): Unit = {
    val tableName = conf.get("tableName")
    val primaryKeyField = conf.get("primaryKeyField")
    val timestampField = conf.get("timestampField")
    val partitionField = conf.get("partitionField")
    data.createOrReplaceTempView("tmp")
    data.sparkSession.sql(s"create table if not exists ${tableName} using hudi "+
      s"tblproperties(type='cow',primaryKey='${primaryKeyField}',preCombineField='${timestampField}')"+
      s"partitioned by (${partitionField}) as select * from tmp where 0=1;")
    data.sparkSession.sql(s"insert into ${tableName} select * from tmp")
  }
}
