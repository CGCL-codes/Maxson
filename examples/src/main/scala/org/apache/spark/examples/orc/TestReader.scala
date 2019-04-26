package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession

object TestReader {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()

        val log = spark.sql("select frequency,time from log")
        log.collect()
         log.show(10)
  }
}
