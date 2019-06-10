package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession
import java.util.UUID

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.util.JsonExperimentCLI

import scala.collection.mutable.ListBuffer

/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
object ClusterTest {
  def main(args: Array[String]): Unit = {
    val helper = "ClusterTest: a cluster test util for json parse optimizer," +
      " \n -h or --help to view helper"
    val (commandLine, options) = JsonExperimentCLI.parseCommandLine(args)
    val formatter = new HelpFormatter()

    if (commandLine.hasOption("h")) {
      JsonExperimentCLI.printHelpAndExit(formatter, options, helper, -1)
    }
    val spark = SparkSession
      .builder()
      .config("spark.sql.catalogImplementation", "hive")
      //      .config("spark.sql.json.optimize",true)
      //      .config("spark.network.timeout", 3600)
      //      .config("spark.sql.codegen.wholeStage", false)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val baseData = """{"name": "fox", "age": 22, "gender":"male","""
    val funcType = commandLine.getOptionValue("ft")
    assert(funcType != null)
    funcType match {
      case "it" =>
        val partitionNumber = Integer.parseInt(commandLine.getOptionValue("pn"))
        val recordEachPartition = Integer.parseInt(commandLine.getOptionValue("rep"))
        val tableName = commandLine.getOptionValue("tn")
        assert(tableName != null && partitionNumber > 0 && recordEachPartition > 0)
        val data = for (i <- 0 until recordEachPartition) yield baseData
        val dataTable = spark.sparkContext.parallelize(data, partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[JsonTest]
          for (i <- 0 until recordEachPartition) {
            buffer += JsonTest(i, base + s"""uniqueId":"${UUID.randomUUID()}}""")
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("overwrite").option("fileFormat", "orc").saveAsTable(tableName)

      case "pj" =>
        //default_json_test
        val tableName = commandLine.getOptionValue("tn")
        assert(tableName != null)
        val jsonTable = spark.sql("select " +
          "get_json_object(info,'$.name') as info_name," +
          "get_json_object(info,'$.age')as info_age," +
          "get_json_object(info,'$.gender') as info_gender," +
          "get_json_object(info,'$.uniqueId') as info_uniqueId" +
          "from json_test")
        jsonTable.write.format("hive").mode("overwrite").option("fileFormat", "orc").saveAsTable(tableName)

      case _ =>
        JsonExperimentCLI.printHelpAndExit(formatter, options, helper, -1)

    }
  }
}

case class JsonTest(id: Int, info: String)
