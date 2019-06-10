package org.apache.spark.examples.orc

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.util.CreateTableCLI
import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object CreateTableAndCache {
  def main(args: Array[String]): Unit = {
    val helper = "CreateTableAndCache: users pass the para to create Table And Cache," +
      " \n eg: -t table -s sourceFile.txt -ct chacheTable"
    val (commandLine, options) = CreateTableCLI.parseCommandLine(args)
    val formatter = new HelpFormatter()

    if (commandLine.hasOption("h")) {
      CreateTableCLI.printHelpAndExit(formatter, options, helper, 0)
    }




    val flag = (commandLine.getOptionValue("t") != null && commandLine.getOptionValue("s") != null && commandLine.getOptionValue("ct") != null)
    if (flag) {

      val spark = SparkSession
        .builder()
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.json.writeCache", true)
        .enableHiveSupport()
        .getOrCreate()
      println("flag is true !!!!!!")
      val tableName = commandLine.getOptionValue("t")
      val path = commandLine.getOptionValue("s")
      val cacheTableName = commandLine.getOptionValue("ct")
      TestUtil.createTable(spark, path, tableName)
      TestUtil.cacheJson(spark, cacheTableName, tableName)
    } else {
      CreateTableCLI.printHelpAndExit(formatter, options, helper, 0)
    }


  }
}
