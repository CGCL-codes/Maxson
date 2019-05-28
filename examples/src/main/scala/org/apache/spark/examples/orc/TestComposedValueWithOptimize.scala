package org.apache.spark.examples.orc

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.orc.Huge.tableName
import org.apache.spark.examples.util.CommonCLIHelper
import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object TestComposedValueWithOptimize {
  def main(args: Array[String]): Unit = {


    val helper = "TestComposedValueWithOptimizeOpen: users pass the para to decide Whether to turn on Optimization," +
      "the table to query,the number of test cycles. \n eg: -o true -t log -n 2"
    val (commandLine,options) = CommonCLIHelper.parseCommandLine(args)
    val formatter = new HelpFormatter()

    if(commandLine.hasOption("h")) {
      CommonCLIHelper.printHelpAndExit(formatter, options, helper, 0)
    }
    val flag:Boolean =(commandLine.getOptionValue("o") == null
      || commandLine.getOptionValue("t") == null
      || commandLine.getOptionValue("n") == null)
    if(flag){
      println("flag is true !!!!!!")
      val optimizeUsed = commandLine.getOptionValue("o")
      val tableName = commandLine.getOptionValue("t")
      val cycleTimes = commandLine.getOptionValue("n")
      val spark = SparkSession
        .builder()
        .master("local")
        .config("spark.sql.catalogImplementation","hive")
        .config("spark.sql.json.optimize",optimizeUsed)
        .enableHiveSupport()
        .getOrCreate()

      TestUtil.readCol(spark,tableName,cycleTimes.toInt)
    }else{
      println("flag is false !!!")
      CommonCLIHelper.printHelpAndExit(formatter,options,helper,1)
    }
  }
}
