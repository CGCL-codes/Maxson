package org.apache.spark.examples.sql

import java.util.Date

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.schema.AllSQL
import org.apache.spark.examples.util.CreateTableCLI
import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object TestSQL {




  def main(args: Array[String]): Unit = {
    val helper = "TestSQl:"+
      " \n -h or --help to view helper"

    val (commandLine, options) = CreateTableCLI.parseCommandLine(args)
    val formatter = new HelpFormatter()

    if (commandLine.hasOption("h")) {
      CreateTableCLI.printHelpAndExit(formatter, options, helper, -1)
    }



    val sqlNumber = commandLine.getOptionValue("ssn")
    val optimize = commandLine.getOptionValue("o")
    val cycleNumber = commandLine.getOptionValue("cn").toInt
    assert(sqlNumber != null && optimize != null && cycleNumber > 0)

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .config("spark.sql.json.optimize",optimize)
      .enableHiveSupport()
      .getOrCreate()

    sqlNumber match {
      case "1" =>
        var st = new Date().getTime
        spark.sql(AllSQL.sql1).foreachPartition(iter => println(s"iter size:${iter.size}") )
        var et = new Date().getTime
        println(s"TestSQL 1: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql1).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 1:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")


      case "2" => //sql2有数组形式
        var st = new Date().getTime
        val df = spark.sql(AllSQL.sql2)
        df.foreachPartition(iter =>println(s"iter size:${iter.size}") )
        var et = new Date().getTime
        df.show(10)
        println(s"TestSQL 2: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql2).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 2:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")
      case "3" =>
        var st = new Date().getTime
        spark.sql(AllSQL.sql3).foreachPartition(iter => println(s"iter size:${iter.size}") )
        var et = new Date().getTime
        println(s"TestSQL 3: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql3).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 3:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")
      case "4" =>
        var st = new Date().getTime
        spark.sql(AllSQL.sql4).foreachPartition(iter => println(s"iter size:${iter.size}") )
        var et = new Date().getTime
        println(s"TestSQL 4: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql4).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 4:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")
      case "5" =>
        var st = new Date().getTime
        spark.sql(AllSQL.sql5).foreachPartition(iter => println(s"iter size:${iter.size}") )
        var et = new Date().getTime
        println(s"TestSQL 5: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql5).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 5:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")
      case "6" =>
        var st = new Date().getTime
        spark.sql(AllSQL.sql6).foreachPartition(iter => println(s"iter size:${iter.size}"))
        var et = new Date().getTime
        println(s"TestSQL 6: First execution time = ${(et-st)/1000.0}s ")

        st = new Date().getTime
        for(i <-0 until cycleNumber){
          spark.sql(AllSQL.sql6).foreachPartition(iter => println(s"iter size:${iter.size}") )
        }
        et = new Date().getTime
        println(s"TestSQL 6:${cycleNumber} times average time = ${(et-st)/(cycleNumber*1000.0)}s")
    }


  }
}
