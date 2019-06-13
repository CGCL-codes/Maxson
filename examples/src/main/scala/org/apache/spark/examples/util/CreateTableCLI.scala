package org.apache.spark.examples.util

import org.apache.commons.cli._

/**
  * create with com.fox.examples.util
  * USER: husterfox
  */
object CreateTableCLI extends CommonCLI {
  def printHelp(formatter: HelpFormatter, options: Options, header: String): Unit = {
    formatter.printHelp(header, options)
  }

  def printHelpAndExit(formatter: HelpFormatter, options: Options, header: String, exitValue: Int): Unit = {
    printHelp(formatter, options, header)
    System.exit(exitValue)
  }

  def parseCommandLine(args: Array[String]): (CommandLine, Options) = {
    val commandLineParser = new GnuParser()
    val options = new Options()
    val help = new Option("h", "help,", false, "print help")


    val tableName = new Option("t", "tableName", true, "tableName")
    tableName.setArgName("tableName")
    tableName.setType(classOf[String])


    val sourceFile = new Option("s", "sourceFile", true, "sourceFile")
    sourceFile.setArgName("sourceFile")
    sourceFile.setType(classOf[String])

    val partitionNumber = new Option("pn", "partitionNumber", true, "partition Number")
    partitionNumber.setArgName("partitionNumber")
    partitionNumber.setType(classOf[Int])


    val recordEachPartition = new Option("rep","recordEachPartition", true, "record Each Partition")
    recordEachPartition.setArgName("recordEachPartition")
    recordEachPartition.setType(classOf[Int])

    val selectedSQlNumber = new Option("ssn","selectedSQlNumber",true,"selected SQl number")
    selectedSQlNumber.setArgName("selectedSQLNumber")
    selectedSQlNumber.setType(classOf[String])


    val optimize = new Option("o","optimize",true,"optimize or not (true or false)")
    optimize.setArgName("optimize")
    optimize.setType(classOf[Boolean])

    val cycleNumber = new Option("cn","cycleNumber",true,"number of cycles")
    cycleNumber.setArgName("cycleNumber")
    cycleNumber.setType(classOf[Int])




    val cacheTableName = new Option("ct", "cacheTableName", true, "cacheTableName")
    cacheTableName.setArgName("cacheTableName")
    cacheTableName.setType(classOf[String])



    options.addOption(help)
    options.addOption(sourceFile)
    options.addOption(tableName)
    options.addOption(partitionNumber)
    options.addOption(recordEachPartition)
    options.addOption(cacheTableName)
    options.addOption(selectedSQlNumber)
    options.addOption(optimize)
    options.addOption(cycleNumber)

    (commandLineParser.parse(options, args), options)
  }
}

