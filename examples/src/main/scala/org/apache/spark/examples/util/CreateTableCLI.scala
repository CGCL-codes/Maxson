package org.apache.spark.examples.util

import org.apache.commons.cli._

/**
  * @author zyp
  */
object CreateTableCLI {
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

    val cacheTableName = new Option("ct", "cacheTableName", true, "cacheTableName")
    cacheTableName.setArgName("cacheTableName")
    cacheTableName.setType(classOf[String])



    options.addOption(help)
    options.addOption(sourceFile)
    options.addOption(tableName)
    options.addOption(cacheTableName)

    (commandLineParser.parse(options, args), options)
  }
}
