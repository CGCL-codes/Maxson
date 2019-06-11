package org.apache.spark.examples.util
import org.apache.commons.cli._

/**
  * create with org.apache.spark.examples.util
  * USER: husterfox
  */
object JsonExperimentCLI {
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


    /**
      * 1.  it: initial table, save table
      * 2.  pj: parseJson
      */

    val funcType = new Option("ft", "funcType", true, "func type [it: initial table, save table |  pj: parseJson]")
    funcType.setArgName("tableName")
    funcType.setType(classOf[String])



    val tableName = new Option("tn", "tableName", true, "table Name")
    tableName.setArgName("tableName")
    tableName.setType(classOf[String])

    val partitionNumber = new Option("pn", "partitionNumber", true, "partition Number")
    tableName.setArgName("partitionNumber")
    tableName.setType(classOf[Int])


    val recordEachPartition = new Option("rep","recordEachPartition", true, "record Each Partition")
    tableName.setArgName("tableName")
    tableName.setType(classOf[Int])

    options.addOption(help)
    options.addOption(funcType)
    options.addOption(tableName)
    options.addOption(partitionNumber)
    options.addOption(recordEachPartition)

    (commandLineParser.parse(options, args), options)
  }

}
