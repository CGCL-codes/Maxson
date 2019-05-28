package org.apache.spark.examples.util
import org.apache.commons.cli._

/**
  * @author zyp
  */
object CommonCLIHelper {
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


    val cycleTimes = new Option("n", "cycleTimes", true, "CycleTimes")
    cycleTimes.setArgName("cycleTimes")
    cycleTimes.setType(classOf[String])

    val optimizeUsed = new Option("o", "optimizeUsed", true, "whether using the optimize.eg:true")
    optimizeUsed.setArgName("optimizeUsed")
    optimizeUsed.setType(classOf[String])



    options.addOption(help)
    options.addOption(optimizeUsed)
    options.addOption(tableName)
    options.addOption(cycleTimes)

    (commandLineParser.parse(options, args), options)
  }

}
