package org.apache.spark.examples.util

import org.apache.commons.cli._

/**
  * create with com.fox.examples.util
  * USER: husterfox
  */
trait CommonCLI {
  def printHelp(formatter: HelpFormatter, options: Options, header: String): Unit
  def printHelpAndExit(formatter: HelpFormatter, options: Options, header: String, exitValue: Int): Unit
  def parseCommandLine(args: Array[String]): (CommandLine, Options)

}
