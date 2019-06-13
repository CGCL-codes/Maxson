package org.apache.spark.examples.sql

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.util.CreateTableCLI
import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object CacheTable {
  def main(args: Array[String]): Unit = {
    val helper = "CreateTable:creating table for experiments"+
      " \n -h or --help to view helper"

    val (commandLine, options) = CreateTableCLI.parseCommandLine(args)
    val formatter = new HelpFormatter()

    if (commandLine.hasOption("h")) {
      CreateTableCLI.printHelpAndExit(formatter, options, helper, -1)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.json.writeCache",true)
      .enableHiveSupport()
      .getOrCreate()

    val filePath = commandLine.getOptionValue("s")
//    val pattern = new Regex("""jsonpath_(\w+)?_""")
//    val tableName = pattern.findFirstMatchIn(filePath).get.group(1)
//    println(s"tableName:$tableName")
    assert(filePath != null)
    val tableMap = spark.sparkContext.textFile(filePath).map(line => {
      val jsonPathInfoArr = line.split(",")
      val dbName = jsonPathInfoArr(0)
      val tableName = jsonPathInfoArr(1)
      val columnName = jsonPathInfoArr(2)
      val jsonPath = jsonPathInfoArr(3)
      (dbName+"."+tableName,columnName+"#"+jsonPath)
    })
      .groupByKey()
      .map(v => {
        val iter = v._2.toIterator
        val path = iter.next()
        val pathArr = path.split("#")
        val alias = s"${pathArr(0)}_${pathArr(1)}".replace(".","_").replace("[","").replace("]","")
        var wrappedJsonPath = s"get_json_object(${pathArr(0)}, '$$.${pathArr(1)}') as ${alias}"
        while(iter.hasNext){
          val path = iter.next()
          val pathArr = path.split("#")
          val alias = s"${pathArr(0)}_${pathArr(1)}".replace(".","_").replace("[","").replace("]","")
          wrappedJsonPath += s",get_json_object(${pathArr(0)}, '$$.${pathArr(1)}') as ${alias}"
        }
        (v._1,wrappedJsonPath)
    }).collect().toMap

    for(x <- tableMap){
      val dbAndTableName = x._1.split("\\.")
      val dbName = dbAndTableName(0)
      val tableName = dbAndTableName(1)
      val selectContentString = x._2
      println(selectContentString)
      spark.sql(s"drop table if exists ${dbName}_${tableName}")
      spark.sql(s"select ${selectContentString} from ${dbName}.${tableName}").write.format("hive").option("fileFormat","orc").mode("append").saveAsTable(s"${dbName}_${tableName}")

      spark.sql(s"select * from ${dbName}_${tableName} limit 1").show()
    }




  }
}
