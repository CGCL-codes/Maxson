package org.apache.spark.examples.sql

import org.apache.commons.cli.HelpFormatter
import org.apache.spark.examples.schema._
import org.apache.spark.examples.util.CreateTableCLI
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex

/**
  * @author zyp
  */
object CreateTable {
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
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val tableSourcePath = commandLine.getOptionValue("s")
    println(s"CreateTable:$tableSourcePath")

    val pattern = new Regex("""basedata_(\w+)?_""")
    val tableName = pattern.findFirstMatchIn(tableSourcePath).get.group(1)
    println(s"tableName:$tableName")


    val partitionNumber = Integer.parseInt(commandLine.getOptionValue("pn"))
    val recordEachPartition = Integer.parseInt(commandLine.getOptionValue("rep"))

    assert(tableSourcePath != null && tableName != null && partitionNumber != null && recordEachPartition != null)

    val baseData = Source.fromFile(tableSourcePath).mkString
    val seq =for (i <- 0 until partitionNumber) yield baseData

    tableName match {
      case "ods_pcic_dmc_model_data" =>
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter =>{
          val base = iter.next()
          val buffer = new ListBuffer[ods_pcic_dmc_model_data]
          val array = base.split("%%%%%")
          val tnt_inst_id = array(0)
          val id = array(1)
          val gmt_create = array(2)
          val gmt_creator = array(3)
          val gmt_modified = array(4)
          val gmt_modifier = array(5)
          val category = array(6)
          val entity_type = array(7)
          val entity_name = array(8)
          val entity_code = array(9)
          val data_provider = array(10)
          val data_time = array(11)
          val object_content = array(12)
          val metadata = array(13)
          val data_org_id = array(14)
          val collect_execution_id = array(15)
          for (i <- 0 until recordEachPartition) {
            buffer += ods_pcic_dmc_model_data(tnt_inst_id,id ,gmt_create ,gmt_creator ,gmt_modified ,gmt_modifier ,
              category ,entity_type ,entity_name ,entity_code ,data_provider ,data_time ,object_content ,metadata ,
              data_org_id ,collect_execution_id )
          }
          buffer.iterator
        }).toDF
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      case "ods_pdm_order_operate" =>  {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[ods_pdm_order_operate]
          val array = base.split("%%%%%")
          val operate_no= array(0)
          val data_col = array(1)
          for(i <- 0 until recordEachPartition){
            buffer += ods_pdm_order_operate(operate_no ,data_col )
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "ods_lnia_org_info" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[ods_lnia_org_info]
          val array = base.split("%%%%%")
          val inst_code = array(0)
          val ip_id = array(1)
          val ip_role_id = array(2)
          val in_acct_no = array(3)
          val in_acct_tp = array(4)
          val out_acct_no = array(5)
          val out_acct_tp = array(6)
          for(i <- 0 until recordEachPartition){
            buffer += ods_lnia_org_info(inst_code ,ip_id ,ip_role_id ,in_acct_no ,in_acct_tp ,out_acct_no ,out_acct_tp )
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "ods_parm_d" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[ods_parm_d]
          val array = base.split("%%%%%")
          val parm_t_code = array(0)
          val crtor = array(1)
          val last_moder =  array(2)
          val gmt_create = array (3)
          val gmt_modified = array (4)
          val json_data = array(5)
          for(i <- 0 until recordEachPartition){
            buffer += ods_parm_d(parm_t_code  ,crtor ,last_moder  ,gmt_create ,gmt_modified ,json_data)
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "ods_parm_d2" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[ods_parm_d2]
          val array = base.split("%%%%%")
          for(i <- 0 until recordEachPartition){
            buffer += ods_parm_d2(array(0),array(1),array(2),array(3),array(4),array(5))
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "s_gd_poi_base" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[s_gd_poi_base]
          val array = base.split("%%%%%")
          for(i <- 0 until recordEachPartition){
            buffer += s_gd_poi_base(array(0),array(1))
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "cms_ces_generic_review_df" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[cms_ces_generic_review_df]
          val array = base.split("%%%%%")
          for(i <- 0 until recordEachPartition){
            buffer += cms_ces_generic_review_df(BigInt(array(0)),array(1),array(2))
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case "s_generic_task_edit_result_json" => {
        val dataTable = spark.sparkContext.parallelize(seq,partitionNumber).mapPartitions(iter => {
          val base = iter.next()
          val buffer = new ListBuffer[s_generic_task_edit_result_json]
          val array = base.split("%%%%%")
          for(i <- 0 until recordEachPartition){
            buffer += s_generic_task_edit_result_json(array(0))
          }
          buffer.iterator
        }).toDF()
        dataTable.write.format("hive").mode("append").option("fileFormat", "orc").saveAsTable(tableName)
      }
      case _=> println(s"file not found:$tableName")
    }







  }
}
