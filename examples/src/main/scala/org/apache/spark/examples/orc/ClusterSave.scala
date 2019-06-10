package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
object ClusterSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.catalogImplementation", "hive")
      //      .config("spark.sql.json.optimize",true)
      //      .config("spark.network.timeout", 3600)
      //      .config("spark.sql.codegen.wholeStage", false)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val baseData = Seq(
      """{"name": "fox", "age": 22, "gender":"male",""",
      """{"name": "jinyu", "age": 22, "gender":"female",""",
      """{"name": "fzz", "age": 23, "gender":"male",""",
      """{"name": "jy", "age": 23, "gender":"male",""")
    val num = Integer.parseInt(args(0))
    val tableName = args(1)
    val tables = spark.sparkContext.parallelize(baseData, 4).mapPartitions(iter => {
      val count = num
      val base = iter.next()
      val buffer = new ListBuffer[JsonTest]
      for (i <- 0 until count) {
        buffer += JsonTest(i, base + s""" "uniqueId":"${UUID.randomUUID()}"}""")
      }
      buffer.iterator
    }).toDF()
    tables.write.format("hive").mode("overwrite").option("fileFormat", "orc").saveAsTable(tableName)
  }
}

case class JsonTest(id: Int, info: String)
