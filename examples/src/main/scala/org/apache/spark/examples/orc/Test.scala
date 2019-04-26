package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession
case class people(name:String,age:Int)
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val  df = spark.sparkContext.textFile("examples/src/main/resources/people.txt").map(x => {
      val info = x.split(",")
      people(info(0),info(1).toInt)
    }).toDF()

 //   spark.sql("drop table people")
    df.write.format("hive").option("fileFormat","orc").saveAsTable("people")
  }
}
