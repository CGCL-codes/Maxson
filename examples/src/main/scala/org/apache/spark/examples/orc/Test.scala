package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession
//case class People2(name:String,age:Long)
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()
//    import spark.implicits._
//    val  df = spark.sparkContext.textFile("examples/src/main/resources/people.txt").map(x => {
//      val info = x.split(",")
//      Path(info(0))
//    }).toDF()
//
// //   spark.sql("drop table people")
//    df.write.format("hive").option("fileFormat","orc").saveAsTable("path")

    /****************测试读到iter最后一个元素的处理*************************/

    val peo  = spark.sql("select * from path")
    peo.collect()
    peo.show()
  }
}
case class Path(path:String)
