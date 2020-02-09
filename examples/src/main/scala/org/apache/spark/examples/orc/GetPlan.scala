package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession

/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
object GetPlan {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.json.optimize", value = false)
      .config("spark.sql.catalogImplementation", "hive")
      .master("local")
      .getOrCreate()
    import spark.implicits._


    val df = spark.sql(
      """
        |select
        |non_json_column0,
        |get_json_object(json_column0,'$.url') as json_column0_id
        |from T
        |order by get_json_object(json_column0,'$.id')
        |limit 10
      """.stripMargin)

    df.explain(true)

//    df.show()

    Thread.sleep(360000)
  }
}
