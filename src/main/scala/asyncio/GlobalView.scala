package asyncio

import org.apache.spark.sql.SparkSession

object GlobalView {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Serial Job")
      .master("local[4]")
      .getOrCreate()

    val file = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/1.csv")
      .cache()

    file.createGlobalTempView("myView")

//    file
//      .write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .mode("overwrite")
//      .save("src/main/resources/file1_out")
    spark
      .newSession()
      .sql("SELECT * FROM global_temp.myView")
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/file1_out")

//    println(file.count())
    file.crossJoin(file.limit(1000)).count()

    Thread.sleep(1000000)
  }
}
