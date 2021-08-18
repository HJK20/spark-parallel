package parallel

import org.apache.spark.sql.SparkSession

object Serial {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Serial Job")
      .master("local[4]")
      .getOrCreate()

    val test_file = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/file1.csv")
      .cache()

    test_file.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/file1_out")

    test_file.show()
    test_file.select("name", "age").show()

    //    println(test_file.count())
    //    println(test_file.rdd.getNumPartitions)

    Thread.sleep(1000000)
  }
}
