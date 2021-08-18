package parallel

import org.apache.spark.sql.SparkSession


object Serial2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val time = System.currentTimeMillis()

    spark.range(100).map(x => {
      println(f"${Thread.currentThread()} cal $x start")
      Thread.sleep(1000)
      val y = x * x
      println(f"${Thread.currentThread()} cal $x end")
      y
    })
//      .show(1000)
//      .count()
//      .coalesce(2)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/file1_out")

    println(s"time count: ${System.currentTimeMillis() - time}")

    spark.range(100, 200).repartition(2).map(x => {
      println(f"${Thread.currentThread()} cal $x start")
      Thread.sleep(1000)
      val y = x * x
      println(f"${Thread.currentThread()} cal $x end")
      y
    })
//      .show(1000)
      .count()
//      .coalesce(4)
//      .write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .mode("overwrite")
//      .save("src/main/resources/file2_out")

    println(s"time count: ${System.currentTimeMillis() - time}")

    Thread.sleep(1000000)
  }
}