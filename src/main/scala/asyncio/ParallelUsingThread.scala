package asyncio

import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors

object ParallelUsingThread{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    val file = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/1.csv")
      .cache()

    val jobExecutor = Executors.newFixedThreadPool(1)
    jobExecutor.execute(() => {
      file
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode("overwrite")
        .save("src/main/resources/file1_out")
    })
    jobExecutor.shutdown()

    file.crossJoin(file.limit(1000)).count()

    Thread.sleep(1000000)
  }
}