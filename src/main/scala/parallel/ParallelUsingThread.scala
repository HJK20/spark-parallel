package parallel
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors

object ParallelUsingThread{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    val test_file = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("src/main/resources/file1.csv")
      .cache()

    val jobExecutor = Executors.newFixedThreadPool(1)
    jobExecutor.execute(() => {
      test_file.coalesce(2)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode("overwrite")
        .save("src/main/resources/file1_out")
    })
    jobExecutor.shutdown()

    test_file.show()
    test_file.select("name", "age").show()

//    println(test_file.count())
//    println(test_file.rdd.getNumPartitions)

    Thread.sleep(1000000)
  }
}