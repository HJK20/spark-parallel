package parallel

import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors


object ParallelUsingThread2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val time = System.currentTimeMillis()

    //开一个线程提交job，并不意味着这个job只会用一个线程。在实际运行中可能会使用多个线程
    val jobExecutor = Executors.newFixedThreadPool(1)
    jobExecutor.execute(() => {
      spark.range(101, 200).map(x => {
        println(f"${Thread.currentThread()} cal $x start")
        Thread.sleep(1000)
        val y = x * x
        println(f"${Thread.currentThread()} cal $x end")
        y
      })
//        .show(1000)
        .count()
//        .coalesce(2)
//        .write.format("com.databricks.spark.csv")
//        .option("header", "true")
//        .mode("overwrite")
//        .save("src/main/resources/file1_out")

      println(s"time count: ${System.currentTimeMillis() - time}")
    })
    jobExecutor.shutdown()

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
      .save("src/main/resources/file2_out")

    println(s"time count: ${System.currentTimeMillis() - time}")

    Thread.sleep(1000000)
  }
}