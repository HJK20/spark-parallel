package parallel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.Future

object ParallelUsingFAIR {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Test Parallel Jobs Using FAIR With More Pools")
      .master("local[4]")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.scheduler.allocation.file", "src/main/resources/multiplepool.xml")
      .getOrCreate()
    import spark.implicits._

    val time = System.currentTimeMillis()


    val jobExecutor: ExecutorService = Executors.newFixedThreadPool(1)
    jobExecutor.execute(() => {
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
      spark.range(100, 200).map(x => {
        println(f"${Thread.currentThread()} cal $x start")
        Thread.sleep(1000)
        val y = x * x
        println(f"${Thread.currentThread()} cal $x end")
        y
      })
        .show(1000)
      println(s"time count: ${System.currentTimeMillis() - time}")
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
    })
    jobExecutor.shutdown()

//    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
//    spark.range(100, 200).map(x => {
//      println(f"${Thread.currentThread()} cal $x start")
//      Thread.sleep(1000)
//      val y = x * x
//      println(f"${Thread.currentThread()} cal $x end")
//      y
//    })
//      .show(1000)
//    println(s"time count: ${System.currentTimeMillis() - time}")
//    spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)

    spark.range(100).map(x => {
      println(f"${Thread.currentThread()} cal $x start")
      Thread.sleep(1000)
      val y = x * x
      println(f"${Thread.currentThread()} cal $x end")
      y
    })
      .show(1000)
    println(s"time count: ${System.currentTimeMillis() - time}")

    Thread.sleep(1000000)
  }
}
