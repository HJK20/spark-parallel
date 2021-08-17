package demo

import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors


object ParallelUsingThread3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val time = System.currentTimeMillis()

    val jobExecutor = Executors.newFixedThreadPool(3)
    jobExecutor.execute(() => {
      spark.range(50, 100).map(x => {
        println(f"${Thread.currentThread()} cal $x start")
        Thread.sleep(1000)
        val y = x * x
        println(f"${Thread.currentThread()} cal $x end")
        y
      })
        .show(1000)
      println(s"time count: ${System.currentTimeMillis() - time}")
    })

    jobExecutor.execute(() => {
      spark.range(100, 150).map(x => {
        println(f"${Thread.currentThread()} cal $x start")
        Thread.sleep(1000)
        val y = x * x
        println(f"${Thread.currentThread()} cal $x end")
        y
      })
        .show(1000)
      println(s"time count: ${System.currentTimeMillis() - time}")
    })

    jobExecutor.execute(() => {
      spark.range(150, 200).map(x => {
        println(f"${Thread.currentThread()} cal $x start")
        Thread.sleep(1000)
        val y = x * x
        println(f"${Thread.currentThread()} cal $x end")
        y
      })
        .show(1000)
      println(s"time count: ${System.currentTimeMillis() - time}")
    })

    jobExecutor.shutdown()

    spark.range(50).map(x => {
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