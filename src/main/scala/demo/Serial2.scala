package demo

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
      .show(1000)
    println(s"time count: ${System.currentTimeMillis() - time}")

    spark.range(100, 200).map(x => {
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