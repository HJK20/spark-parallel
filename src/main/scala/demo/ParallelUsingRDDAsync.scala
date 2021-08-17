package demo

import org.apache.spark.sql.SparkSession


object ParallelUsingRDDAsync {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test Parallel Jobs Using Thread")
      .master("local[4]")
      .getOrCreate()

    val data = spark.range(100).rdd
    println(data.countAsync().get())
    println(data.count())

    Thread.sleep(1000000)
  }
}