package io.univalence.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")

    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("/opt/spark-data/*.txt").repartition(10)

    val words: RDD[String] = text.flatMap(_.split(" "))

    words
      .map((_, 1))
      .reduceByKey(_ + _)
      .foreach(println)

    println(s"Number of partitions : ${text.getNumPartitions}")
  }
}
