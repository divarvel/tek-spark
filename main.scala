package main

import org.apache.spark._

object Main {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val tontonsFile = "./data/tontons.txt"
    val classeFile = "./data/classe-americaine.txt"

    val tontonsData = sc.textFile(tontonsFile, 2).cache()
    val classeData = sc.textFile(classeFile, 2).cache()

    println("============================================================================")
    println(classeData.first())
    println("============================================================================")

  }
}
