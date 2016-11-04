package main

import org.apache.spark._
import org.apache.spark.rdd._

object Main {

  def cleanRDD(x: RDD[String]): RDD[String] = {
    val splitWords = (line: String) => {
      line.split(' ').toList
    }
    val removePunctuationMarks = (word: String) => {
      word
        .replace(".", "")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
    }

    val isLongWord = (word: String) => word.length >= 6
    val isName = (word: String) => word.endsWith(":")

    x
      .flatMap(splitWords)
      .map(removePunctuationMarks)
      .filter(isLongWord)
      .filter(!isName(_))
  }


  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val tontonsFile = "./data/tontons.txt"
    val classeFile = "./data/classe-americaine.txt"

    val tontonsData = sc.textFile(tontonsFile, 2).cache()
    val classeData = sc.textFile(classeFile, 2).cache()

    val tontonsWords = cleanRDD(tontonsData)
    val classeWords = cleanRDD(classeData)

    val tontonsScore = tontonsWords.countByValue.toList
    val highest = tontonsScore.sortBy(_._2).takeRight(10)

    val classeScore = classeWords.countByValue.toList
    val classeHighest = classeScore.sortBy(_._2).takeRight(20)

    val common = tontonsWords.intersection(classeWords)
    val commonScore = common.countByValue.toList
    val commonHighest = commonScore.sortBy(_._2).takeRight(20)

    println("=========================================================")
    println(commonHighest)
    println("=========================================================")

  }
}
