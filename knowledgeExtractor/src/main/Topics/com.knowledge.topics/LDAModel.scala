package com.knowledge.topics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by Cheney WANG on 2018/7/13.
  */
object LDAModel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDA")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)
    val dictPath = args(2)

    val dictionary = sc.textFile(dictPath).map(x => {
      val line = x.substring(1, x.length - 1)
      (line.split(",")(0), line.split(",")(1).toInt)
    }).collectAsMap()
    val dict = sc.broadcast(dictionary)

    val ldaModel = DistributedLDAModel.load(sc, inputPath)
    val topics = ldaModel.topicsMatrix
    val res: List[String] = List()
    val id2Word = dict.value.map(_.swap)
    /*for (topic <- Range(0, 100)) {
      var line = ""
      line += "Topic" + topic + ":"
      println("Topic" + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        val weight = topics(word, topic)
        line += " " + id2Word.getOrElse(word, "None") + "->" + f"$weight%1.4f"
        print(" " + id2Word.getOrElse(word, "None") + "->" + f"$weight%1.4f")
//        line += " " + id2Word.getOrElse(word, "None") + "->" + f"$weight%1.4f"
      }
      println()
      res.::(line)
    }*/

    /*val topicId = Range(0, 100)
    topicId.map(x => (x, ldaModel.describeTopics(x)))*/
    val describeTopics = ldaModel.describeTopics(20)

    /*for (topic <- Range(0, 100)) {
      val describeTopics = ldaModel.describeTopics(topic)
      print("Topic" + topic + ":")
      /*for(word <- Range(0, 20)) {
//        val wordName = id2Word.getOrElse(describeTopics(topic)._1(word), "None")
//        val weight = describeTopics(topic)._2(word)
//        print(" " + wordName + "->" + f"$weight%1.4f")
      }
      println()*/
    }*/

    //    ldaModel.javaTopTopicsPerDocument(0)
    sc.parallelize(describeTopics, 1).saveAsTextFile(outputPath)
  }
}