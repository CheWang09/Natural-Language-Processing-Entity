package com.knowledge.topics

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Cheney WANG on 2018/7/13.
  */
object LDATrain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fp-growth")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)
    val outputVocabularyPath = args(2)

    val cutRes = sc.textFile(inputPath).repartition(60).filter(x => x.split("\t").length == 2).map(x => x.split("\t")(1).replace("\3", " "))

    // 过滤掉停用词（无用词性）
    val filteredWords = cutRes.map(line => {
      line.split(" ").filter(word => word.split("\1").length == 2).filter(word => {
        val pos = word.split("\1")(1)
        pos.equals("jbn") || pos.equals("xqhy") || pos.equals("xqcs") || pos.equals("xqjn") || pos.equals("xqpp")
        //        !(pos.equals("t") || pos.equals("x") || pos.equals("m") || pos.equals("c") || pos.equals("r"))
      }).mkString(" ")
    })

    // 生成词表并广播
    val dictionaryRdd = filteredWords.flatMap(x => x.split(" ")).distinct().zipWithIndex()
    val dictionary = dictionaryRdd.collectAsMap()
    val dict = sc.broadcast(dictionary)
    val totalNum = sc.broadcast(dictionary.size)

    // 生成训练数据
    val trainData = filteredWords.map(line => {
      val tmpArray = new Array[Long](totalNum.value)
      line.split(" ").foreach(word => {
        // 从词典map中取得词的id
        val index: Long = dict.value.getOrElse(word, -1)
        if(index != -1) {
          tmpArray(index.toInt) = tmpArray(index.toInt) + 1
        }
      })

      tmpArray.mkString(" ")
    })

    // Load and parse the data
    val parsedData = trainData.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // num of topics
    val topicNum = 100

    // Cluster the documents into topicNum topics using LDA
    val ldaModel = new LDA().setK(topicNum).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    val res: List[String] = List()
    val id2Word = dict.value.map(_.swap)
    for (topic <- Range(0, 3)) {
      var line = ""
      line += "Topic" + topic + ":"
      for (word <- Range(0, ldaModel.vocabSize)) {
        val weight = topics(word, topic)
        line += " " + id2Word.getOrElse(word, "None") + "->" + f"$weight%1.4f"
      }
      res.::(line)
    }

    // Save and load model.
    ldaModel.save(sc, outputPath)
    dictionaryRdd.saveAsTextFile(outputVocabularyPath)
    val sameModel = DistributedLDAModel.load(sc, outputPath)
  }
}
