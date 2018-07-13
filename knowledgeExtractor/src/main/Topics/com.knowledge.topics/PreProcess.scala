package com.knowledge.topics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Cheney WANG on 2018/7/13.
  */
object PreProcess {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDA")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    val cutRes = sc.textFile(inputPath).repartition(60).filter(x => x.split("\t").length == 2).map(x => x.split("\t")(1).replace("\3", " "))

    // 过滤掉停用词（无用词性）
    val filteredWords = cutRes.map(line => {
      line.split(" ").filter(word => word.split("\1").length == 2).filter(word => {
        val pos = word.split("\1")(1)
        !(pos.equals("t") || pos.equals("x") || pos.equals("m") || pos.equals("c") || pos.equals("r"))
      }).mkString(" ")
    })

    // 生成词表并广播
    val dictionary = filteredWords.flatMap(x => x.split(" ")).distinct().zipWithIndex().collectAsMap()
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

    trainData.saveAsTextFile(outputPath)
  }

}
