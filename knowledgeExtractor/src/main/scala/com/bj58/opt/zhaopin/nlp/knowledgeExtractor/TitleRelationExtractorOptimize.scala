package com.bj58.opt.zhaopin.nlp.knowledgeExtractor

/**
  * Created by Cheney Wang on 2018/7/5.
  */

import org.apache.spark.{SparkConf,SparkContext}


object TitleRelationExtractorOptimize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OptimizeRelationExtractor")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    val data = sc.textFile(inputPath)
    val Rmsdata = data.map(line => line.replaceAll("Some\\(", ""))

    val RelatPPtoJBN = Rmsdata.filter(line => line.split("\t")(0) == "RelationPPToJBN:").map(line => {
      val Relation = "RelationJBNToXQPP:\t"+ line.split("\t")(3)+"\t-\t"+line.split("\t")(1)
      val Relations = Array(Relation,line)
      val Newline = Relations.mkString("\3")
      Newline
    })

    val RelatCStoJBN = Rmsdata.filter(line => line.split("\t")(0) == "RelationCSToJBN:").map(line => {
      val Relation = "RelationJBNToXQCS:\t"+ line.split("\t")(3)+"\t-\t"+line.split("\t")(1)
      val Relations = Array(Relation,line)
      val Newline = Relations.mkString("\3")
      Newline
    })


    val Result = RelatCStoJBN.flatMap(line => line.split("\3")).union(Rmsdata).union(RelatPPtoJBN.flatMap(line => line.split("\3"))).map(line => line.replaceFirst("XQCS","CS").replaceFirst("XQPP","PP")).distinct

    Result.saveAsTextFile(outputPath)

  }

}
