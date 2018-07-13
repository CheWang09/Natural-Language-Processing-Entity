package com.bj58.opt.zhaopin.nlp.knowledgeExtractor

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.Path

/**
  * Created by Cheney Wang on 2018/7/11.
  */
object RelationWeight {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AddRelationWeight")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val inputPath = args(0).split(",")
    val outputPath = args(1)

    var data = sc.textFile(inputPath(0))

    for (i <- 1 until inputPath.length) {
      //union之前要先判断数据是否存在
      val pathInput = new Path(inputPath(i))
      if (fileSystem.exists(pathInput)) {
        data = data.union(sc.textFile(inputPath(i)))
      }
    }

    val Merdata = data.map(line => line.replaceAll("Some\\(", ""))

    val RelatPPtoJBN = Merdata.filter(line => line.split("\t")(0) == "RelationPPToJBN:").map(line => {
      val Relation = "RelationJBNToXQPP:\t" + line.split("\t")(3) + "\t-\t" + line.split("\t")(1)
      val Relations = Array(Relation, line)
      val Newline = Relations.mkString("\3")
      Newline
    })

    val RelatCStoJBN = Merdata.filter(line => line.split("\t")(0) == "RelationCSToJBN:").map(line => {
      val Relation = "RelationJBNToXQCS:\t" + line.split("\t")(3) + "\t-\t" + line.split("\t")(1)
      val Relations = Array(Relation, line)
      val Newline = Relations.mkString("\3")
      Newline
    })


    val ResultBAdW = RelatCStoJBN.flatMap(line => line.split("\3")).union(Merdata).union(RelatPPtoJBN.flatMap(line => line.split("\3"))).map(line => line.replaceFirst("XQCS", "CS").replaceFirst("XQPP", "PP"))

    val RelationCountPairsRDD = ResultBAdW.map(line => (line, 1)).reduceByKey(_ + _).map(x => Array(x._1,x._2.toString)).map(x => x.mkString("\tRelation Weight is:\t")).distinct

    //所得权重没有去重
    RelationCountPairsRDD.saveAsTextFile(outputPath)

  }
}
