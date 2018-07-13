package com.bj58.opt.zhaopin.nlp.knowledgeExtractor

/**
  * Created by wangche on 2018/7/5.
  */
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.rdd.RDD

object TitleCoverageAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CoverageAnalysis")
    val sc = new SparkContext(conf)

    val CS = sc.textFile("/home/hdp_lbg_supin/middata/chewang/KnowledgeExtractor/userdict/newChangsuo.txt")

    val JBN = sc.textFile("/home/hdp_lbg_supin/middata/chewang/KnowledgeExtractor/userdict/newLeimu.txt")

    val PP = sc.textFile("/home/hdp_lbg_supin/middata/chewang/KnowledgeExtractor/userdict/newPinpai.txt")

    val data = sc.textFile("/home/hdp_lbg_supin/middata/chewang/KnowledgeExtractor/TitleExtractorResult/output_optimized")

    val CS_Name = CS.map(line => line.split("\t")(0))
    val PP_Name = PP.map(line => line.split("\t")(0))
    val JBN_Name = JBN.map(line => line.split("\t")(0))

    val CS_Name_Num = CS_Name.count //498
    val PP_Name_Num = PP_Name.count //3888
    val JBN_Name_Num = JBN_Name.count //24031

    var result = ArrayBuffer[String](CS_Name_Num.toString,PP_Name_Num.toString,JBN_Name_Num.toString)

    //val data = sc.textFile(args(0))


    val PPinData = data.map(line => line.split("\t")(1)).filter(line => line.split("\1")(1) == "xqpp").map(line => line.split("\1")(0)).distinct
    val PPinData_Num = PPinData.count //1707

    result += PPinData_Num.toString

    val CSinData = data.map(line => line.split("\t")(1)).filter(line => line.split("\1")(1) == "xqcs").map(line => line.split("\1")(0)).distinct
    val CSinData_Num = CSinData.count //481

    result += CSinData_Num.toString

    val JBNinData = data.map(line => line.split("\t")(1)).filter(line => line.split("\1")(1) == "jbn").map(line => line.split("\1")(0)).distinct
    val JBNinData_Num = JBNinData.count //5945

    result += JBNinData_Num.toString

    //val newresult = result.mkString("  Value:")

    //val result = Array("")

//    val inputPath = args(0).split(",")
//
//    var mergedCutRes = sc.textFile(inputPath(0))
//    //val mergedCutRes = sc.textFile(inputPath)


//    val hadoopConf = sc.hadoopConfiguration
//    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//
//    for (i <- 1 until inputPath.length) {
//      //union之前要先判断数据是否存在
//      val pathInput = new Path(inputPath(i))
//      if (fileSystem.exists(pathInput)) {
//        mergedCutRes = mergedCutRes.union(sc.textFile(inputPath(i)))
//      }
//    }




    val Final = sc.parallelize(result)

    Final.coalesce(1).saveAsTextFile("/home/hdp_lbg_supin/middata/chewang/KnowledgeExtractor/TitleExtractorResult/RateResult")








  }

}
