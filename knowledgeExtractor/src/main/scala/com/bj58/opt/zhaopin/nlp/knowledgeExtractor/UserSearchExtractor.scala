package com.bj58.opt.zhaopin.nlp.knowledgeExtractor

/**
    * Created by Cheney Wang on 2018/7/9.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.hadoop.fs.Path

object UserSearchExtractor {
  def DataPreProc(line : String):String = {
    val LabelList = Array("""jbn""","""xqpp""","""xqcs""")
    var Entities = line.split("\2")
    var NewEntities = ArrayBuffer[String]()
    //Entities.foreach{entity => if(LabelList.contains(entity.split("\1")(1))){(newline+entity+"\2")}}
    for (entity <- Entities) {
      try {
        val label: String = entity.split("\1")(1)
        if (LabelList.contains(label)) {
          NewEntities += entity
        }
      }
      catch {
        case e:ArrayIndexOutOfBoundsException => println(line)
      }
    }
    NewEntities.toArray.mkString("\2")
  }
  def RelationExtractor(line:String):String = {
    var dict:HashMap[String,ArrayBuffer[String]]=HashMap("jbn"->new ArrayBuffer[String](),"xqcs"->new ArrayBuffer[String](),"xqpp"->new ArrayBuffer[String]())
    val LabelList =Array("""jbn""","""xqpp""","""xqcs""")
    val Entities = line.split("\2")
    Entities.foreach{entity => {
      val label = entity.split("\1")(1)
      label match {
        case "jbn" => dict("jbn") += entity
        case "xqpp" => dict("xqpp") += entity
        case "xqcs" => dict("xqcs") += entity
      }
    }
    }
    val JBN = dict("jbn").toArray
    val XQPP = dict("xqpp").toArray
    val XQCS = dict("xqcs").toArray
    val Arr = JBN ++ XQPP ++ XQCS
    var NewEntities = ArrayBuffer[String]()
    // println("-------------------------------")
    for(ele_1 <- Arr)
    {
      for(ele_2 <- Arr)
      {
        if(ele_1.split("\1")(1) != ele_2.split("\1")(1))
        {
          val NewEntity = "Relation"+ ele_1.split("\1")(1).toString.toUpperCase + "To" + ele_2.split("\1")(1).toString.toUpperCase + ":\t" +ele_1 + "\t-\t" + ele_2
          NewEntities += NewEntity
        }
      }
    }
    NewEntities.mkString("\3")
  }
  def main(args : Array[String]):Unit ={
    val conf = new SparkConf().setAppName("UserSearchExtractor")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    //val inputPath = args(0)
    val inputPath = args(0).split(",")

    var mergedCutRes = sc.textFile(inputPath(0))
    //val mergedCutRes = sc.textFile(inputPath)

    for (i <- 1 until inputPath.length) {
      //union之前要先判断数据是否存在
      val pathInput = new Path(inputPath(i))
      if (fileSystem.exists(pathInput)) {
        mergedCutRes = mergedCutRes.union(sc.textFile(inputPath(i)))
      }
    }
    //val Data = mergedCutRes.coalesce(1000)


    //var Data = sc.textFile(inputPath(0))
    val FirstResult = mergedCutRes.repartition(1000).filter(line => line.split("\t")(1).split("\2").length>1).map(line => line.split("\t")(1))
    val SecondResult = FirstResult.map(DataPreProc)
    val ThirdResult = SecondResult.filter(line => line.split("\2").length >1)
    //Relation Extracting
    val ForthResult = ThirdResult.map(RelationExtractor)
    var FifthResult = ForthResult.flatMap(line => line.split("\3")).filter(line => !line.isEmpty)
    //val FinalResult = FifthResult.sortBy(line => line)

    //替换 + 为 \7
    val knowledgeCard = sc.textFile("/home/hdp_lbg_supin/resultdata/knowledge/knowledgeCard/").map(line => line.replaceAll("\\+","\7")).filter(line => line.split("\t")(7) == "True").map(x => (x.split("\t")(0), x.split("\t")(8))).collectAsMap()
    val knowledgeCardMap = sc.broadcast(knowledgeCard)

    //处理relation 前后都有 jbn
    val CompoundNounsPro = FifthResult.map(line => {
      val entities = Array(line.split("\t")(1),line.split("\t")(3))
      var EntCol = ArrayBuffer[String]()
      var Marker = false
      for(entity <- entities) {
        val CompundN = entity.split("\1")(0)
        // println(knowledgeCardMap.keys.toArray.contains(CompundN))
        if (knowledgeCardMap.value.contains(CompundN)) {
          val AddEntity = knowledgeCardMap.value.get(CompundN).toString
          if (AddEntity.split("_")(1) == "cs") {
            val tempLine = line.replaceAll(CompundN, AddEntity.split(":")(0).split("\7")(1))
            val NewEntity_1 = AddEntity.split(":")(0).split("\7")(0) + "\1xqcs"
            val NewEntity_2 = AddEntity.split(":")(0).split("\7")(1) + "\1jbn"
            val FinalNewLine_1 = "RelationCSToJBN:\t" + NewEntity_1 + "\t-\t" + NewEntity_2
            val FinalNewLine_2 = "RelationJBNToCS:\t" + NewEntity_2 + "\t-\t" + NewEntity_1
            EntCol += tempLine
            EntCol += FinalNewLine_1
            EntCol += FinalNewLine_2
            Marker = true
          }
          else if (AddEntity.split("_")(1) == "pp") {
            val tempLine = line.replaceAll(CompundN, AddEntity.split(":")(0).split("\7")(1))
            val NewEntity_1 = AddEntity.split(":")(0).split("\7")(0) + "\1xqpp"
            val NewEntity_2 = AddEntity.split(":")(0).split("\7")(1) + "\1jbn"
            val FinalNewLine_1 = "RelationPPToJBN:\t" + NewEntity_1 + "\t-\t" + NewEntity_2
            val FinalNewLine_2 = "RelationJBNToPP:\t" + NewEntity_2 + "\t-\t" + NewEntity_1
            EntCol += tempLine
            EntCol += FinalNewLine_1
            EntCol += FinalNewLine_2
            Marker = true
          }
          else{
            if(Marker == false)
            {
              EntCol += line
              Marker = true
            }
          }
        }
        else
        {
          if(Marker == false){
            EntCol += line
            Marker = true
          }
        }
      }
      EntCol.mkString("\3")
    }
    )

    val resultss = CompoundNounsPro.flatMap(line => line.split("\3"))
    resultss.saveAsTextFile(args(1))
  }

}
