package com.bj58.opt.zhaopin.nlp.knowledgeExtractor.utils

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by wangche on 2018/7/12.
  */
object Process {
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


}
