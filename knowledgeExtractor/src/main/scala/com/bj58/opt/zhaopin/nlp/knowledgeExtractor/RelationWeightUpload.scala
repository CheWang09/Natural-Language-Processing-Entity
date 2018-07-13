package com.bj58.opt.zhaopin.nlp.knowledgeExtractor
import com.bj58.opt.zhaopin.nlp.knowledgeExtractor.utils._
import org.neo4j.driver.v1._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Cheney WANG on 2018/7/12.
  */
object RelationWeightUpload {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RelationWeightUpload")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    //val inputPath = args(0)
    val inputPath = args(0).split(",")
    val outputPath = args(1)

    var RelationData = sc.textFile(inputPath(0))

    for (i <- 1 until inputPath.length) {
      //union之前要先判断数据是否存在
      val pathInput = new Path(inputPath(i))
      if (fileSystem.exists(pathInput)) {
        RelationData = RelationData.union(sc.textFile(inputPath(i)))
      }
    }

    //var Result = RelationData.repartition(1).mapPartitions { eachPartition => {
      //var session: Session = Neo4jUtils.session

    val Result = RelationData.repartition(1).mapPartitions { eachPartition => {
      var session: Session = Neo4jUtils.session

      var yy = eachPartition.map { relation => {
        try{
          var terms = relation.split("\t")
          var label1 =terms(1).split("\1")(1)
          var node1 =terms(1).split("\1")(0)+"_Us_test"
          var label2 = terms(3).split("\1")(1)
          var node2 = terms(3).split("\1")(0)+"_Us_test"
          var relations = terms(0).replace("R","r").replace(":","").replace("T","t")
          val weight = terms(5).toInt

          writeRelation(session,node1,label1,node2,label2,relations,weight)
        }catch{
          case e:Exception => e.getMessage
        }

      } }
      //  session.close()
      yy}
    }

//      var Result = RelationData.repartition(1).map { relation => {
//        try{
//          var terms = relation.split("\t")
//          var label1 =terms(1).split("\1")(1)
//          var node1 =terms(1).split("\1")(0)+"test"
//          var label2 = terms(3).split("\1")(1)
//          var node2 = terms(3).split("\1")(0)+"test"
//          var relations = terms(0).replace("R","r").replace(":","").replace("T","t")
//          val weight = terms(5).toInt
//
//          writeRelation(session,node1,label1,node2,label2,relations,weight)
//        }catch{
//          case e:Exception => e.getMessage
//        }
//      }
//      }
   // session.close()
      Result.saveAsTextFile(outputPath)

  }
  def writeRelation(session:Session,node1:String,label1:String,node2:String,lable2:String,relation:String,weight:Int) :String ={
    var  result = session.run( "MATCH ( x :"+label1+") WHERE x.name = \"" + node1 +"\""+ " RETURN x")
    if(!result.hasNext()){
      session.run( "CREATE ( x :" +label1+" { name : \"" + node1 +"\""+"} )")
    }
    result = session.run( "MATCH ( x :"+lable2+") WHERE x.name = \"" + node2 +"\""+ " RETURN x")
    if(!result.hasNext()){
      session.run( "CREATE ( x :" +lable2+" { name : \"" + node2 +"\""+"} )")
    }

    val sm1:String = " MATCH ( x1:"+label1+")-[r:"+relation+" ]->( x2:"+lable2+")  WHERE x1.name = \"" + node1 +"\"" + "  AND x2.name = \"" + node2 +"\""+"RETURN r"
    println(sm1)
    result =  session.run(sm1)
    //      System.out.println(result.next().get(0).asRelationship().startNodeId())
    if(!result.hasNext()){
      val sm:String = " MATCH (x1:"+label1+"{name:\"" + node1 +"\""+"})"+" , (x2:" +lable2+"{name:\"" + node2 +"\""+"}"+") CREATE (x1)-[r:"+relation+"{weight:"+weight+"}]->(x2)"
      session.run(sm)
      return sm
    }
    else
    {
      val queryweight = " MATCH ( x1:"+label1+")-[r:"+relation+" ]->( x2:"+lable2+")  WHERE x1.name = \"" + node1 +"\"" + "  AND x2.name = \"" + node2 +"\""+"RETURN r.weight AS weight;"
      val result = session.run(queryweight)
      var formerWeight = 0
      if(result.hasNext())
      {
        val record = result.next()
        formerWeight = record.get("weight").asInt()
      }
      val Weight: Int = formerWeight + weight
      if(Weight < Int.MaxValue)
        {
          val sm:String = " MATCH ( x1:"+label1+")-[r:"+relation+" ]->( x2:"+lable2+")  WHERE x1.name = \"" + node1 +"\"" + "  AND x2.name = \"" + node2 +"\""+"SET r.weight = "+Weight
          session.run(sm)
          return sm
        }
    }
    return sm1
  }

}
