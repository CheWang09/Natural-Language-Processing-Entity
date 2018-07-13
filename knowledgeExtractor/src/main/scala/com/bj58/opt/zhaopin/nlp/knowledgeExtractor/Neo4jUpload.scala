package com.bj58.opt.zhaopin.nlp.knowledgeExtractor
import com.bj58.opt.zhaopin.nlp.knowledgeExtractor.utils._
import org.neo4j.driver.v1._
import org.apache.spark.{ SparkConf, SparkContext}

/**
  * Created by Cheney WANG on 2018/7/9.
  */
object Neo4jUpload {
  def main(args:Array[String])={
    var sparkConf = new SparkConf().setAppName("zhaopin-nlp-spark-knowledgeExtractor-RelationUpload")
    var sc = new SparkContext(sparkConf)

    var inputPaths = args(0).split(",")
    var outputPath = args(1)


    var inputRelationData = sc.textFile(inputPaths(0))
    if(inputPaths.length > 1){
      for(i <- 1 to inputPaths.length - 1){
        inputRelationData = sc.textFile(inputPaths(0)).union(inputRelationData)
      }
    }

    var outRes = inputRelationData.repartition(1).mapPartitions { eachPartition => {
      var session: Session = Neo4jUtils.session

      var yy = eachPartition.map { relation => {
        try{
          var terms = relation.split("\t")
          var label1 =terms(1).split("\1")(1)
          var node1 =terms(1).split("\1")(0)+"_Us_test"
          var label2 = terms(3).split("\1")(1)
          var node2 = terms(3).split("\1")(0)+"_Us_test"
          var relations = terms(0).replace("R","r").replace(":","").replace("T","t")

          writeRelation(session,node1,label1,node2,label2,relations)
        }catch{
          case e:Exception => e.getMessage
        }

      } }
      //  session.close()
      yy}
    }
    outRes.saveAsTextFile(outputPath)
  }

  def writeRelation(session:Session,node1:String,label1:String,node2:String,lable2:String,relation:String):String={
    var  result = session.run( "MATCH ( x :"+label1+") WHERE x.name = \"" + node1 +"\""+ " RETURN x")
    if(!result.hasNext()){
      session.run( "CREATE ( x :" +label1+" { name : \"" + node1 +"\""+"} )")
    }
    result = session.run( "MATCH ( x :"+lable2+") WHERE x.name = \"" + node2 +"\""+ " RETURN x")
    if(!result.hasNext()){
      session.run( "CREATE ( x :" +lable2+" { name : \"" + node2 +"\""+"} )")
    }

    var sm1:String = " MATCH ( x1:"+label1+")-[r:"+relation+" ]->( x2:"+lable2+")  WHERE x1.name = \"" + node1 +"\"" + "  AND x2.name = \"" + node2 +"\""+"RETURN r"
    println(sm1)
    result =  session.run(sm1)
    //      System.out.println(result.next().get(0).asRelationship().startNodeId())
    if(!result.hasNext()){
      var sm:String = " MATCH (x1:"+label1+"{name:\"" + node1 +"\""+"})"+" , (x2:" +lable2+"{name:\"" + node2 +"\""+"}"+") CREATE (x1)-[r:"+relation+" ]->(x2)"
      session.run(sm)
      return sm
    }

    return sm1
  }

}
