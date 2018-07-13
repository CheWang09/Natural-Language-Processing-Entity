package com.bj58.opt.zhaopin.nlp.knowledgeExtractor
import com.bj58.opt.zhaopin.nlp.knowledgeExtractor.utils._
import org.neo4j.driver.v1._
/**
  * Created by CheneyWang on 2018/7/10.
  */
object Neo4jTest {
  def main(args: Array[String]): Unit = {
    var session:Session = utils.Neo4jUtils.session

    writeRelation(session,node1="Robert Zemeckis",label1="Person",node2="Forrest Gump",lable2="Movie",relation="DIRECTED",weight = 6)
    //println(rela)

    var test = "match (a:Person)-[r:DIRECTED]->(b:Movie) where a.name = \"Robert Zemeckis\" and b.name = \"Forrest Gump\" RETURN r.weight AS weight;"
    var result = session.run(test)
    if(result.hasNext())
    {
      val record = result.next()
      println( record.get("weight") )
    }
    session.close()

  }
  def writeRelation(session:Session,node1:String,label1:String,node2:String,lable2:String,relation:String,weight:Int):Unit={
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
      var sm:String = " MATCH (x1:"+label1+"{name:\"" + node1 +"\""+"})"+" , (x2:" +lable2+"{name:\"" + node2 +"\""+"}"+") CREATE (x1)-[r:"+relation+"{weight:"+weight+"}]->(x2)"
      session.run(sm)
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
      }
    }

  }

}
