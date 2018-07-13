package com.bj58.opt.zhaopin.nlp.knowledgeExtractor.utils
import org.neo4j.driver.v1._

/**
  * Created by Cheney WANG on 2018/7/9.
  */
object Neo4jUtils {
  lazy val driver:Driver = GraphDatabase.driver( "bolt://xxx:xxx:xxx:xxxx", AuthTokens.basic( "neo4j", "neo4j" ) )
  lazy val session:Session = driver.session()
}
