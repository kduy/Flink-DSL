package org.apache.flink.fsql

import org.scalacheck.Test.{check, Parameters}
import org.scalacheck.{Test, Gen, Properties}
import org.scalacheck.Prop._




object parserTest extends Properties("parserTest") with FsqlParser{
 /* property("list tail") =
    forAll { (x: Int, xs: List[Int]) =>       
      (x::xs).tail == xs     
    }
  property("list head") = 
    forAll { 
      xs: List[Int] =>     if (xs.isEmpty)       
      throws(classOf[NoSuchElementException]) { xs.head }     
      else       
        xs.head == xs(0)   
    }

  val schemaString = Gen.oneOf(
    Seq("create schema name1 (a boolean) extends parents",
    "create schema name1 (a boolean)")
  )
  
  property("schemaString") =
    forAll(schemaString) {
      (str : String) => {
        val result = parseAll (createSchemaStmtSyntax,str ) match {
          case Success(r, n) => {
            println(r)
            1
          }
          case _ => {
            print("nothing")
            throws(classOf[NoSuchElementException])
            {1}
          }
        }
        result == 1
      }

    }
*/
  val selectString = Gen.oneOf(
    Seq(
     //"select * from stream",
     //"select id, s.speed, stream.time from stream as s",
     //"select id + 3 from stream as s where id = 2 or (speed > 3 and time = 1)",
    "select id from stream [size 3 min on time every 1 partitioned on time]")
  )

  val select =
    forAll(selectString) {
      (str : String) => {
        val result = parseAll (selectStmtSyntax,str ) match {
          case Success(r, n) => {
            println(r)
            1
          }
          case error => {
            print(error)
            throws(classOf[NoSuchElementException])
            {1}
          }
        }
        result == 1
      }

    }

  val myParams = new Parameters.Default {
    override val minSuccessfulTests = 3
  }
  
  Test.check(myParams,select)
}