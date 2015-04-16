package flinkdsl


import scala.util.parsing.combinator._
import java.sql.{Types => JdbcTypes}

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.parsing.combinator._
object Unso {
  trait Un {
    type Term = Unso.Term[Option[String]]
  }

  sealed trait Term[T]
  case class ArithExpr[T](lhs: Term[T], op: String, rhs: Term[T]) extends Term[T]
  case class Column[T](name: String, table: T) extends Term[T]
}


trait ParserTest extends RegexParsers with Unso.Un with PackratParsers{
  import Unso._
  def parseAllWith(p: Parser[Term], sql: String) = (parseAll(p, sql))
  
  
  lazy val arith: PackratParser[Term] = simpleTerm* ("+" ^^^ { (lhs: Term, rhs: Term) => ArithExpr(lhs, "+", rhs) })


  lazy val arithParens = "(" ~> arith <~ ")"

  lazy val simpleTerm: PackratParser[Term] = ( 
    column
  )

  lazy val column = (
    ident ~ "." ~ ident ^^ { case t ~ _ ~ c => col(c, Some(t)) }
      | ident ^^ (c => col(c, None))
    )


  private def col(name: String, table: Option[String]) = Column(name, table)

  

  implicit class KeywordOps(kw: String) {
    def i = keyword(kw)
  }

  def keyword(kw: String): Parser[String] = ("(?i)" + kw + "\\b").r

  def quoteChar: Parser[String] = "\""

  // name table/column.....
  lazy val ident = (quotedIdent | rawIdent)

  lazy val reserved =
    ("select".i | "delete".i | "insert".i | "update".i | "from".i | "into".i | "where".i | "as".i |
      "and".i | "or".i | "join".i | "inner".i | "outer".i | "left".i | "right".i | "on".i | "group".i |
      "by".i | "having".i | "limit".i | "offset".i | "order".i | "asc".i | "desc".i | "distinct".i |
      "is".i | "not".i | "null".i | "between".i | "in".i | "exists".i | "values".i | "create".i |
      "set".i | "union".i | "except".i | "intersect".i)

  lazy val rawIdent = not(reserved) ~> identValue
  lazy val quotedIdent = quoteChar ~> identValue <~ quoteChar

  lazy val stringLit =
    "'" ~ """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""".r ~ "'" ^^ { case _ ~ s ~ _ => s }

  lazy val identValue: Parser[String] = "[a-zA-Z][a-zA-Z0-9_-]*".r
  lazy val numericLit: Parser[String] = """(-)?(\d+(\.\d*)?|\d*\.\d+)""".r
  lazy val integer: Parser[Int] = """\d+""".r ^^ (s => s.toInt)

}


object Test2 extends ParserTest{

  def main(args: Array[String]) {
    println(parseAllWith(arith, "age + hight + width"))
    println("-------" * 10)
    
    
  }


  }
