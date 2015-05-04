package org.apache.flink.fsql

import java.sql.{Types => JdbcTypes}

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.parsing.combinator._

trait FsqlParser extends RegexParsers with PackratParsers with Ast.Unresolved{
  import Ast._
  
  def parseAllWith (p: Parser[Statement], sql: String) = getParsingResult(parseAll(p, sql))
  
  def getParsingResult(res: ParseResult[Statement]) = res match {
    case Success(r , q) => ok(r)
    case err: NoSuccess => fail(err.msg, err.next.pos.column, err.next.pos.line)
  }


  /**
   * * Top statement
   */
  lazy val stmt = createSchemaStmtSyntax | createStreamStmtSyntax  |  selectStmtSyntax //| deleteStmt   | insertStmt | splitStmt


  /**
   * *  createStmt
   * * create a schema
   * 
   * * createSchemaStmt ::= "create" "schema" IDENT (IDENT| typedColumns) 
						("extends" IDENT ("," IDENT)*)?
	          typedColumns ::= "(" typedColumn ("," typedColumn)*")"
	          typedColumn ::= IDENT DATA_TYPE 
   */
  
  lazy val createSchemaStmtSyntax: Parser[Statement] = "create".i ~> "schema".i ~> ident ~ new_schema ~ opt("extends".i ~> ident) ^^{
    case i ~ n ~ e => createSchema (i, n , e)
  }
  
  lazy val typedColumn :Parser[StructField]= (ident ~ dataType) ^^ {
    case n ~ t => StructField(n, t)
  }
  lazy val anonymous_schema = "("~> rep1sep(typedColumn,",")<~")" ^^ {case columns => Schema(None, columns)}
  //lazy val new_schema =  ident ^^ {case i => Schema[Option[String]]( Some(i), List())} //| anonymous_schema
  lazy val new_schema =  ident ^^ {case i => Schema[Option[String]]( Some(i), List())} |  anonymous_schema


  /**
   *  createStmt 
   *  *  
   *  createStmt ::= "create" "stream" IDENT (IDENT| typedColumns)
						(source)?

	    source 	  ::= ("as" selectStmt) | ("source" rawSource)
	    rawSource ::= "host" "("STRING_LIT","INTEGER")"
					| "file" "("STRING_LIT")"
					| "stream" IDENT 
   *
   */
  
  lazy val createStreamStmtSyntax : Parser[Statement] = 
      "create".i ~> "stream".i ~> ident ~ new_schema ~ opt(source) ^^{
        case i ~ schema ~ source => createStream(i, schema, source)
 
      }
  
  lazy val source : Parser[Source] = raw_source | derived_source
  // TODO: derived_source should be subselect
  lazy val derived_source = "as".i ~> integer ^^ {case i => derivedSource[Option[String]](i)}
  lazy val raw_source = "source".i ~>(host_source | file_source)
  lazy val host_source = "host"~> "(" ~> stringLit  ~ "," ~ integer <~")" ^^ {
    case h ~_~ p => hostSource[Option[String]](h,p)
    
  }
  lazy val file_source = "file" ~> "("~>stringLit<~")" ^^ {
    case path => fileSource[Option[String]](path)
  }

  /**
   *  select
   * */
  lazy val selectStmtSyntax = selectClause ~ fromClause ~ opt(whereClause)^^ {
    case s ~ f ~ w => Select(s,f,w)
    
  }

  // select clause
  lazy val selectClause = "select".i ~> repsep(named, ",")

  
  // named
  lazy val named = expr ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Column(n, _)) ~ a => Named(n,a,c)
    case (c@AllColumns(_)) ~ a => Named("*",a,c)
    case (e@ArithExpr(_, _,_)) ~ a => Named("<constant>", a, e)
    case (c@Constant(_,_)) ~ a => Named("<constant", a, c)
  }
  /*lazy val named = opt("distinct".i) ~> (comparison | arith | simpleTerm) ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Constant(_, _)) ~ a          => Named("<constant>", a, c)
    case (f@Function(n, _)) ~ a          => Named(n, a, f)
    case (c@Column(n, _)) ~ a            => Named(n, a, c)
    case (i@Input()) ~ a                 => Named("?", a, i)
    case (c@AllColumns(_)) ~ a           => Named("*", a, c)
    case (e@ArithExpr(_, _, _)) ~ a      => Named("<constant>", a, e)
    case (c@Comparison1(_, _)) ~ a       => Named("<constant>", a, c)
    case (c@Comparison2(_, _, _)) ~ a    => Named("<constant>", a, c)
    case (c@Comparison3(_, _, _, _)) ~ a => Named("<constant>", a, c)
    case (s@Subselect(_)) ~ a            => Named("subselect", a, s) // may not use
    case (c@Case(_, _)) ~ a              => Named("case", a, c)
  }*/
  
  // expr  (previous: term)
  lazy val expr = arithExpr | simpleExpr
  lazy val exprs : PackratParser[Expr] = "(" ~> repsep(expr, ",") <~ ")"  ^^ ExprList.apply
  
  // TODO: improve arithExpr
  lazy val arithExpr : PackratParser[Expr] = (simpleExpr | arithParens)* (
    "+" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "+", rhs) }
    | "-" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "-", rhs) }
    | "*" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "*", rhs) }
    | "/" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "/", rhs) }
    | "%" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "%", rhs) }
  )
  
  lazy val arithParens : PackratParser[Expr] = "(" ~> arithExpr <~ ")"
  
  lazy val simpleExpr : PackratParser[Expr] = (
    column |
    allColumns|
    optParens(simpleExpr)|
    stringLit ^^ constS |
    numericLit ^^ (n => if (n.contains(".")) constD(n.toDouble) else constL(n.toLong) )
    // caseExpr|
    // functionExpr |
    // extraTerms
    // "?"
    
  )
  
  lazy val allColumns = 
    opt(ident <~ ".") <~ "*" ^^ (schema => AllColumns(schema))
  lazy val column = (
    ident ~ "." ~ ident ^^ {case s ~ _ ~  c => col(c, Some(s))}
    | ident ^^ (c => col(c, None))
  )
  private def col(name: String , schema: Option[String]) = 
    Column(name, schema)


  // from clause
  lazy val fromClause = "from".i ~> streamReference


  // stream Reference
  lazy val streamReference = rawStream //| derivedStream | joinedWindowStream

  // raw (Windowed)Stream
  lazy val rawStream = ident ~ opt("as".i ~> ident)  ^^ {
    case n ~ a => Stream(n, a)
  }

  //lazy val rawStream = ident ~ opt(windowSpec) ~ opt("as".i~>ident)
  //lazy val windowSpec = ???
  
  


  // where clause
  lazy val whereClause = "where".i ~> predicate ^^ Where.apply
  
  lazy val predicate : PackratParser[Predicate]  = (simplePredicate | parens | notPredicate)* (
    "and".i ^^^ {(p1: Predicate, p2: Predicate) => And(p1,p2)}
    |"or".i ^^^ {(p1: Predicate, p2: Predicate) => Or(p1,p2)}
  )

  lazy val parens: PackratParser[Predicate] = "(" ~> predicate  <~ ")"
  lazy val notPredicate: PackratParser[Predicate] = "not".i ~> predicate ^^ Not.apply

  lazy val simplePredicate : PackratParser[Predicate] = (
    expr ~ "="  ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Eq, rhs) }
      | expr ~ "!=" ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Neq, rhs) }
      | expr ~ "<>" ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Neq, rhs) }
      | expr ~ "<"  ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Lt, rhs) }
      | expr ~ ">"  ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Gt, rhs) }
      | expr ~ "<=" ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Le, rhs) }
      | expr ~ ">=" ~ expr          ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Ge, rhs) }
      | expr ~ "like".i ~ expr      ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Like, rhs) }
      //| expr ~ "in".i ~ (exprs | subselect) ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, In, rhs) }
      //| expr ~ "not".i ~ "in".i ~ (exprs | subselect) ^^ { case lhs ~ _ ~ _ ~ rhs => Comparison2(lhs, NotIn, rhs) }
      | expr ~ "between".i ~ expr ~ "and".i ~ expr ^^ { case t1 ~ _ ~ t2 ~ _ ~ t3 => Comparison3(t1, Between, t2, t3) }
      | expr ~ "not".i ~ "between".i ~ expr ~ "and".i ~ expr ^^ { case t1 ~ _ ~ _ ~ t2 ~ _ ~ t3 => Comparison3(t1, NotBetween, t2, t3) }
      | expr <~ "is".i ~ "null".i           ^^ { t => Comparison1(t, IsNull) }
      | expr <~ "is".i ~ "not".i ~ "null".i ^^ { t => Comparison1(t, IsNotNull) }
      //| "exists".i ~> subselect             ^^ { t => Comparison1(t, Exists) }
      //| "not" ~> "exists".i ~> subselect    ^^ { t => Comparison1(t, NotExists)}

    )
  
  

  /**
   * delete
   * */

  
  
  /**
   * ==============================================
   *  Utilities
   * ==============================================
   */
  
  def optParens[A](p: PackratParser[A]): PackratParser[A] = (
    "(" ~> p <~ ")" | p
    )
  
  

  /**
   * ==============================================
   *  LEXICAL
   * ==============================================  
   */

  /**
   *  reserved keyword 
   */
  
  lazy val reserved =
    ("select".i | "delete".i | "insert".i | "update".i | "from".i | "into".i | "where".i | "as".i |
      "and".i | "or".i | "join".i | "inner".i | "outer".i | "left".i | "right".i | "on".i | "group".i |
      "by".i | "having".i | "limit".i | "offset".i | "order".i | "asc".i | "desc".i | "distinct".i |
      "is".i | "not".i | "null".i | "between".i | "in".i | "exists".i | "values".i | "create".i |
      "set".i | "union".i | "except".i | "intersect".i|
    
      "window".i| "schema".i)
  
  implicit class KeywordOpts(kw: String) {
    def i = keyword(kw)
  }
  def keyword(kw: String) : Parser[String] =  ("(?i)"+ kw + "\\b").r

  /**
   *  identity 
   */
  lazy val ident = rawIdent | quotedIdent
  
  lazy val rawIdent = not(reserved) ~> identValue

  lazy val quotedIdent = quoteChar ~> identValue <~ quoteChar
  def quoteChar :Parser[String] = "\""

  lazy val identValue : Parser[String] = "[a-zA-Z][a-zA-Z0-9_-]*".r

  
  /**
   *  basic type 
   */
  lazy val stringLit : Parser[String]=     "'" ~ """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""".r ~ "'" ^^ { case _ ~ s ~ _ => s }
//  ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"\"").r

  lazy val numericLit : Parser[String] = """(\d+(\.\d*)?|\d*\.\d+)""".r // decimalNumber

  lazy val integer : Parser[Int] = """-?\d+""".r ^^  (s => s.toInt)

  /**
   *  Basic type name 
   */

  lazy val dataType =  "int".i | "string".i | "double".i  | "date".i | "byte".i | "short".i | "long".i | "float".i| "character".i | "boolean".i

  /**
   *  Constant 
   */
  def constB(b: Boolean)       = const((typeOf[Boolean], JdbcTypes.BOOLEAN), b)
  def constS(s: String)        = const((typeOf[String], JdbcTypes.VARCHAR), s)
  def constD(d: Double)        = const((typeOf[Double], JdbcTypes.DOUBLE), d)
  def constL(l: Long)          = const((typeOf[Long], JdbcTypes.BIGINT), l)
  def constNull                = const((typeOf[AnyRef], JdbcTypes.JAVA_OBJECT), null)
  def const(tpe: (Type, Int), x: Any) = Constant[Option[String]](tpe, x)


  /**
   * ==============================================
   *  Test
   * ==============================================
   */
    
  def printCreateSchemaParser = parseAll(createSchemaStmtSyntax, "create schema name1 (a boolean) extends parents") match {
    case Success(r, n) => println(r)
    case _ => print("nothing")
  }

  def printCreateStreamParser = parseAll(createStreamStmtSyntax, "create stream name1 name2 source file ('path')") match {
    case Success(r, n) => println(r)
    case _ => print("nothing")
  }
  
}

object TestFsql extends FsqlParser{
  def main(args: Array[String]) {
    printCreateSchemaParser
    printCreateStreamParser
  }
  
}
