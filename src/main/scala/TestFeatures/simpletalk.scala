package TestFeatures

import scala.io.Source
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

object Simpletalk extends StandardTokenParsers with App {
  lexical.reserved += ("print", "space", "repeat", "next", "let", "HELLO", "GOODBYE")
  lexical.delimiters += ("=")

  val input = Source.fromFile("input.talk").getLines.reduceLeft[String](_ + '\n' + _)
  val tokens = new lexical.Scanner(input)

  val result = phrase(program)(tokens)
  result match {
    case Success(tree, _) => new Interpreter(tree).run()

    case e: NoSuccess => {
      Console.err.println(e)
    }
  }

  def program = stmt+

  def stmt: Parser[Statement] = ( "print" ~ expr ^^ { case _ ~ e => Print(e) }
    | "space" ^^^ Space()
    | "repeat" ~ numericLit ~ (stmt+) ~ "next" ^^ {
    case _ ~ times ~ stmts ~ _ => Repeat(times.toInt, stmts)
  }
    | "let" ~ ident ~ "=" ~ expr ^^ { case _ ~ id ~ _ ~ e => Let(id, e) } )

  def expr = ( "HELLO" ^^^ Hello()
    | "GOODBYE" ^^^ Goodbye()
    | stringLit ^^ { case s => Literal(s) }
    | numericLit ^^ { case s => Literal(s) }
    | ident ^^ { case id => Variable(id) } )
}

class Interpreter(tree: List[Statement]) {
  def run() {
    walkTree(tree, EmptyContext)
  }

  private def walkTree(tree: List[Statement], context: Context) {
    tree match {
      case Print(expr) :: rest => {
        println(expr.value(context))
        walkTree(rest, context)
      }

      case Space() :: rest => {
        println()
        walkTree(rest, context)
      }

      case Repeat(times, stmts) :: rest => {
        for (i <- 0 until times) {
          walkTree(stmts, context.child)
        }

        walkTree(rest, context)
      }

      case (binding: Let) :: rest => walkTree(rest, context + binding)

      case Nil => ()
    }
  }
}

class Context(ids: Map[String, Let], parent: Option[Context]) {
  lazy val child = new Context(Map[String, Let](), Some(this))

  def +(binding: Let) = {
    val newIDs = ids + (binding.id -> binding)
    new Context(newIDs, parent)
  }

  def resolve(id: String): Option[Let] = {
    if (ids contains id) {
      Some(ids(id))
    } else {
      parent match {
        case Some(c) => c resolve id
        case None => None
      }
    }
  }
}

object EmptyContext extends Context(Map[String, Let](), None)

sealed abstract class Statement

case class Print(expr: Expression) extends Statement
case class Space() extends Statement

case class Repeat(times: Int, stmts: List[Statement]) extends Statement
case class Let(val id: String, val expr: Expression) extends Statement

sealed abstract class Expression {
  def value(context: Context): String
}

case class Literal(text: String) extends Expression {
  override def value(context: Context) = text
}

case class Variable(id: String) extends Expression {
  override def value(context: Context) = {
    context.resolve(id) match {
      case Some(binding) => binding.expr.value(context)
      case None => throw new RuntimeException("Unknown identifier: " + id)
    }
  }
}

case class Hello() extends Expression {
  override def value(context: Context) = "Hello, World!"
}

case class Goodbye() extends Expression {
  override def value(context: Context) = "Farewell, sweet petunia!"
}