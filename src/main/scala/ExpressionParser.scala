

import scala.util.parsing.combinator.{PackratParsers, JavaTokenParsers}

abstract  class Expression extends Product {
  
  
}

object BasicTypeInfo extends Enumeration {
  
  val INT_TYPE_INFO , LONG_TYPE_INFO, DOUBLE_TYPE_INFO,FLOAT_TYPE_INFO,BOOLEAN_TYPE_INFO, STRING_TYPE_INFO= Value
}
object Literal {
  def apply(l: Any): Literal = l match {
    case i:Int => Literal(i, BasicTypeInfo.INT_TYPE_INFO)
    case l:Long => Literal(l, BasicTypeInfo.LONG_TYPE_INFO)
    case d: Double => Literal(d, BasicTypeInfo.DOUBLE_TYPE_INFO)
    case f: Float => Literal(f, BasicTypeInfo.FLOAT_TYPE_INFO)
    case str: String => Literal(str, BasicTypeInfo.STRING_TYPE_INFO)
    case bool: Boolean => Literal(bool, BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }
}

case class Literal(value: Any, tpe: BasicTypeInfo.Value){

}


object ExpressionParser extends JavaTokenParsers with PackratParsers {

  // Literals

  lazy val numberLiteral: PackratParser[Expression] =
    ((wholeNumber <~ ("L" | "l")) | floatingPointNumber | decimalNumber | wholeNumber) ^^ {
      str =>
        if (str.endsWith("L") || str.endsWith("l")) {
          Literal(str.toLong)
        } else if (str.matches("""-?\d+""")) {
          Literal(str.toInt)
        } else if (str.endsWith("f") | str.endsWith("F")) {
          Literal(str.toFloat)
        } else {
          Literal(str.toDouble)
        }
    }

  lazy val singleQuoteStringLiteral: Parser[Expression] =
    ("'" + """([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r ^^ {
      str => Literal(str.substring(1, str.length - 1))
    }

  lazy val stringLiteralFlink: PackratParser[Expression] = super.stringLiteral ^^ {
    str => Literal(str.substring(1, str.length - 1))
  }

  lazy val boolLiteral: PackratParser[Expression] = ("true" | "false") ^^ {
    str => Literal(str.toBoolean)
  }

  lazy val literalExpr: PackratParser[Expression] =
    numberLiteral |
      stringLiteralFlink | singleQuoteStringLiteral |
      boolLiteral

  lazy val fieldReference: PackratParser[Expression] = ident ^^ {
    case sym => UnresolvedFieldReference(sym)
  }

  lazy val atom: PackratParser[Expression] =
    ( "(" ~> expression <~ ")" ) | literalExpr | fieldReference

  // suffix ops
  lazy val isNull: PackratParser[Expression] = atom <~ ".isNull" ^^ { e => IsNull(e) }
  lazy val isNotNull: PackratParser[Expression] = atom <~ ".isNotNull" ^^ { e => IsNotNull(e) }

  lazy val abs: PackratParser[Expression] = atom <~ ".abs" ^^ { e => Abs(e) }

  lazy val sum: PackratParser[Expression] = atom <~ ".sum" ^^ { e => Sum(e) }
  lazy val min: PackratParser[Expression] = atom <~ ".min" ^^ { e => Min(e) }
  lazy val max: PackratParser[Expression] = atom <~ ".max" ^^ { e => Max(e) }
  lazy val count: PackratParser[Expression] = atom <~ ".count" ^^ { e => Count(e) }
  lazy val avg: PackratParser[Expression] = atom <~ ".avg" ^^ { e => Avg(e) }

  lazy val as: PackratParser[Expression] = atom ~ ".as(" ~ fieldReference ~ ")" ^^ {
    case e ~ _ ~ as ~ _ => Naming(e, as.name)
  }

  lazy val substring: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ "," ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ ~ to ~ _ => Substring(e, from, to)

    }

  lazy val substringWithoutEnd: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ => Substring(e, from, Literal(Integer.MAX_VALUE))

    }

  lazy val suffix =
    isNull | isNotNull |
      abs | sum | min | max | count | avg |
      substring | substringWithoutEnd | atom


  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> suffix ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> suffix ^^ { e => UnaryMinus(e) }

  lazy val unaryBitwiseNot: PackratParser[Expression] = "~" ~> suffix ^^ { e => BitwiseNot(e) }

  lazy val unary = unaryNot | unaryMinus | unaryBitwiseNot | suffix

  // binary bitwise opts

  lazy val binaryBitwise = unary * (
    "&" ^^^ { (a:Expression, b:Expression) => BitwiseAnd(a,b) } |
      "|" ^^^ { (a:Expression, b:Expression) => BitwiseOr(a,b) } |
      "^" ^^^ { (a:Expression, b:Expression) => BitwiseXor(a,b) } )

  // arithmetic

  lazy val product = binaryBitwise * (
    "*" ^^^ { (a:Expression, b:Expression) => Mul(a,b) } |
      "/" ^^^ { (a:Expression, b:Expression) => Div(a,b) } |
      "%" ^^^ { (a:Expression, b:Expression) => Mod(a,b) } )

  lazy val term = product * (
    "+" ^^^ { (a:Expression, b:Expression) => Plus(a,b) } |
      "-" ^^^ { (a:Expression, b:Expression) => Minus(a,b) } )

  // Comparison

  lazy val equalTo: PackratParser[Expression] = term ~ "===" ~ term ^^ {
    case l ~ _ ~ r => EqualTo(l, r)
  }

  lazy val equalToAlt: PackratParser[Expression] = term ~ "=" ~ term ^^ {
    case l ~ _ ~ r => EqualTo(l, r)
  }

  lazy val notEqualTo: PackratParser[Expression] = term ~ "!==" ~ term ^^ {
    case l ~ _ ~ r => NotEqualTo(l, r)
  }

  lazy val greaterThan: PackratParser[Expression] = term ~ ">" ~ term ^^ {
    case l ~ _ ~ r => GreaterThan(l, r)
  }

  lazy val greaterThanOrEqual: PackratParser[Expression] = term ~ ">=" ~ term ^^ {
    case l ~ _ ~ r => GreaterThanOrEqual(l, r)
  }

  lazy val lessThan: PackratParser[Expression] = term ~ "<" ~ term ^^ {
    case l ~ _ ~ r => LessThan(l, r)
  }

  lazy val lessThanOrEqual: PackratParser[Expression] = term ~ "<=" ~ term ^^ {
    case l ~ _ ~ r => LessThanOrEqual(l, r)
  }

  lazy val comparison: PackratParser[Expression] =
    equalTo | equalToAlt | notEqualTo |
      greaterThan | greaterThanOrEqual |
      lessThan | lessThanOrEqual | term

  // logic

  lazy val logic = comparison * (
    "&&" ^^^ { (a:Expression, b:Expression) => And(a,b) } |
      "||" ^^^ { (a:Expression, b:Expression) => Or(a,b) } )

  // alias

  lazy val alias: PackratParser[Expression] = logic ~ "as" ~ fieldReference ^^ {
    case e ~ _ ~ name => Naming(e, name.name)
  } | logic

  lazy val expression: PackratParser[Expression] = alias

  lazy val expressionList: Parser[List[Expression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): List[Expression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case Failure(msg, _) => throw new ExpressionException("Could not parse expression: " + msg)

      case Error(msg, _) => throw new ExpressionException("Could not parse expression: " + msg)
    }
  }

  def parseExpression(exprString: String): Expression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case fail =>
        throw new ExpressionException("Could not parse expression: " + fail.toString)
    }
  }
}
