package org.apache.flink.fsql

import scala.reflect.runtime.universe._

private[fsql] object Ast {

  trait Unresolved {
    type Expr = Ast.Expr[Option[String]]
    type Statement = Ast.Statement[Option[String]]
    type StructField = Ast.StructField[Option[String]]
    type Schema = Ast.Schema[Option[String]]
    type Source = Ast.Source[Option[String]]
    
    type Select = Ast.Select[Option[String]]
    type Predicate = Ast.Predicate[Option[String]]
    
  }
  object Unresolved extends  Unresolved

  /**
   *  STATEMENT
   * @tparam T
   */

  sealed trait Statement[T]

          /**
           *  CREATE A NEW SCHEMA
           * * @tparam T
           */
  sealed trait newSchema[T]
  case class anonymousSchema[T](value: List[StructField[T]])  extends newSchema[T]
  case class namedSchema[T] (name : String) extends newSchema[T]

  case class createSchema[T](s: String, schema: Schema[T], parentSchema: Option[String]) extends Statement[T]


  case class StructField[T](
                             name : String,
                             dataType: String,
                             nullable: Boolean = true) {

    //override def toString : String = s"StructField($name, ${dataType.scalaType}, $nullable )"

  }
  case class Schema[T](name: Option[String], fields: List[StructField[T]])


          /**
           *  CREATE A NEW STREAM
           * */

  case class createStream[T](name : String, schema : Schema[T], source: Option[Source[T]]) extends Statement[T]

  sealed  trait Source[T]
  case class hostSource[T](host: String, port : Int) extends Source[T]
  case class fileSource[T](fileName: String) extends Source[T]
  case class derivedSource[T](i: Int) extends Source[T]
  
  case class Stream(name : String, windowSpec : Option[WindowSpec],alias: Option[String])


  case class Named[T](name: String, alias: Option[String], expr: Expr[T]){
    def aliasName = alias getOrElse name
  }

          /**
           *    SELECT
           * */

  case class Select[T](projection: List[Named[T]],
                       streamReferences: Stream,
                       where: Option[Where[T]]
                        ) extends Statement[T]
  
  case class Where[T](predicate: Predicate[T])

  /**
   *  WINDOW
   */
  
  case class WindowSpec(window: Window, every: Option[Every], partition: Option[Partition])
  case class Window()
  case class Every()
  case class Partition()

  /**
   * * EXPRESSION
   * @tparam T
   */
  // Expression (previously : Term)
  sealed trait Expr[T]
  case class Constant[T](tpe: (Type, Int), value : Any) extends Expr[T]
  case class Column[T](name: String, schema : T) extends Expr[T]
  case class AllColumns[T](schema: T) extends Expr[T]
  case class Function[T](name: String, params:List[Predicate[T]]) extends Expr[T]
  case class ArithExpr[T](lhs:Expr[T], op: String, rhs:Expr[T]) extends Expr[T]
  case class Input[T]() extends Expr[T]
  case class SubSelect[T](select: Select[T]) extends Expr[T]
  case class ExprList[T](exprs: List[Expr[T]]) extends Expr[T]
  case class Case[T](conditions: List[(Predicate[T], Expr[T])], elze: Option[Expr[T]]) extends Expr[T]


  /**
   * * OPERATOR
   */
  // Operator
  sealed trait Operator1
  case object IsNull extends Operator1
  case object IsNotNull extends Operator1
  case object Exists extends Operator1
  case object NotExists extends Operator1

  sealed trait Operator2
  case object Eq extends Operator2
  case object Neq extends Operator2
  case object Lt extends Operator2
  case object Gt extends Operator2
  case object Le extends Operator2
  case object Ge extends Operator2
  case object In extends Operator2
  case object NotIn extends Operator2
  case object Like extends Operator2

  sealed trait Operator3
  case object Between extends Operator3
  case object NotBetween extends Operator3

  /**
   * * PREDICATE
   * @tparam T
   */
  // Predicate
  sealed trait Predicate[T] {
    def find(p: Predicate[T] => Boolean): Option[Predicate[T]] = {
      if (p(this)) Some(this)
      else 
        this match {
          case And(e1, e2) => e1.find(p) orElse e2.find(p)
          case Or(e1, e2)  => e1.find(p) orElse e2.find(p)
          case _ => None
        }
    }
  }
  
  case class And[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
  case class Or[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
  case class Not[T](p: Predicate[T]) extends Predicate[T]
  
  sealed trait SimplePredicate[T] extends Predicate[T]
  case class Comaprison0[T](boolExpr: Expr[T]) extends SimplePredicate[T]
  case class Comparison1[T](expr: Expr[T], op: Operator1) extends SimplePredicate[T]
  case class Comparison2[T](lhs: Expr[T], op: Operator2, rhs: Expr[T]) extends SimplePredicate[T]
  case class Comparison3[T](t: Expr[T], op: Operator3, value1: Expr[T], value2: Expr[T]) extends SimplePredicate[T]


}
