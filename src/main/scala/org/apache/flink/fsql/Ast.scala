package org.apache.flink.fsql

import scala.reflect.runtime.universe._

private[fsql] object Ast {

  trait Unresolved {
    type Expr = Ast.Expr[Option[String]]
    type Named  = Ast.Predicate[Option[String]]
    type Statement = Ast.Statement[Option[String]]
    type Source = Ast.Source[Option[String]]
    type WindowedStream = Ast.WindowedStream[Option[String]]
    type Select = Ast.Select[Option[String]]
    type Predicate = Ast.Predicate[Option[String]]
    type PolicyBased = Ast.PolicyBased[Option[String]]
    type Where = Ast.Where[Option[String]]
    type StreamReference = Ast.StreamReferences[Option[String]]
    type ConcreteStream  = Ast.ConcreteStream[Option[String]]
    type DerivedStream  = Ast.DerivedStream[Option[String]]
    type Join           = Ast.Join[Option[String]]
    type JoinSpec       = Ast.JoinSpec[Option[String]]
    type Function       = Ast.Function[Option[String]]

  }
  object Unresolved extends  Unresolved

  /**
   *  STATEMENT
   * @tparam T
   */

  sealed trait Statement[T]{
    def streams : List[Stream]
    def isQuery = false
  }

          /**
           *  CREATE A NEW SCHEMA
           * * @tparam T
           */
  sealed trait newSchema[T]
  case class anonymousSchema[T](value: List[StructField])  extends newSchema[T]
  case class namedSchema[T] (name : String) extends newSchema[T]

  case class createSchema[T](s: String, schema: Schema, parentSchema: Option[String]) extends Statement[T] {
    def streams = Nil
    
  }


  case class StructField(
                             name : String,
                             dataType: String,
                             nullable: Boolean = true) {

    //override def toString : String = s"StructField($name, ${dataType.scalaType}, $nullable )"

  }
  case class Schema(name: Option[String], fields: List[StructField])


          /**
           *  CREATE A NEW STREAM
           * */

  // create a new stream
  case class CreateStream[T](name : String, schema : Schema, source: Option[Source[T]]) extends Statement[T] {
       def streams = Nil
            
  }

  sealed  trait Source[T]
  case class hostSource[T](host: String, port : Int) extends Source[T]
  case class fileSource[T](fileName: String) extends Source[T]
  case class derivedSource[T](i: Int) extends Source[T]
  
  
  sealed trait StreamReferences[T]{

    def streams :List[Stream]
    def name: String
  }
  case class ConcreteStream[T] (windowedStream : WindowedStream[T], join: Option[Join[T]]) extends  StreamReferences[T]{
    def streams = windowedStream.stream :: join.fold(List[Stream]())(_.stream.streams)
    def name = windowedStream.stream.name
    
  }
  case class DerivedStream[T] (name : String, subSelect: Select[T], join: Option[Join[T]]) extends StreamReferences[T]{
    def streams = Stream(name, None) :: join.fold(List[Stream]())(_.stream.streams)
    
  }
  
  case class Stream(name : String, alias: Option[String])
  case class WindowedStream[T](stream: Stream, windowSpec: Option[WindowSpec[T]])
  case class Named[T](name: String, alias: Option[String], expr: Expr[T]){
    def aliasName = alias getOrElse name
  }

          /**
           *    SELECT
           * */

          //TODO: should not have any Nil. for testing purpose only
  case class Select[T](projection: List[Named[T]] ,
                       streamReference: StreamReferences[T],
                       where: Option[Where[T]],
                       groupBy: Option[GroupBy[T]]
                      ) extends Statement[T] {
            
    def streams = streamReference.streams
    override def isQuery = true
  }
  
  case class Where[T](predicate: Predicate[T])

  /**
   *  WINDOW
   */
  
  case class WindowSpec[T](window: Window[T], every: Option[Every[T]], partition: Option[Partition[T]])
  case class Window[T](policyBased: PolicyBased[T])
  case class Every[T](policyBased: PolicyBased[T])
  case class Partition[T](field: Named[T])
  case class PolicyBased[T] (value: Int, timeUnit: Option[String], onField: Option[Column[T]])


  /**
   * * JOIN
   */
  
  case class Join[T] (stream: StreamReferences[T], JoinSpec: Option[JoinSpec[T]], joinDesc: JoinDesc) {}
  
  sealed trait JoinDesc
  case object Cross extends JoinDesc
  case object LeftOuter extends JoinDesc
  
  sealed trait JoinSpec[T]
  case class NamedColumnJoin[T] (columns: String) extends JoinSpec[T]
  case class QualifiedJoin[T](predicate: Predicate[T]) extends JoinSpec[T]
  

  /**
   * * EXPRESSION
   */
  // Expression (previously : Term)
  sealed trait Expr[T]
  case class Constant[T](tpe: (Type, Int), value : Any) extends Expr[T]
  case class Column[T](name: String, stream : T) extends Expr[T]
  case class AllColumns[T](schema: T) extends Expr[T]
  case class Function[T](name: String, params:List[Expr[T]]) extends Expr[T]
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

  
  case class GroupBy[T](exprs: List[Expr[T]], having: Option[Having[T]])
  case class Having[T](predicate: Predicate[T])


  /**
   *  INSERT 
   */
  
  case class  Insert[T](stream: WindowedStream[T], colNames: Option[List[String]], source: Source[T])



  /**************************************************************************************************
   * * 
   *                            RESOLVE
   * * 
   * * *************************************************************************************/


  trait Resolved {
    type Expr = Ast.Expr[Stream]
    type Named  = Ast.Predicate[Stream]
    type Statement = Ast.Statement[Stream]
    type Source = Ast.Source[Stream]
    type WindowedStream = Ast.WindowedStream[Schema]
    type Select = Ast.Select[Stream]
    type Predicate = Ast.Predicate[Stream]
    type PolicyBased = Ast.PolicyBased[Stream]
    type Where = Ast.Where[Stream]
    type StreamReference = Ast.StreamReferences[Stream]
    type ConcreteStream  = Ast.ConcreteStream[Stream]
    type DerivedStream  = Ast.DerivedStream[Stream]
    type Join           = Ast.Join[Stream]
    type JoinSpec       = Ast.JoinSpec[Stream]
    type Function       = Ast.Function[Stream]
  }
  object Resolved extends  Resolved
  
  
  

  def resolvedStreams(stmt : Statement[Option[String]])//: ?[Statement[Stream]]
  = stmt match {
    case s@Select(_,_,_,_) => resolveSelect(s)(stmt.streams)
  }
  
  def resolveSelect (s: Select[Option[String]])(env: List[Stream] = List())= {
    val r = new ResolveEnv(env)
    for {
      p <- r.resolveProj(s.projection)
      //s <- r.resolveStreamRef(s.streamReference)
    } yield p
  }
  
  
  
  
  private class ResolveEnv (env : List[Stream]){
    def resolve(expr : Expr[Option[String]]): ?[Expr[Stream]] = expr  match {
      case c@Column(_,_) => resolveColumn(c)
    }

    //projection
    def resolveProj(proj : List[Named[Option[String]]]) : ?[List[Named[Stream]]]
    = sequence(proj map resolveNamed)
/*

    //StreamReference
    def resolveStreamRef(streamRefs: StreamReferences[Option[String]])= streamRefs match {
      case c@ConcreteStream(_,_,_) => ???
      case d@DerivedStream(_,_,_) => ???
      
    }

    def resolvePolicyBased(based: PolicyBased[Option[String]]): ?[PolicyBased[Stream]] = 
        based.onField match {
          case Some(field) => resolveNamed(field) map (f => based.copy(onField =  Some(f)))
          case None => ???
          
        }


    def resolveWindowing(window: Window[Option[String]]) : ?[Window[Stream]] = {
      resolvePolicyBased(window.policyBased) map (p => window.copy( policyBased = p))
    }

    def resolveWindowedSpec( windowedStream : WindowedStream[Option[String]]) : ?[Option[WindowSpec[Stream]]] =
      windowedStream.windowSpec.fold((None.ok)) {
        spec => for {
          w <- resolveWindowing(spec.window)
          e <- resolveEvery(spec.every  )
          p <- resolvePartition(spec.partition)
          
        } yield spec.copy(window = w, every =  e , partition =  p)
      }
      
    
    
    //where
    
    
    //groupBy

*/
    def resolveNamed(n: Named[Option[String]]) : ?[Named[Stream]]=
      resolve(n.expr) map (e => n.copy(expr =  e))



    def resolveColumn(col: Column[Option[String]]): ?[Column[Stream]] = {
      env find { 
        s =>
          (col.stream, s.alias) match {
            case (Some(ref), None) => println (ref, s.name,ref == s.name);ref == s.name
            case (Some(ref), Some(alias)) => (ref == s.name) || (ref == alias)
            case (None, _) => true // assume that we take the first stream
                                   // if there are more than 1, not working
          }
      } map ( s => col.copy(stream = s)) orFail("Column references unknown")
    }
    
    
  }





}




