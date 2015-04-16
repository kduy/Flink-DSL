package sqltyped

import scala.reflect.runtime.universe._

private[sqltyped] object Ast {
  // Types used for AST when references to tables are not yet resolved
  // (table is optional string reference).
  trait Unresolved {
    type Expr           = Ast.Expr[Option[String]]
    type Term           = Ast.Term[Option[String]]
    type Named          = Ast.Named[Option[String]]
    type Statement      = Ast.Statement[Option[String]]
    type ArithExpr      = Ast.ArithExpr[Option[String]]
    type Comparison     = Ast.Comparison[Option[String]]
    type Column         = Ast.Column[Option[String]]
    type Function       = Ast.Function[Option[String]]
    type Constant       = Ast.Constant[Option[String]]
    type Case           = Ast.Case[Option[String]]
    type Select         = Ast.Select[Option[String]]
    type Join           = Ast.Join[Option[String]]
    type JoinType       = Ast.JoinType[Option[String]]
    type TableReference = Ast.TableReference[Option[String]]
    type ConcreteTable  = Ast.ConcreteTable[Option[String]]
    type DerivedTable   = Ast.DerivedTable[Option[String]]
    type Where          = Ast.Where[Option[String]]
    type OrderBy        = Ast.OrderBy[Option[String]]
    type Limit          = Ast.Limit[Option[String]]
  }
  object Unresolved extends Unresolved

  // Types used for AST when references to tables are resolved
  trait Resolved {
    type Expr           = Ast.Expr[Table]
    type Term           = Ast.Term[Table]
    type Named          = Ast.Named[Table]
    type Statement      = Ast.Statement[Table]
    type ArithExpr      = Ast.ArithExpr[Table]
    type Comparison     = Ast.Comparison[Table]
    type Column         = Ast.Column[Table]
    type Function       = Ast.Function[Table]
    type Constant       = Ast.Constant[Table]
    type Case           = Ast.Case[Table]
    type Select         = Ast.Select[Table]
    type Join           = Ast.Join[Table]
    type JoinType       = Ast.JoinType[Table]
    type TableReference = Ast.TableReference[Table]
    type ConcreteTable  = Ast.ConcreteTable[Table]
    type DerivedTable   = Ast.DerivedTable[Table]
    type Where          = Ast.Where[Table]
    type OrderBy        = Ast.OrderBy[Table]
    type Limit          = Ast.Limit[Table]
  }
  object Resolved extends Resolved

  sealed trait Term[T]

  case class Named[T](name: String, alias: Option[String], term: Term[T]) {
    def aname = alias getOrElse name
  }

  case class Constant[T](tpe: (Type, Int), value: Any) extends Term[T]
  case class Column[T](name: String, table: T) extends Term[T]
  case class AllColumns[T](table: T) extends Term[T]
  case class Function[T](name: String, params: List[Expr[T]]) extends Term[T]
  case class ArithExpr[T](lhs: Term[T], op: String, rhs: Term[T]) extends Term[T]
  case class Input[T]() extends Term[T]
  case class Subselect[T](select: Select[T]) extends Term[T]
  case class TermList[T](terms: List[Term[T]]) extends Term[T]
  case class Case[T](conditions: List[(Expr[T], Term[T])], elze: Option[Term[T]]) extends Term[T]

  case class Table(name: String, alias: Option[String])

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

  sealed trait Expr[T] {
    def find(p: Expr[T] => Boolean): Option[Expr[T]] =
      if (p(this)) Some(this)
      else this match {
        case And(e1, e2) => e1.find(p) orElse e2.find(p)
        case Or(e1, e2)  => e1.find(p) orElse e2.find(p)
        case _ => None
      }
  }

  sealed trait Comparison[T] extends Expr[T] with Term[T]

  case class DataType(name: String, precision: List[Int] = Nil)

  case class SimpleExpr[T](term: Term[T]) extends Expr[T]
  case class Comparison1[T](term: Term[T], op: Operator1) extends Comparison[T]
  case class Comparison2[T](lhs: Term[T], op: Operator2, rhs: Term[T]) extends Comparison[T]
  case class Comparison3[T](t1: Term[T], op: Operator3, t2: Term[T], t3: Term[T]) extends Comparison[T]
  case class And[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]
  case class Or[T](e1: Expr[T], e2: Expr[T]) extends Expr[T]
  case class Not[T](e: Expr[T]) extends Expr[T]
  case class TypeExpr[T](dataType: DataType) extends Expr[T]

  // Parametrized by Table type (Option[String] or Table)
  sealed trait Statement[T] {
    def tables: List[Table]
    def isQuery = false
  }







  def isProjectedByJoin(stmt: Statement[Table], col: Column[Table]): Option[Join[Table]] = stmt match {
    case Select(_, tableRefs, _, _, _, _) => (tableRefs flatMap {
      case ConcreteTable(t, joins) => if (col.table == t) None else isProjectedByJoin(joins, col)
      case DerivedTable(_, _, joins) => isProjectedByJoin(joins, col)
    }).headOption
    case Composed(l, r) => isProjectedByJoin(l, col) orElse isProjectedByJoin(r, col)
    case SetStatement(l, _, r, _, _) => isProjectedByJoin(l, col) orElse isProjectedByJoin(r, col)
    case _ => None
  }

  def isProjectedByJoin(joins: List[Join[Table]], col: Column[Table]): Option[Join[Table]] =
    joins.flatMap(j => isProjectedByJoin(j, col)).headOption

  def isProjectedByJoin(join: Join[Table], col: Column[Table]): Option[Join[Table]] = join.table match {
    case ConcreteTable(t, joins) => if (col.table == t) Some(join) else isProjectedByJoin(joins, col)
    case DerivedTable(_, _, joins) => isProjectedByJoin(joins, col)
  }


  
  case class Delete[T](tables: List[Table], where: Option[Where[T]]) extends Statement[T]

  sealed trait InsertInput[T]
  case class ListedInput[T](values: List[Term[T]]) extends InsertInput[T]
  case class SelectedInput[T](select: Select[T]) extends InsertInput[T]

  case class Insert[T](table: Table, colNames: Option[List[String]], insertInput: InsertInput[T]) extends Statement[T] {
    def output = Nil
    def tables = table :: Nil
  }

  case class SetStatement[T](left: Statement[T], op: String, right: Statement[T], 
                             orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T] {
    def tables = left.tables ::: right.tables
    override def isQuery = true
  }

  case class Update[T](tables: List[Table], set: List[(Column[T], Term[T])], where: Option[Where[T]], 
                       orderBy: Option[OrderBy[T]], limit: Option[Limit[T]]) extends Statement[T]

  case class Create[T]() extends Statement[T] {
    def tables = Nil
  }

  case class Composed[T](left: Statement[T], right: Statement[T]) extends Statement[T] {
    def tables = left.tables ::: right.tables
    override def isQuery = left.isQuery || right.isQuery
  }

  case class Select[T](projection: List[Named[T]], 
                       tableReferences: List[TableReference[T]], // should be NonEmptyList
                       where: Option[Where[T]], 
                       groupBy: Option[GroupBy[T]],
                       orderBy: Option[OrderBy[T]],
                       limit: Option[Limit[T]]) extends Statement[T] {
    def tables = tableReferences flatMap (_.tables)
    override def isQuery = true
  }

  sealed trait TableReference[T] {
    def tables: List[Table]
    def name: String
  }
  case class ConcreteTable[T](table: Table, join: List[Join[T]]) extends TableReference[T] {
    def tables = table :: join.flatMap(_.table.tables)
    def name = table.name
  }
  case class DerivedTable[T](name: String, subselect: Select[T], join: List[Join[T]]) extends TableReference[T] {
    def tables = Table(name, None) :: join.flatMap(_.table.tables)
  }

  case class Where[T](expr: Expr[T])

  case class Join[T](table: TableReference[T], joinType: Option[JoinType[T]], joinDesc: JoinDesc)

  sealed trait JoinDesc
  case object Inner extends JoinDesc
  case object LeftOuter extends JoinDesc
  case object RightOuter extends JoinDesc
  case object FullOuter extends JoinDesc
  case object Cross extends JoinDesc

  trait JoinType[T]
  case class QualifiedJoin[T](expr: Expr[T]) extends JoinType[T]
  case class NamedColumnsJoin[T](columns: List[String]) extends JoinType[T]

  case class GroupBy[T](terms: List[Term[T]], withRollup: Boolean, having: Option[Having[T]])

  case class Having[T](expr: Expr[T])

  case class OrderBy[T](sort: List[Term[T]], orders: List[Option[Order]])

  sealed trait Order
  case object Asc extends Order
  case object Desc extends Order

  case class Limit[T](count: Either[Int, Input[T]], offset: Option[Either[Int, Input[T]]])
}
