package TestFeatures
import shapeless.poly._
import shapeless._

//https://github.com/milessabin/shapeless/tree/master/examples/src/main/scala/shapeless/examples

object shapelessTest extends App {

  // Polymorphic func values
  object choose extends (Set ~> Option) {
    def apply[T](s : Set[T]) = s.headOption
  }
  println(choose(Set(1,2,3)))



  //


}

  object size extends Poly1 {
    implicit def caseInt = at[Int](x => 1) // Poly1.this.type
    implicit def caseString = at[String](_.length)
    implicit def caseTuple[T, U]
    (implicit st : Case.Aux[T, Int], su : Case.Aux[U, Int]) =
      at[(T, U)](t => size(t._1)+size(t._2))

    //size(23)
    //size(((23, "foo"), 12))

  }


// heterogenous lists
object heteList extends App {

  class BiMapIS[K, V]

  implicit val intToString = new BiMapIS[Int, String]
  implicit val stringToInt = new BiMapIS[String, Int]
  
  val hm = HMap[BiMapIS](23 -> "foo", "bar" -> 13)
  
    /*
      scala> hm.get(23)
      res0: Option[String] = Some(foo)
      
      scala> hm.get("bar")
      res1: Option[Int] = Some(13)
    */
    println(hm.get(23))

    val l  = 23 :: "bar" :: HNil

    //println(l.map(hm)) ////// fail ?
}


/*

object SingletonTypeLit  extends App{
  import shapeless._, syntax.singleton._
  
  23.narrow // narrow this value to its type
  
  val (wTrue, wFalse) = (Witness(true), Witness(false))
  type True = wTrue.T
  type False = wFalse.T
  
  trait Select[B] {type Out}
  implicit val selInt = new Select[True] {type Out = Int}
  implicit val selString = new Select[False] {type Out = String}

  def select[T](b: WitnessWith[Select])(t: b.Out) = t
  
  //select(true)(23)  // Int = 
  //select(false)(23) // error
}



object ExtRecords extends App {
  import shapeless._, syntax.singleton._ , record._

  val book =
      ("author" ->> "Benjamin Pierce") ::
      ("title"  ->> "Types and Programming Languages") ::
      ("id"     ->>  262162091) ::
      ("price"  ->>  44.11) ::
      HNil
  
  book("author")
  book("title")
  book.keys
  book.values
  val newPrice = book("price").asInstanceOf[Double]+2.0
  val updated = book + ("price" ->> newPrice)
  updated("price")
  val extended = updated + ("inPrint" ->> true)
  val remove = extended - "id"
  
}


// Coproducts and discriminated unions



object CoProductsTest extends App {
  type ISB = Int :+: String :+: Boolean :+: CNil
  val isb = Coproduct[ISB]("foo")  // String ->> "foo", put foo to String group
  
  isb.select[Int] // None
  isb.select[String] // Some(foo)

  object size extends Poly1 {
    implicit def caseInt = at[Int](i => (i, i))
    implicit def caseString = at[String](s => (s, s.length))
    implicit def caseBoolean = at[Boolean](b => (b, 1))
  }
  
  val newPro = isb map size
  //newPro: (Int, Int) :+: (String, Int) :+: (Boolean, Int) :+: CNil = (foo,3)
  
  
  newPro.select[(String, Int)] // Some((foo,3))
  
}

object Records extends App {
  import record.RecordType, syntax.singleton._, union._
  import record.RecordType
  import syntax.singleton._
  import union._
  
  
  val uSchema = RecordType.like('i ->> 23 :: 's ->> "foo" :: 'b ->> true :: HNil)
  type U = uSchema.Union


  val u = Coproduct[U]('s ->> "foo")  // Inject a String into the union at label 's
  u.get('i)   // Nothing at 'i
  u.get('s)   // Something at 's
}

object Rep {

  // generic representation of (sealed families of) case classes


  /*Generic[T], where T is a case class or an abstract type at the root of a case class hierarchy,
maps between values of T and a generic sum of products representation (HLists and Coproducts),
* * */
  case class Foo(i: Int, s: String, b: Boolean)
  //defined class Foo

  val fooGen = Generic[Foo]
  //fooGen: shapeless.Generic[Foo]{ type Repr = Int :: String :: Boolean :: HNil } = $1$$1@724d2dfe

  val foo = Foo(23, "foo", true)
  //foo: Foo = Foo(23,foo,true)

  fooGen.to(foo)
  //res0: fooGen.Repr = 23 :: foo :: true :: HNil

//  13 :: res0.tail
  //res1: Int :: String :: Boolean :: HNil = 13 :: foo :: true :: HNil
  

//  fooGen.from(res1)
  //res2: Foo = Foo(13, foo, true)


}



*/











/*

object lens2 extends App {
  import shapeless._

  // A pair of ordinary case classes ...
  case class Address(street : String, city : String, postcode : String)
  case class Person(name : String, age : Int, address : Address)

  // Some lenses over Person/Address ...
  val nameLens     = lens[Person] >> 'name
  val ageLens      = lens[Person] >> 'age
  val addressLens  = lens[Person] >> 'address
  val streetLens   = lens[Person] >> 'address >> 'street
  val cityLens     = lens[Person] >> 'address >> 'city
  val postcodeLens = lens[Person] >> 'address >> 'postcode

  val person = Person("Joe Grey", 37, Address("Southover Street", "Brighton", "BN2 9UA"))
  val age1 = ageLens.get(person)
  
  //https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-2.0.0
  
}
*/























object record extends App{
  import syntax.RecordOps

  implicit def recordOps[L <: HList](l : L) : RecordOps[L] = new RecordOps(l)

  /**
   * The type of fields with keys of singleton type `K` and value type `V`.
   */
  type FieldType[K, V] = V with KeyTag[K, V]
  trait KeyTag[K, V]

  /**
   * Yields a result encoding the supplied value with the singleton type `K' of its key.
   */
  def field[K] = new FieldBuilder[K]

  class FieldBuilder[K] {
    def apply[V](v : V): FieldType[K, V] = v.asInstanceOf[FieldType[K, V]]
  }
  
  val l = 1::2::3::HNil
  println(l.productArity)
  println(field[Int]("hoho"))
}




/*
object TestExplicitRecordType {
  import shapeless._,labelled._, record._, syntax.singleton._

  object testF extends Poly1 {
    implicit def atFieldType[F, V](implicit wk: shapeless.Witness.Aux[F]) = at[FieldType[F, V]] {
      f => wk.value.toString
    }
  }

  type Error = Record.`"k1" -> String, "k2" -> Long`.T

  // That seems to work...
  val err1        = "k1" ->> "1" :: "k2" ->> 1L :: HNil
  val err2: Error = "k1" ->> "1" :: "k2" ->> 1L :: HNil

  testF(err1.head)  // OK
  testF(err2.head)  // OK
}*/

