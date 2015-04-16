import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object Builds extends Build {
  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    version := "0.1",
    organization := "Flink",
    scalaVersion := "2.11.5"
  )

  crossScalaVersions := Seq("2.11.5", "2.10.4")
  
  lazy val app = Project(
      "Flink-DSL",
      file("."), 
      settings = buildSettings ++ assemblySettings
    ) settings(
    libraryDependencies := {
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if scala 2.11+ is used, add dependency on scala-xml module
        case Some((2, scalaMajor)) if scalaMajor >= 11 =>
          libraryDependencies.value ++ Seq(
            "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
            "org.scala-lang.modules" %% "scala-xml" % "1.0.3",
            "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),


            "com.chuusai" % "shapeless_2.10.4" % "2.0.0" withSources() withJavadoc(),
            "net.sourceforge.schemacrawler" % "schemacrawler" % "8.17",
            "org.scala-lang" % "scala-reflect" % "2.11.5" withSources() withJavadoc(),
            "org.scala-lang" % "scala-actors" % "2.11.5" % "test",
            "mysql" % "mysql-connector-java" % "5.1.21" % "test",
            "postgresql" % "postgresql" % "9.1-901.jdbc4" % "test"
          )
        case _ =>
          // or just libraryDependencies.value if you don't depend on scala-swing
          libraryDependencies.value :+ "org.scala-lang" % "scala-swing" % scalaVersion.value
      }
    }
    )
}
/*

lazy val macros: Project = Project(
  "macros",
  file("macros"),
  settings = buildSettings ++ Seq(
    // This dependency is needed to get access to the reflection API
    libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _  ),
    //libraryDependencies ++= List("org.scalamacros" %% "quasiquotes" % "2.0.1"  ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if scala 2.11+ is used, quasiquotes are merged into scala-reflect
        case Some((2, scalaMajor)) if scalaMajor >= 11 =>
          libraryDependencies.value
        // in Scala 2.10, quasiquotes are provided by macro paradise
        case Some((2, 10)) =>
          libraryDependencies.value ++ Seq(
            compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
            "org.scalamacros" %% "quasiquotes" % "2.0.0" cross CrossVersion.binary)
      }
    }
  )
)*/
