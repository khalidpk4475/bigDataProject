ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.6"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.26"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

// Add Stanford CoreNLP dependency
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2" classifier "models"

//libraryDependencies += "com.cibo" %% "evilplot-repl" % "0.8.0"
//libraryDependencies += "io.github.cibotech" %%% "evilplot" % "0.9.0"
//libraryDependencies += "graphframes" %% "graphframes" % "0.8.1"
//libraryDependencies += "org.scala-graph" %% "graph-core" % "1.11.5"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.8"
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"
// https://mvnrepository.com/artifact/org.plotly-scala/plotly-render
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.5.4"

//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"
//libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.3.1"


lazy val root = (project in file("."))
  .settings(
    name := "BigDataProject"
  )
