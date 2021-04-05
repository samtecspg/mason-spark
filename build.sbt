name := "mason-spark"

version := "latest"
val scalaMajorVersion = "2.12"
scalaVersion := f"${scalaMajorVersion}.2"
val sparkVersion = "3.0.0"
val hadoopVersion = "2.7.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% s"spark-core" % sparkVersion,
  "org.apache.spark" %% s"spark-sql" % sparkVersion,
  "org.apache.spark" %% s"spark-hive" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "com.holdenkarau" %% s"spark-testing-base" % s"${sparkVersion}_1.0.0",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "org.scalactic" %% "scalactic" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "sun", xs @ _*) => MergeStrategy.first
  case PathList("org", "glassfish", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "datanucleus", xs @ _*) => MergeStrategy.last
  case PathList("io", "netty", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "fasterxml", xs @ _*) => MergeStrategy.last
  case PathList("com", "amazonaws", xs @_*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @_*) => MergeStrategy.last
  case PathList("javax", "xml", xs @_*) => MergeStrategy.last
  case PathList("javax", "jdo", xs @_*) => MergeStrategy.last
  case PathList("org", "objenesis", xs @_*) => MergeStrategy.last
  case PathList("codegen", "config.fmpp") => MergeStrategy.first
  case PathList("io", "github") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "about.html" => MergeStrategy.rename
  case "git.properties" => MergeStrategy.rename
  case "mime.types" => MergeStrategy.rename
  case "jetty-dir.css" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "parquet.thrift" => MergeStrategy.last
  case "plugin.xml" => MergeStrategy.last
  case ".gitkeep" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "module-info.class" => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html"  => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".thrift"  =>  MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml"  =>  MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


