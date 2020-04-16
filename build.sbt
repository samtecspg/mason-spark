name := "mason-spark"

version := "1.02"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "2.7.5",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.14.0" % "test",
  "org.scalactic" %% "scalactic" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)
.map(_.excludeAll(
  ExclusionRule(organization = "org.scalacheck"),
  ExclusionRule(organization = "org.scalactic"),
  ExclusionRule(organization = "org.scalatest")
))

assemblyMergeStrategy in assembly := {
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "sun", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "amazonaws", xs @_*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @_*) => MergeStrategy.last
  case PathList("javax", "xml", xs @_*) => MergeStrategy.last
  case PathList("org", "objenesis", xs @_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("codegen", "config.fmpp") => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "git.properties" => MergeStrategy.rename
  case "mime.types" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "parquet.thrift" => MergeStrategy.last
  case "plugin.xml" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


