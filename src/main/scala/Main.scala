package mason.spark

import mason.spark.configs.{MainJobConfig}

object Main {
  def main(args: Array[String]) {

    val argMap = args.grouped(2).map { case Array(p1: String,p2: String) => (p1,p2) }.toArray.groupBy(_._1).map { case (k,v) => (k,v.map(_._2).head)}
    val jobTypeArgs = Array("--job", argMap("--job"))
    val otherArgs = argMap.-("--job").toArray.flatMap{case (k,v) => List(k,v)}

    val result = for {
      config <- MainJobConfig.parser.parse(jobTypeArgs, MainJobConfig.zero).toRight(new Exception("Bad Configuration"))
      run <- config.run(otherArgs)
    } yield run

    result.fold(e => throw e, m => println(s"Succesfull operation: ${m}"))

  }

}


