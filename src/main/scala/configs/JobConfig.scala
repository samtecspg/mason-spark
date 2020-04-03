package mason.spark.configs

import scopt.OptionParser

object JobConfig {
  def zero = JobConfig("")
}

case class JobConfig(job: String = "merge") {

  val parser = new OptionParser[JobConfig]("mason-spark") {
    head("mason-spark", "0.1")
    opt[String]('j', "job")
      .required()
      .valueName("<job_type>")
      .action((x,c) => c.copy(job = x))
  }

}
