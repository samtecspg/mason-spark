package mason.spark

import mason.spark.configs.JobConfig

object Main {
  def main(args: Array[String]) {
    val config = JobConfig()
    val job = config.job
    println("job==" + job)
    config.run()
  }
}
