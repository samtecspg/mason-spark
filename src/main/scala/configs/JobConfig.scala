package mason.spark.configs

import mason.spark.jobs.MergeJob
//import org.apache.log4j.{Logger}

case class JobConfig(job: String = "merge") {

  def run(): Unit = {
    job match {
      case "merge" => (new MergeJob).run(this)
      case c: String => {
        // Logger.getLogger("mason.spark").error(s"Job type ${c} not supported")
        println(s"Job type ${c} not supported")
      }
    }
  }

}
