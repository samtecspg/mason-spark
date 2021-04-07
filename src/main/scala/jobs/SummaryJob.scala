package mason.spark.jobs

import mason.spark.configs.SummaryConfig

object SummaryJob {

  def run(config: SummaryConfig): Either[Exception, String] = {
    Left(new Exception("Job not implemented yet"))
  }

}
