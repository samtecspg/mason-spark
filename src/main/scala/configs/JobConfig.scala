package mason.spark.configs

import jobs.{PreviewJob, SummaryJob}
import mason.spark.jobs.{FormatJob, MergeJob}
import scopt.{OptionParser, Zero}

sealed trait JobType
case object FormatType extends JobType
case object PreviewType extends JobType
case object SummaryType extends JobType
case object MergeType extends JobType

trait JobConfig

case class MainJobConfig(job: String = "unknown") {
  val bad: Exception = new Exception("Bad Configuration")

  def run(otherArgs: Array[String]): Either[Exception, String] = job match {
    case "merge" => MergeConfig.parser.parse(otherArgs, MergeConfig.zero).toRight(bad).flatMap(MergeJob.run)
    case "preview" => PreviewConfig.parser.parse(otherArgs, PreviewConfig.zero).toRight(bad).flatMap(PreviewJob.run)
    case "summary" => SummaryConfig.parser.parse(otherArgs, SummaryConfig.zero).toRight(bad).flatMap(SummaryJob.run)
    case "format" => FormatConfig.parser.parse(otherArgs, FormatConfig.zero).toRight(bad).flatMap(FormatJob.run)
    case s: String => Left(new Exception(s"Unsupported job: ${s}"))
  }
}


object MainJobConfig {
  val zero = MainJobConfig("unknown")

  def parser = new OptionParser[MainJobConfig]("mason-spark") {
    opt[String]('j', "job")
      .required()
      .valueName("<job>")
      .action((x,c) => c.copy(job = x))
  }
}

