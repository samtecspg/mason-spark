package mason.spark.jobs

import mason.spark.configs.JobConfig

abstract class SparkJob extends Serializable {

  def run(config: JobConfig): Unit

}
