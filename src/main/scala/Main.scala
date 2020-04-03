package mason.spark

import mason.spark.configs.{JobConfig, MergeConfig}

object Main {
  def main(args: Array[String]) {

    val jc = JobConfig.zero
    jc.parser.parse(args, jc) match {
      case Some(config) =>
        if (config.job == "merge") {
          val mc = MergeConfig.zero
          mc.parser.parse(args, mc) match {
            case Some(mergeConfig) => {
              mergeConfig.run()
            }
            case None => println(s"Bad merge config specification ${args}")
          }
        }
      case None => println(s"Bad config specification ${args}")
    }
  }

}

