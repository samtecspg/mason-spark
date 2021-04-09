package util

import org.apache.spark.sql.DataFrame

object Spark {

  def assertEquals(df: DataFrame, data: String) = {
    val splitted = data.split("\n")
    val a = splitted
      .zipWithIndex.filter{r => (Array(0,1,2,3,splitted.length,splitted.length - 1, splitted.length - 2).contains(r._2) == false)}
      .map{r => r._1.stripPrefix("|").stripSuffix("|").split('|').map(_.trim()).toSeq.mkString(",")}.toSeq.sorted.mkString(",")

    // Strictly a stringly test for now
    val b = df.collect().map{r =>
      r.toSeq.map{a => if (a == null) { "null" } else { a.toString() } }.mkString(",")
    }.toSeq.sorted.mkString(",")
    assert(a == b)
  }

}
