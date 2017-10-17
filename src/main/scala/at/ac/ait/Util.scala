package at.ac.ait

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object Util {

  def cache(spark: SparkSession)(ds: Dataset[_], name: String) {
    ds.createTempView(name)
    spark.catalog.cacheTable(name)
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Time: " + (t1 - t0) / 1000 + "s")
    result
  }

}
