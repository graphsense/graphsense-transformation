package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import scala.annotation.tailrec

object Currency extends Enumeration {
  type Currency = Value
  val EUR, USD = Value
}


object CurrencyConversion {

  private val convertSatoshis = udf[Value, Long, Double, Double] { (satoshi, eurPrice, usdPrice) =>
    val convert: (Long, Double) => Double = (satoshi, price) =>
      (satoshi * price / 1000000 + 0.5).toLong / 100.0
    Value(satoshi, convert(satoshi, eurPrice), convert(satoshi, usdPrice))
  }

  def convertBitcoinValues[A, B](
      marketPrices: Dataset[A],
      joinColumn: String,
      dataFrame: Dataset[B],
      columns: List[String]) = {
    @tailrec
    def processColumns(df: DataFrame, cs: List[String]): DataFrame = {
      if (cs.isEmpty) df
      else processColumns(
        df.withColumn(cs(0), convertSatoshis(col(cs(0)), col("eur"), col("usd"))),
        cs.tail)
    }
    processColumns(dataFrame.join(marketPrices, joinColumn), columns)
      .drop("eur")
      .drop("usd")
  }
}
