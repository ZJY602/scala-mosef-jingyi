package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    val separator = autoDetectSeparator(path)
    sparkSession
      .read
      .option("sep", separator)
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }

  // Private method to detect CSV separator
  private def autoDetectSeparator(path: String): String = {
    val sample = sparkSession.read.textFile(path).limit(100).collect().mkString
    val possibleSeparators = Seq(',', ';', '\t', '|', ' ')
    val separatorCounts = possibleSeparators.map { sep =>
      sep -> sample.split(sep).length
    }
    separatorCounts.maxBy(_._2)._1.toString
  }

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format("parquet")
      .load(path)
  }
}
