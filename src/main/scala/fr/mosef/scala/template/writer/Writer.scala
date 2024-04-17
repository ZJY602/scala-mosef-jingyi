package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.collection.JavaConverters._

class Writer {

  def writeCsv(df: DataFrame, mode: SaveMode = SaveMode.Overwrite, path: String, options: Map[String, String] = Map("header" -> "true")): Unit = {
    val writer = df.write.options(options).mode(mode)
    writer.csv(path)
  }

  def writeParquet(df: DataFrame, mode: SaveMode = SaveMode.Overwrite, path: String, options: Map[String, String] = Map()): Unit = {
    df.write.options(options).mode(mode).parquet(path)
  }
}
