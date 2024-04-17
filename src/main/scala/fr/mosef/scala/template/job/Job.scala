package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}

trait Job {
  val reader: Reader
  val processor: Processor
  val writer: Writer

  // Les chemins doivent être fournis par la classe implémentant ce trait.
  def srcCsvPath: String
  def dstCsvPath: String
  def srcParquetPath: String
  def dstParquetPath: String

  def processCsv(sparkSession: SparkSession): Unit = {
    val inputDF: DataFrame = reader.read(srcCsvPath)
    val processedDF: DataFrame = processor.processCsv(inputDF)
    writer.writeCsv(processedDF, SaveMode.Overwrite, dstCsvPath)
  }

  def processParquet(sparkSession: SparkSession): Unit = {
    val inputParquetDF: DataFrame = reader.readParquet(srcParquetPath)
    val processedParquetDF: DataFrame = processor.processParquet(inputParquetDF)
    writer.writeParquet(processedParquetDF, SaveMode.Overwrite, dstParquetPath)
  }
}
