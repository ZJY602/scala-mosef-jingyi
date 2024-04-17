package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {
  def processCsv(inputDF: DataFrame): DataFrame
  def processParquet(inputDF: DataFrame): DataFrame
  def processFile(inputDF: DataFrame, fileType: String): DataFrame
}