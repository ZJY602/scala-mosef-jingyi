package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // Import Spark SQL functions

//class ProcessorImpl() extends Processor {
class ProcessorImpl(spark: SparkSession) extends Processor {

  def processCsv(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("Country")
      .agg(
        sum("Number").alias("TotalNumber"),
        count("*").alias("Count"),
        max("Number").alias("MaxNumber"),
        min("Number").alias("MinNumber")
      )
  }
  //   val groupKeyName = "group_key"
  // if(inputDF.columns.contains(groupKeyName)) {
  // La colonne existe, on peut continuer
  //  inputDF.groupBy(groupKeyName).count()
  // } else {
  // La colonne n'existe pas, on lance une exception ou on gère l'erreur différemment
  // throw new IllegalArgumentException(s"La colonne '$groupKeyName' n'existe pas dans le DataFrame.")

  def processFile(inputDF: DataFrame, fileType: String): DataFrame = {
    // Implement the file processing logic based on fileType
    fileType match {
      case "csv" => processCsv(inputDF)
      // Add cases for other file types if needed
      case _ => throw new IllegalArgumentException(s"Unsupported file type: $fileType")
    }
  }


  def processParquet(inputDF: DataFrame): DataFrame = {
    inputDF.filter(col("variety") === "Setosa")
      .groupBy("variety")
      .agg(
        avg(col("`sepal.length`")).as("avg_sepal_length"),
        avg(col("`sepal.width`")).as("avg_sepal_width")
      )
  }
}
