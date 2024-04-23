package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App with Job {
  val cliArgs = args
  val processCsv = cliArgs.contains("default")
  val processParquet = cliArgs.contains("--parquet")

  val MASTER_URL: String = cliArgs.lift(0).getOrElse("local[1]")
  val srcCsvPath: String = cliArgs.lift(1).getOrElse("./input/test1.csv")
  val dstCsvPath: String = cliArgs.lift(2).getOrElse("./default/output-writer-csv")
  val srcParquetPath: String = cliArgs.lift(3).getOrElse("./input/Iris.parquet")
  val dstParquetPath: String = cliArgs.lift(4).getOrElse("./default/output-writer-parquet")

  val conf = new SparkConf()
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl(sparkSession)
  val writer: Writer = new Writer()

  if (processCsv && srcCsvPath.endsWith(".csv")) {
    val inputDF: DataFrame = reader.read(srcCsvPath)
    val processedDF: DataFrame = processor.processCsv(inputDF)

    val csvOutputPath = if (dstCsvPath.endsWith(".csv")) {
      dstCsvPath
    } else if (dstCsvPath.endsWith("/")) {
      s"${dstCsvPath}output-writer-csv"
    } else {
      s"${dstCsvPath}/output-writer-csv"
    }

    println(s"CSV Output Path: $csvOutputPath")
    writer.writeCsv(processedDF, SaveMode.Overwrite, csvOutputPath)
  }

  if (processParquet && srcParquetPath.endsWith(".parquet")) {
    val inputParquetDF: DataFrame = reader.readParquet(srcParquetPath)
    inputParquetDF.printSchema()  // Print the schema for debugging
    val processedParquetDF: DataFrame = processor.processParquet(inputParquetDF)
    writer.writeParquet(processedParquetDF, SaveMode.Overwrite, dstParquetPath)
  }
}
