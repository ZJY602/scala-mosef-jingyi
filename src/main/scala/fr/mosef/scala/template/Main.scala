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
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }
  val srcCsvPath: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./input/test1.csv"  // Default CSV file path
    }
  }
  val dstCsvPath: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-csv"  // Default CSV output path
    }
  }
  val srcParquetPath: String = try {
    cliArgs(3)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./input/Iris.parquet"  // Default Parquet file path
    }
  }
  val dstParquetPath: String = try {
    cliArgs(4)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-parquet"  // Default Parquet output path
    }
  }

  val conf = new SparkConf()
  conf.set("spark.testing.memory", "471859200") //best change !!

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
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl(sparkSession)
  val writer: Writer = new Writer()
  val src_path = srcCsvPath
  val dst_path = dstCsvPath
  val PARQUET_SRC_PATH = srcParquetPath
  val PARQUET_DST_PATH = dstParquetPath

  // CSV Data Processing
  val inputDF: DataFrame = reader.read(src_path)
  val processedDF: DataFrame = processor.processCsv(inputDF)
  //writer.writeCsv(processedDF, "overwrite", dst_path)
  writer.writeCsv(processedDF, SaveMode.Overwrite, dst_path)

  // Parquet Data Processing
  val inputParquetDF: DataFrame = reader.readParquet(PARQUET_SRC_PATH)
  inputParquetDF.printSchema()  // Ajoutez cette ligne pour imprimer le schéma et vérifier les noms de colonnes

  val processedParquetDF: DataFrame = processor.processParquet(inputParquetDF)
  //writer.writeParquet(processedParquetDF, "overwrite", PARQUET_DST_PATH)
  writer.writeParquet(processedParquetDF, SaveMode.Overwrite, PARQUET_DST_PATH)

  // Traitement conditionnel basé sur l'extension du fichier
  if (srcCsvPath.endsWith(".csv")) {
    processCsv(sparkSession) // Méthode du trait Job
  } else if (srcParquetPath.endsWith(".parquet")) {
    processParquet(sparkSession) // Méthode du trait Job
  }
}
