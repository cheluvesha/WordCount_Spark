import java.io.{FileNotFoundException, IOException}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions.explode

/***
 * class uses Spark core and Spark Sql Dependencies
 * @param sparkSession - SparkSession object
 * @param sparkContext - SparkContext object
 */
class WordCount (sparkSession: SparkSession, sparkContext:SparkContext) {
  val spark: SparkSession = sparkSession
  val sc: SparkContext = sparkContext
  /***
   * reads text file and uses flatmap to split the words
   * @param filePath - specifies file path
   * @return DataFrame
   */
  def countWordsDataSet(filePath: String): DataFrame = {
    try {
      import spark.implicits._
    val textDataSet = spark.read.text(filePath).as[String]
    val wordsDataset = textDataSet.flatMap(_.split(" ")).withColumnRenamed("value","words")
    val countDF = wordsDataset.groupBy("words").count()
    countDF
  }
    catch {
      case _:org.apache.spark.sql.AnalysisException =>
        throw new WordCountException(WordCountExceptionEnum.SqlSparkException)
    }
  }

  /***
   * reads text file and uses flatmap to split the words
   * @param filePath - specifies file path
   * @return DataFrame
   */
  def countWordsDataFrame(filePath: String): DataFrame = {
    try {
    val wordDF = spark.read.text(filePath)
    val rowWordDF = wordDF.select(explode(functions.split(wordDF("value"),(" "))).alias("rowDF"))
    val countDF: DataFrame = rowWordDF.groupBy("rowDF").count()
    countDF
  }
  catch {
    case _:org.apache.spark.sql.AnalysisException =>
      throw new WordCountException(WordCountExceptionEnum.SqlSparkException)
  }
  }

  /***
   * reads the text file and generates RDD and split into words
   * @param filePath - specifies file path
   * @return Map data structure
   */
  def countWordsRdd(filePath: String): Map[String, Int] = {
    try {
      val rddWords = sc.textFile(filePath)
      val words: Map[String, Int] = rddWords.
        flatMap(f => f.split(" "))
        .map(m => (m, 1))
        .reduceByKey(_ + _)
        .collect()
        .toMap
      words
    }
    catch  {
      case _:IOException =>
        throw new WordCountException(WordCountExceptionEnum.WrongFilePath)
    }
  }
}
