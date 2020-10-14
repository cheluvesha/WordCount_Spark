import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.explode
import org.scalatest.FunSuite

/***
 * Test Class uses FunSuite ScalaTest
 * checks test cases by asserting
 */
class WordCountTest extends FunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("WordCountTest")
    .master("local[*]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val file = "hdfs://localhost:54310/WordCountSpark/WordCount.txt"
  val wrongFile = "./src/test/Resources/wrongFile.txt"
  val wrongFilePath = "/test/Resources/wrongFile.txt"
  val wordCount = new WordCount(spark,sc)

  test("givenInputTextFileShouldReturnEqualNumOfWords") {
    val readFile = sc.textFile(file)
    val words : Map[String,Int] = readFile.
      flatMap(f => f.split(" "))
      .map(m => (m,1))
      .reduceByKey(_+_)
      .collect()
      .toMap
    val wordMap = wordCount.countWordsRdd(file)
    assert(words.equals(wordMap) === true)
  }

  test("givenWrongInputFileShouldReturnNotEqualNumOfWords") {
    val readFile = sc.textFile(file)
    val words : Map[String,Int] = readFile.
      flatMap(f => f.split(" "))
      .map(m => (m,1))
      .reduceByKey(_+_)
      .collect()
      .toMap
    val wordMap = wordCount.countWordsRdd(wrongFile)
    assert(words.equals(wordMap) === false)
  }

  test("givenWrongInputFilePathShouldThrowException") {
    val thrown: Exception = intercept[Exception] {
      wordCount.countWordsRdd(wrongFilePath)
    }
    assert(thrown.getMessage === WordCountExceptionEnum.WrongFilePath.toString)
  }

  test("givenInputFilePathShouldReturnEqualRecordsFromDataFrame") {
    val wordDF = spark.read.text(file)
    val rowWordDF = wordDF.select(explode(functions.split(wordDF("value"),(" "))).alias("rowDF"))
    val countDF: DataFrame = rowWordDF.groupBy("rowDF").count()
    val resultDF = wordCount.countWordsDataFrame(file)
    assert(countDF.except(resultDF).count() === 0 )
  }

  test("givenWrongInputFileShouldReturnUnEqualRecordsFromDataFrame") {
    val wordDF = spark.read.text(file)
    val rowWordDF = wordDF.select(explode(functions.split(wordDF("value"),(" "))).alias("rowDF"))
    val countDF: DataFrame = rowWordDF.groupBy("rowDF").count()
    val resultDF = wordCount.countWordsDataFrame(wrongFile)
    assert(countDF.except(resultDF).count() != 0 )
  }

  test("givenWrongFileShouldThrowAnException") {
    val thrown = intercept[Exception]{
      wordCount.countWordsDataFrame(wrongFilePath)
    }
    assert(thrown.getMessage === WordCountExceptionEnum.SqlSparkException.toString)
  }

  test("givenInputFileShouldReturnEqualWordsFromDataSet") {
    import spark.implicits._
    val textDataSet = spark.read.text(file).as[String]
    val wordsDataset = textDataSet.flatMap(_.split(" ")).withColumnRenamed("value","words")
    val countDF = wordsDataset.groupBy("words").count()
    val resultDF = wordCount.countWordsDataSet(file)
    assert(countDF.except(resultDF).count() === 0)
  }

  test("givenWrongFilePathShouldReturnException") {
    val thrown = intercept[Exception] {
      wordCount.countWordsDataSet(wrongFilePath)
    }
    assert(thrown.getMessage === WordCountExceptionEnum.SqlSparkException.toString)
  }
}
