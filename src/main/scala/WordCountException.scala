/***
 * custom exception class
 * @param message - Exception type
 */
class WordCountException(message: WordCountExceptionEnum.Value) extends Exception(message.toString){}

  object WordCountExceptionEnum extends Enumeration {
    type WordCountException = Value
    val WrongFilePath: WordCountExceptionEnum.Value =  Value("Wrong Path Specified!!")
    val SqlSparkException: WordCountExceptionEnum.Value = Value("Spark Analysis Exception")
}
