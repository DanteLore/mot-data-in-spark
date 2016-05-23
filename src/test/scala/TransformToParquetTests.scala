import org.scalatest.{Matchers, FlatSpec}

class TransformToParquetTests extends FlatSpec with Matchers {

  val allCsvData = "D:/Data/mot/data/UAT_test_result_*.txt"
  val just2011CsvData ="D:/Data/mot/data/UAT_test_result_2011.txt"
  val allParquetData = "D:/Data/mot/parquet/UAT_test_results.parquet"
  val parquetData2011 = "D:/Data/mot/parquet/UAT_test_results_2011.parquet"

  it should "load csv data then save it to parquet" in {

    import Spark.sqlContext.implicits._

    val motTests = Spark.sc
      .textFile(allCsvData, 1)
      .map(_.split('|'))
      .map(x => MotTestResult(x(0).toInt, x(1).toInt, x(2), x(3), x(4), x(5), x(6).toInt, x(7), x(8), x(9), x(10), x(11), x(12).toInt, x(13)))
      .toDF()

    motTests.printSchema()

    scalax.file.Path.fromString(allParquetData).deleteRecursively()

    motTests.write.format("parquet").save(allParquetData)
  }

  it should "load csv data for 2011 then save it to parquet" in {

    import Spark.sqlContext.implicits._

    val motTests = Spark.sc
      .textFile(just2011CsvData, 1)
      .map(_.split('|'))
      .map(x => MotTestResult(x(0).toInt, x(1).toInt, x(2), x(3), x(4), x(5), x(6).toInt, x(7), x(8), x(9), x(10), x(11), x(12).toInt, x(13)))
      .toDF()

    motTests.printSchema()

    scalax.file.Path.fromString(parquetData2011).deleteRecursively()

    motTests.write.format("parquet").save(parquetData2011)
  }

}
