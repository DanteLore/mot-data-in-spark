import MotUdfs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class TransformToParquetTests extends FlatSpec with Matchers {

  val allCsvData = "/Users/DTAYLOR/Data/mot/data/test_result_*.txt"
  val just2011CsvData ="/Users/DTAYLOR/Data/mot/data/test_result_2011.txt"
  val allParquetData = "/Users/DTAYLOR/Data/mot/parquet/test_results.parquet"
  val parquetData2011 = "/Users/DTAYLOR/Data/mot/parquet/test_results_2011.parquet"

  def filterAndEnrich(input: DataFrame) : DataFrame = {
    input.filter("testClass like '4%'") // Cars, not buses, bikes etc
         .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
         .withColumn("passCount", passCodeToInt(col("testResult")))
         .withColumn("age", testDateAndVehicleFirstRegDateToAge(col("date"), col("firstUseDate")))
         .withColumn("isPetrol", valueToOneOrZero(lit("P"), col("fuelType")))
         .withColumn("isDiesel", valueToOneOrZero(lit("D"), col("fuelType")))
         .withColumn("testYear", dateToYear(col("date")))
         .withColumn("testMonth", dateToMonth(col("date")))
         .withColumn("testDay", dateToDay(col("date")))
  }

  it should "load csv data for 2011 then save it to parquet with extra fields" in {

    import Spark.sqlContext.implicits._

    val motTests = Spark.sc
      .textFile(just2011CsvData, 1)
      .map(_.split('|'))
      .map(x => MotTestResult(x(0).toInt, x(1).toInt, x(2), x(3), x(4), x(5), x(6).toInt, x(7), x(8), x(9), x(10), x(11), x(12).toInt, x(13)))
      .toDF()

    val filteredAndEnriched = filterAndEnrich(motTests)

    filteredAndEnriched.printSchema()

    scalax.file.Path.fromString(parquetData2011).deleteRecursively()

    filteredAndEnriched.write.format("parquet").save(parquetData2011)
  }

  it should "load csv data then save it to parquet with extra fields" in {

    import Spark.sqlContext.implicits._

    val motTests = Spark.sc
      .textFile(allCsvData, 1)
      .map(_.split('|'))
      .map(x => MotTestResult(x(0).toInt, x(1).toInt, x(2), x(3), x(4), x(5), x(6).toInt, x(7), x(8), x(9), x(10), x(11), x(12).toInt, x(13)))
      .toDF()

    val filteredAndEnriched = filterAndEnrich(motTests)

    filteredAndEnriched.printSchema()

    scalax.file.Path.fromString(allParquetData).deleteRecursively()

    filteredAndEnriched.write.format("parquet").save(allParquetData)
  }

}
