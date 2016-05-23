import org.apache.spark.sql.functions._
import org.scalatest._

class MotTests extends FlatSpec with Matchers {
  import MotUdfs._

  val parquetData = "D:/Data/mot/parquet/UAT_test_results.parquet"
  val resultsPath = "C:/Development/mot-data-in-spark/vis/results/"

  it should "find all the car colours" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select colour as colour, count(*) as cnt from mot_tests group by colour")

    val results = colours.map(x => new ResultsByColour(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeMapToFile(results, resultsPath + "motTestsByVehicleColour.json")
    println(results)
  }

  it should "calculate pass rate by age band" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .withColumn("age", testDateAndVehicleFirstRegDateToAge(col("date"), col("firstUseDate")))
      .groupBy("age")
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .selectExpr("age", "cnt", "passCount * 100 / cnt as rate")
      .cache()

    results
      .sort(asc("age"))
      .show(101)

    val resultMap = results.map({
      x => new ResultsByAge(
        x.getInt(0),
        x.getLong(1),
        x.getDouble(2))
      })
      .collect()
    JsonWriter.writeMapToFile(resultMap, resultsPath + "passRateByAgeBand.json")
  }

  it should "calculate pass rate by make and model" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .withColumn("shortModel", modelToShortModel(col("model")))
      .groupBy("make", "shortModel")
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .filter("cnt > 1000")
      .selectExpr("make", "shortModel", "cnt", "passCount * 100 / cnt as rate")
      .cache()

    println("Best:")
    results
      .sort(desc("rate"))
      .show(10)

    println("Worst:")
    results
      .sort(asc("rate"))
      .show(10)

    val resultMap = results.map(x => (x(0) + " " + x(1), x(3))).collect().toMap
    JsonWriter.writeMapToFile(resultMap, resultsPath + "passRateByMakeAndModel.json")
  }

  it should "calculate pass rate by make" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .groupBy("make")
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .filter("cnt > 1000")
      .selectExpr("make", "cnt", "passCount * 100 / cnt as rate")
      .cache()

    println("Best:")
    results
      .sort(desc("rate"))
      .show(10)

    println("Worst:")
    results
      .sort(asc("rate"))
      .show(10)

    val resultMap = results.map(x => (x(0), x(2))).collect().toMap
    JsonWriter.writeMapToFile(resultMap, resultsPath + "passRateByMake.json")
  }
}
