import org.apache.spark.sql.functions._
import org.scalatest._

class MotTests extends FlatSpec with Matchers {
  import MotUdfs._

  val parquetData = "D:/Data/mot/parquet/UAT_test_results.parquet"
  val resultsPath = "C:/Development/mot-data-in-spark/vis/results/"


  it should "count tests by make and model" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val mm = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .withColumn("shortModel", modelToShortModel(col("model")))
      .groupBy("make", "shortModel")
      .agg(count("*") as "cnt")
      .selectExpr("make", "shortModel", "cnt")
      .cache()

    val results = mm.map(x => new CountsByMakeAndModel(x.getString(0).toLowerCase, x.getString(1).toLowerCase, x.getLong(2))).collect()

    val tree = results
      .groupBy(_.make)
      .map({case (key : String, values : Array[CountsByMakeAndModel]) =>
          new MakeModelTreeItem(key,
            values.map(_.count).sum,
            values.map(x => new CountsByModelForTree(x.model, x.count)))
      })

    JsonWriter.writeToFile(tree, resultsPath + "motTestsByMakeAndModel.json")
    println(results)
  }


  it should "count tests by colour" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select colour as colour, count(*) as cnt from mot_tests where testClass like '4%' and testType = 'N' group by colour")

    val results = colours.map(x => new CountsByColour(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeToFile(results, resultsPath + "motTestsByVehicleColour.json")
    println(results)
  }


  it should "count tests by make" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select make, count(*) as cnt from mot_tests where testClass like '4%' and testType = 'N' group by make")

    val results = colours.map(x => new CountsByMake(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeToFile(results, resultsPath + "motTestsByMake.json")
    println(results)
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

    val resultMap =
      results
        .sort("rate")
        .map(x => (x.getString(0), x.getString(1), x.getDouble(3)))
        .collect
        .reverse
        .toSeq
        .zipWithIndex
        .map({case ((ma, mo, r), i) => new RateByMakeAndModel(ma, mo, r, i)} )
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByMakeAndModel.json")
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

    val resultMap =
      results
        .sort("rate")
        .map(x => (x.getString(0), x.getDouble(2)))
        .collect
        .reverse
        .toSeq
        .zipWithIndex
        .map({case ((m, r), i) => new RateByMake(m, r, i)} )
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByMake.json")
  }


  it should "calculate overall pass rate" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .groupBy()
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .selectExpr("passCount * 100 / cnt as rate")
      .cache()

    results.map(x => x.get(0)).collect().foreach(println)
  }


  it should "find all the car colours" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select colour as colour, count(*) as cnt from mot_tests where testClass like '4%' and testType = 'N' group by colour")

    val results = colours.map(x => new CountsByColour(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeToFile(results, resultsPath + "motTestsByVehicleColour.json")
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
      x => new RateByAge(
        x.getInt(0),
        x.getLong(1),
        x.getDouble(2))
      })
      .collect()
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByAgeBand.json")
  }
}
