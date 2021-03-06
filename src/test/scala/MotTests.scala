import org.apache.spark.sql.functions._
import org.scalatest._

class MotTests extends FlatSpec with Matchers {
  import MotUdfs._

  //val parquetData = "/Users/DTAYLOR/Data/mot/parquet/test_results_2011.parquet"
  val parquetData = "/Users/DTAYLOR/Data/mot/parquet/test_results.parquet"
  val resultsPath = "/Users/DTAYLOR/Development/mot-data-in-spark/vis/results/"


  it should "calculate covariance and correlation between mileage and age" in {
    //https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val df = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .withColumn("pass", passCodeToInt(col("testResult")))

    println(s"cov(testMileage, age) = ${df.stat.cov("testMileage", "age")}")
    println(s"corr(testMileage, age) = ${df.stat.corr("testMileage", "age")}")
  }


  it should "calculate covariance and correlation for normal cars" in {
    //https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val df = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("age <= 20")
      .filter("testMileage <= 250000")
      .withColumn("pass", passCodeToInt(col("testResult")))

    println("For cars < 20 years and < 250,000 miles")
    println(s"cov(testMileage, pass) = ${df.stat.cov("testMileage", "pass")}")
    println(s"corr(testMileage, pass) = ${df.stat.corr("testMileage", "pass")}")

    println(s"cov(age, pass) = ${df.stat.cov("age", "pass")}")
    println(s"corr(age, pass) = ${df.stat.corr("age", "pass")}")

    println(s"cov(age, age) = ${df.stat.cov("age", "age")}")
    println(s"corr(age, age) = ${df.stat.corr("age", "age")}")
  }


  it should "calculate covariance and correlation over all the data" in {
    //https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val df = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
      .withColumn("pass", passCodeToInt(col("testResult")))

    println("For all data")
    println(s"cov(testMileage, pass) = ${df.stat.cov("testMileage", "pass")}")
    println(s"corr(testMileage, pass) = ${df.stat.corr("testMileage", "pass")}")

    println(s"cov(age, pass) = ${df.stat.cov("age", "pass")}")
    println(s"corr(age, pass) = ${df.stat.corr("age", "pass")}")

    println(s"cov(age, age) = ${df.stat.cov("age", "age")}")
    println(s"corr(age, age) = ${df.stat.corr("age", "age")}")
  }


  it should "describe the dataset" in {
    // This is very slow!
    Spark
      .sqlContext
      .read
      .parquet(parquetData)
      .toDF()
      .describe()
      .show()
  }


  it should "calculate pass rate by mileage band and age" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .withColumn("mileageBand", mileageToBand(col("testMileage")))
      .groupBy("age", "mileageBand")
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .selectExpr("age", "mileageBand", "cnt", "passCount * 100 / cnt as rate")
      .cache()

    results
      .sort(asc("age"), asc("mileageBand"))
      .show(1000)

    val resultMap = results.map({
      x => RateByAgeAndMileage(
        x.getInt(0),
        x.getDouble(1).toLong,
        x.getLong(2),
        x.getDouble(3))
    })
      .collect()
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByAgeAndMileageBand.json")
  }


  it should "calculate pass rate by mileage band" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results = motTests
      .filter("testClass like '4%'") // Cars, not buses, bikes etc
      .filter("testType = 'N'") // only interested in the first test
      .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
      .withColumn("passCount", passCodeToInt(col("testResult")))
      .withColumn("mileageBand", mileageToBand(col("testMileage")))
      .groupBy("mileageBand")
      .agg(count("*") as "cnt", sum("passCount") as "passCount")
      .selectExpr("mileageBand", "cnt", "passCount * 100 / cnt as rate")
      .cache()

    results
      .sort(asc("mileageBand"))
      .show(1000)

    val resultMap = results.map({
      x => RateByMileage(
        x.getDouble(0).toLong,
        x.getLong(1),
        x.getDouble(2))
    })
      .collect()
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByMileageBand.json")
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
      x => RateByAge(
        x.getInt(0),
        x.getLong(1),
        x.getDouble(2))
    })
      .collect()
    JsonWriter.writeToFile(resultMap, resultsPath + "passRateByAgeBand.json")
  }


  it should "calculate pass rate by age band and make" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val results =
      motTests
        .filter("testClass like '4%'") // Cars, not buses, bikes etc
        .filter("testType = 'N'") // only interested in the first test
        .filter("firstUseDate <> 'NULL' and date <> 'NULL'")
        .withColumn("passCount", passCodeToInt(col("testResult")))
        .withColumn("age", testDateAndVehicleFirstRegDateToAge(col("date"), col("firstUseDate")))
        .groupBy("age", "make")
        .agg(count("*") as "cnt", sum("passCount") as "passCount")
        .selectExpr("make", "age", "cnt", "passCount * 100 / cnt as rate")
        .filter("cnt >= 1000")
        .rdd

    val resultMap =
      results
        .map({
          x => (
            x.getString(0),
            x.getInt(1),
            x.getLong(2),
            x.getDouble(3)
            )
        })

    val mappedResults =
      resultMap
        .groupBy { case (make, age, cnt, rate) => make }
        .map { case (make, stuff) =>
          AgeAndMakeResults(make,
            stuff
              .map { case (_, age, cnt, rate) => RateByAge(age, cnt, rate) }
              .filter(x => x.age >= 3 && x.age <= 20)
              .toSeq
          )
        }
        .filter(_.series.length >= 18)
        .collect()

    JsonWriter.writeToFile(mappedResults, resultsPath + "passRateByAgeBandAndMake.json")
  }


  it should "prepare data for a decision tree to classify probability classes" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val keyFields = Seq("make", "mileageBand", "cylinderCapacity", "age", "isPetrol", "isDiesel")

    val data =
      motTests
        .filter("testClass like '4%'") // Cars, not buses, bikes etc
        .filter("firstUseDate <> 'NULL' and date <> 'NULL'") // Must be able to calculate age
        .filter("testMileage > 0") // ignore tests where no mileage reported
        .filter("testType = 'N'") // only interested in the first test
        .withColumn("testPassed", passCodeToInt(col("testResult")))
        .withColumn("age", testDateAndVehicleFirstRegDateToAge(col("date"), col("firstUseDate")))
        .withColumn("isPetrol", valueToOneOrZero(lit("P"), col("fuelType")))
        .withColumn("isDiesel", valueToOneOrZero(lit("D"), col("fuelType")))
        .withColumn("mileageBand", mileageToBand(col("testMileage")))
        .groupBy(keyFields.map(col): _*)
        .agg(count("*") as "cnt", sum("testPassed") as "passCount")
        .filter("cnt > 10")
        .withColumn("passRateCategory", passRateToCategory(col("cnt"), col("passCount")))
        .selectExpr(keyFields ++ Seq("cnt", "passCount * 100 / cnt as passRate", "passRateCategory"):_*)
        .cache()

    data.printSchema()
    data
      .sort(desc("cnt"))
      .show()

    data
      .sort(asc("passRate"))
      .show()
  }


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

    val results = mm.map(x => CountsByMakeAndModel(x.getString(0).toLowerCase, x.getString(1).toLowerCase, x.getLong(2))).collect()

    val tree = results
      .groupBy(_.make)
      .map({case (key : String, values : Array[CountsByMakeAndModel]) =>
          MakeModelTreeItem(key,
            values.map(_.count).sum,
            values.map(x => CountsByModelForTree(x.model, x.count)))
      })

    JsonWriter.writeToFile(tree, resultsPath + "motTestsByMakeAndModel.json")
    println(results)
  }


  it should "count tests by colour" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select colour as colour, count(*) as cnt from mot_tests where testClass like '4%' and testType = 'N' group by colour")

    val results = colours.map(x => CountsByColour(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeToFile(results, resultsPath + "motTestsByVehicleColour.json")
    println(results)
  }


  it should "count tests by make" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val colours = Spark.sqlContext
      .sql("select make, count(*) as cnt from mot_tests where testClass like '4%' and testType = 'N' group by make")

    val results = colours.map(x => CountsByMake(x.getString(0).toLowerCase, x.getLong(1))).collect()
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
        .map({case ((ma, mo, r), i) => RateByMakeAndModel(ma, mo, r, i)} )
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
        .map({case ((m, r), i) => RateByMake(m, r, i)} )
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

    val results = colours.map(x => CountsByColour(x.getString(0).toLowerCase, x.getLong(1))).collect()
    JsonWriter.writeToFile(results, resultsPath + "motTestsByVehicleColour.json")
    println(results)
  }
}
