import org.apache.spark.sql.functions._
import org.scalatest._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._

class AnalyticsTests extends FlatSpec with Matchers {
  import MotUdfs._

  val parquetData = "D:/Data/mot/parquet/UAT_test_results_2011.parquet"
  val resultsPath = "C:/Development/mot-data-in-spark/vis/results/"

  it should "build a decision tree with category fields" in {
    // Read the data
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    // Define some values
    val labelField = "testPassed"
    val categoryFields = Seq("makeIndex", /*"modelIndex",*/ "fuelTypeIndex", "colourIndex")
    val featureFields = Seq("testMileage", "cylinderCapacity", "age") ++ categoryFields

    // Get the distinct values for all the category fields
    val distinctCategoryValues = Seq("make", /*"model", */"fuelType", "colour")
      .map(fieldName => (fieldName + "Index", motTests.select(col(fieldName)).distinct().map(_.getString(0)).collect().toList)).toMap

    // A UDF to convert a text field into an integer index
    // Should probably do this before the Parquet file is written
    val indexInValues = udf((key : String, item : String) => distinctCategoryValues(key).indexOf(item))

    val data =
      motTests
        .filter("testClass like '4%'") // Cars, not buses, bikes etc
        .filter("firstUseDate <> 'NULL' and date <> 'NULL'") // Must be able to calculate age
        .filter("testMileage > 0") // ignore tests where no mileage reported
        .filter("testType = 'N'") // only interested in the first test

        .withColumn("testPassed", passCodeToInt(col("testResult")))
        .withColumn("age", testDateAndVehicleFirstRegDateToAge(col("date"), col("firstUseDate")))

        .withColumn("makeIndex", indexInValues(lit("makeIndex"), col("make")))
        //.withColumn("modelIndex", indexInValues(lit("modelIndex"), col("model")))
        .withColumn("fuelTypeIndex", indexInValues(lit("fuelTypeIndex"), col("fuelType")))
        .withColumn("colourIndex", indexInValues(lit("colourIndex"), col("colour")))

        .selectExpr((featureFields :+ labelField).map(x => s"cast($x as double) $x"):_*)

    data.printSchema()
    data.show()

    // Convert to LabeledPoints objects
    // Thanks! http://stackoverflow.com/questions/31638770/rdd-to-labeledpoint-conversion
    val labelIndex = data.columns.indexOf(labelField)
    val featureIndexes = featureFields.map(data.columns.indexOf(_))

    val labeledPoints = data.map(row => LabeledPoint(
      row.getDouble(labelIndex),
      Vectors.dense(featureIndexes.map(row.getDouble).toArray)
    ))

    // Build the map of field_id -> value_count for the categorical fields
    labeledPoints.take(10).foreach(println)
    val categoryMap = categoryFields.map(field => {
      ( data.columns.indexOf(field), distinctCategoryValues(field).length )
    }).toMap

    println(categoryMap)

    // Split the data into three sets
    val Array(trainingData, testData, validationData) = labeledPoints.randomSplit(Array(0.8, 0.1, 0.1))
    trainingData.cache()
    testData.cache()
    validationData.cache()

    //println(s"Training: ${trainingData.count()}, Test: ${testData.count()}, Validation: ${validationData.count()}")

    val model = DecisionTree.trainClassifier(trainingData, 2, categoryMap, "gini", 16, 1000)

    val predictionsAndLabels = validationData.map(row => (model.predict(row.features), row.label))
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    println(s"Precision: ${metrics.precision}")

    println("Confusion Matrix")
    println(metrics.confusionMatrix)

    println(s"Class: 0, Precision: ${metrics.precision(0.0)}, Recall: ${metrics.recall(0.0)}")
    println(s"Class: 1, Precision: ${metrics.precision(1.0)}, Recall: ${metrics.recall(1.0)}")
  }



  it should "build a decision tree with one-hot fields for categories" in {
    val motTests = Spark.sqlContext.read.parquet(parquetData).toDF()
    motTests.registerTempTable("mot_tests")

    val labelField = "testPassed"
    val featureFields = Seq("testMileage", "cylinderCapacity", "age", "isPetrol", "isDiesel",
      "isLeylandDaf", "isBedford", "isAustin", "isTalbot", "isLdv", "isRover", "isFord", "isRenault", "isMorris", "isProton",
      "isMini", "isLexus", "isMcc", "isBentley", "isJaguar", "isPorche", "isToyota", "isSubaru", "isRollsRoyce", "isMercedes")

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

        // Worst 10
        .withColumn("isLeylandDaf", valueToOneOrZero(lit("LEYLAND DAF"), col("make")))
        .withColumn("isBedford", valueToOneOrZero(lit("BEDFORD"), col("make")))
        .withColumn("isAustin", valueToOneOrZero(lit("AUSTIN"), col("make")))
        .withColumn("isTalbot", valueToOneOrZero(lit("TALBOT"), col("make")))
        .withColumn("isLdv", valueToOneOrZero(lit("LDV"), col("make")))
        .withColumn("isRover", valueToOneOrZero(lit("ROVER"), col("make")))
        .withColumn("isFord", valueToOneOrZero(lit("FORD"), col("make")))
        .withColumn("isRenault", valueToOneOrZero(lit("RENAULT"), col("make")))
        .withColumn("isMorris", valueToOneOrZero(lit("MORRIS"), col("make")))
        .withColumn("isProton", valueToOneOrZero(lit("PROTON"), col("make")))

        // Best 10
        .withColumn("isMini", valueToOneOrZero(lit("MINI"), col("make")))
        .withColumn("isLexus", valueToOneOrZero(lit("LEXUS"), col("make")))
        .withColumn("isMcc", valueToOneOrZero(lit("MCC"), col("make")))
        .withColumn("isBentley", valueToOneOrZero(lit("BENTLEY"), col("make")))
        .withColumn("isJaguar", valueToOneOrZero(lit("JAGUAR"), col("make")))
        .withColumn("isPorche", valueToOneOrZero(lit("PORSCHE"), col("make")))
        .withColumn("isToyota", valueToOneOrZero(lit("TOYOTA"), col("make")))
        .withColumn("isSubaru", valueToOneOrZero(lit("SUBARU"), col("make")))
        .withColumn("isRollsRoyce", valueToOneOrZero(lit("ROLLS ROYCE"), col("make")))
        .withColumn("isMercedes", valueToOneOrZero(lit("MERCEDES"), col("make")))

        .selectExpr((featureFields :+ labelField).map(x => s"cast($x as double) $x"):_*)

    data.printSchema()
    data.show()

    // Thanks! http://stackoverflow.com/questions/31638770/rdd-to-labeledpoint-conversion
    val labelIndex = data.columns.indexOf(labelField)
    val featureIndexes = featureFields.map(data.columns.indexOf(_))

    val labeledPoints = data.map(row => LabeledPoint(
      row.getDouble(labelIndex),
      Vectors.dense(featureIndexes.map(row.getDouble).toArray)
    ))

    labeledPoints.take(10).foreach(println)

    val Array(trainingData, testData, validationData) = labeledPoints.randomSplit(Array(0.8, 0.1, 0.1))
    trainingData.cache()
    testData.cache()
    validationData.cache()

    //println(s"Training: ${trainingData.count()}, Test: ${testData.count()}, Validation: ${validationData.count()}")

    val model = DecisionTree.trainClassifier(trainingData, 2, Map[Int, Int](), "gini", 16, 1000)

    val predictionsAndLabels = validationData.map(row => (model.predict(row.features), row.label))
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    println(s"Precision: ${metrics.precision}")

    println("Confusion Matrix")
    println(metrics.confusionMatrix)

    println(s"Class: 0, Precision: ${metrics.precision(0.0)}, Recall: ${metrics.recall(0.0)}")
    println(s"Class: 1, Precision: ${metrics.precision(1.0)}, Recall: ${metrics.recall(1.0)}")
  }

}
