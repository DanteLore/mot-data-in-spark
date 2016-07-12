import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.functions._

object MotUdfs {

  val passCodeToInt = udf((testResult : String) => testResult match {
    case "P" => 1
    case "PRS" => 1
    case _ => 0
  })

  val modelToShortModel = udf((model : String) => {
    model.toUpperCase().split(' ').take(1).mkString(" ")
  })

  val testDateAndVehicleFirstRegDateToAge = udf((testDate : String, firstUse : String) => {
    val testYear = DateTime.parse(testDate).getYear
    val firstUseYear = DateTime.parse(firstUse).getYear
    Math.min(testYear - firstUseYear, 100)
  })

  val dateToYear = udf((date : String) => {
    DateTime.parse(date).getYear
  })

  val dateToMonth = udf((date : String) => {
    DateTime.parse(date).getDayOfMonth
  })

  val dateToDay = udf((date : String) => {
    DateTime.parse(date).getDayOfMonth
  })

  val valueToOneOrZero = udf((key : String, fuel : String) => if(key == fuel) 1.0 else 0.0)

  val passRateToCategory = udf((rowCount : Double, passCount : Double) => Math.floor((passCount / rowCount) * 10))

  val mileageToBand = udf((mileage : Double) => Math.round(mileage / 10000.0) * 10000.0)
}
