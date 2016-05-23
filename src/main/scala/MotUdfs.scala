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

  val valueToOneOrZero = udf((key : String, fuel : String) => if(key == fuel) 1 else 0)
}
