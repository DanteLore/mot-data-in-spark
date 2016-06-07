case class RateByAge(age : Long, count : Long, rate : Double)

case class RateByAgeAndMake(make : String, age : Long, count : Long, rate : Double)

case class RateByMake(make : String, rate : Double, rank : Long)

case class RateByMakeAndModel(make : String, model : String, rate : Double, rank : Long)

case class CountsByColour(colour : String, count : Long)

case class CountsByMake(make : String, count : Long)

case class CountsByMakeAndModel(make : String, model : String, count : Long)

case class MakeModelTreeItem(name : String, count : Long, children : Seq[CountsByModelForTree])

case class CountsByModelForTree(name : String, count : Long)

case class AgeAndMakeResults(make: String, series: Seq[RateByAge])