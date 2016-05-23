case class ResultsByAge(age : Long, count : Long, rate : Double)

case class ResultsByColour(colour : String, count : Long)

case class ResultsByMake(make : String, rate : Double, rank : Long)

case class ResultsByMakeAndModel(make : String, model : String, rate : Double, rank : Long)