import org.apache.spark.mllib.linalg.Matrix

object CsvWriter {
  def writeMatrixToFile(matrix: Matrix, filename : String): Unit = {
    import java.io._

    val localMatrix: List[Array[Double]] = matrix
      .transpose // Transpose since .toArray is column major
      .toArray
      .grouped(matrix.numCols)
      .toList

    val lines: List[String] = localMatrix
      .map(line => line.mkString(","))
      .map(_ + "\n")

    val writer = new PrintWriter(new File(filename))
    lines.foreach(writer.write)
    writer.close()
  }
}
