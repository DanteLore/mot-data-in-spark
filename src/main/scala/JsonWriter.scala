import java.io.PrintWriter

import org.json4s.jackson.Serialization.writePretty
import org.json4s.{DefaultFormats, Formats}

object JsonWriter {
  private implicit lazy val jsonFormats: Formats = DefaultFormats

  def writeToFile(data: AnyRef, filename: String): Unit = {
    val json = writePretty(data)
    new PrintWriter(filename) { write(json); close() }
  }
}
