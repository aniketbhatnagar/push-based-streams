package streams

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files

import org.scalatest._

class TextFileSourceSpec extends FlatSpec with ShouldMatchers {
  import scala.collection.JavaConversions._
  "TextFileSource " should "read all lines in a file" in {
    val filePath = this.getClass.getClassLoader.getResource("data.txt").getFile
    val lines = Files.readAllLines(new File(filePath).toPath, Charset.defaultCharset())
    val source = new TextFileSource(filePath)
    val observer = new ValuesAssertObserver[String](source.stream, lines : _*)
    source.start()
    observer.assertAllCallBacksFired()
  }
}
