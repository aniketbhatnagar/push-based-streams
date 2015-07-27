package streams

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

/**
 * A source that reads a file line by line.
 */
class TextFileSource(filePath: String) extends Source[String] {

  /**
   * @return an instance of PushStream to which this source will push data to.
   */
  override val stream: PushStream[String] = new PushStream[String]

  /**
   * Starts pushing data to the stream. All aggregations, transformations, etc must be already defined on stream before
   * this function is called.
   */
  override def start(): Unit = {
    val fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)))
    var line = fileReader.readLine()
    while (line != null) {
      stream.push(line)
      line = fileReader.readLine()
    }
    stream.pushEOF()
  }
}
