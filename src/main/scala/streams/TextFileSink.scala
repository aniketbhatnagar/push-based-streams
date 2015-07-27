/**
 * SapientNitro (c) 2015.
 */
package streams

import java.io.{OutputStreamWriter, FileOutputStream, BufferedWriter}

/**
 * Writes all elements to a text file.
 */
class TextFileSink(outputFilePath: String) extends Observer[String] {
  private val fileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFilePath)))

  /**
   * Called when an element is emitted by a stream.
   * @param stream Stream that emitted the element.
   * @param element Emitted element.
   */
  override def onElement(stream: PushStream[String], element: String): Unit = {
    fileWriter.write(element)
    fileWriter.newLine()
  }

  /**
   * Called when a stream no longer was any values to emit.
   * @param stream stream that has completed.
   */
  override def onStreamComplete(stream: PushStream[String]): Unit = {
    fileWriter.close()
  }
}
