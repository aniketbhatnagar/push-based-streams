package streams

/**
 * A source is responsible for producing and pushing data to a PushStream.
 */
trait Source[T] {

  /**
   * @return an instance of PushStream to which this source will push data to.
   */
  def stream: PushStream[T]

  /**
   * Starts pushing data to the stream. All aggregations, transformations, etc must be already defined on stream before
   * this function is called.
   */
  def start(): Unit
}
