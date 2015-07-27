package streams

/**
 * Observers a stream.
 */
trait Observer[T] {

  /**
   * Called when the observer is added a stream.
   * @param stream Stream to which observer was added.
   */
  def onAdded(stream: PushStream[T]): Unit = {}

  /**
   * Called when an element is emitted by a stream.
   * @param stream Stream that emitted the element.
   * @param element Emitted element.
   */
  def onElement(stream: PushStream[T], element: T): Unit

  /**
   * Called when a stream no longer was any values to emit.
   * @param stream stream that has completed.
   */
  def onStreamComplete(stream: PushStream[T]): Unit
}
