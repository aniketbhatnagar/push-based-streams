package streams

import org.scalatest.Matchers

class ValuesAssertObserver[T](stream: PushStream[T], valuesToAssert: T*) extends Observer[T] with Matchers {
  private var pendingValuesToAssert = valuesToAssert
  private var streamAdded = true
  private var streamCompleted = false

  stream.observe(this)

  override def onAdded(stream: PushStream[T]): Unit = {
    this.stream should be (stream)
    streamAdded = true
  }


  override def onElement(stream: PushStream[T], element: T): Unit = {
    this.stream should be (stream)
    streamAdded should be (true) // onAdded must be called before onElement
    element should be (pendingValuesToAssert.head)
    pendingValuesToAssert = pendingValuesToAssert.tail
  }

  override def onStreamComplete(stream: PushStream[T]): Unit = {
    this.stream should be (stream)
    streamAdded should be (true) // onAdded must be called before onStreamComplete
    pendingValuesToAssert.size should be (0)
    streamCompleted = true
  }

  def assertAllCallBacksFired(): Unit = {
    streamAdded should be (true)
    streamCompleted should be (true)
  }
}
