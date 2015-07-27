package streams

import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.{ShouldMatchers, FlatSpec}

class PushStreamSpec extends FlatSpec with ShouldMatchers {
  "pushed values " should "be seen by observer" in {
    val valuesToPush = List("testElement1", "testElement2")
    val stream = new PushStream[String]
    val observer = new ValuesAssertObserver[String](stream, valuesToPush : _*)
    stream.pushAllAndEOF(valuesToPush : _*)
    observer.assertAllCallBacksFired()
  }

  behavior of "mapped values"
  it should "be emitted correctly" in {
    val values = List("testElement1", "testElement2")
    def mapper(value: String) = value + "appended"
    testMap(values, mapper)
  }

  behavior of "stateful mapped values"
  it should "be emitted correctly" in {
    val values = List("testElement1", "testElement2")
    val valuesToAssert = List("testElement1_1", "testElement2_2")
    def mapper(state: Int, value: String) = {
      val newValue = value + "_" + state
      val newState = state + 1
      (newState, newValue)
    }
    val stream = new PushStream[String]
    val mappedStream = stream.mapWithState(1)(mapper)
    val observer = new ValuesAssertObserver[String](mappedStream, valuesToAssert : _*)
    stream.pushAllAndEOF(values: _*)
    observer.assertAllCallBacksFired()
  }

  behavior of "flat mapped values"
  it should "be emitted correctly" in {
    val values = List("testElement1", "testElement2")
    def mapper(value: String) = Seq(value + "_1", value + "_2")
    testFlatMap(values, mapper)
  }

  behavior of "fold on stream"
  it should "sum correctly" in {
    val values = (1 to 5).toSeq
    def folder(oldSum: Int, element: Int) = oldSum + element
    testFold(values, 0, folder)

  }
  it should "count correctly" in {
    val values = (1 to 5).toSeq
    def folder(oldCount: Int, element: Int) = oldCount + 1
    testFold(values, 0, folder)
  }

  private def testFold[T, U](values: Seq[T], foldInitial: U, folder: (U, T) => U): Unit = {
    val stream = new PushStream[T]
    val expectedOutput = values.foldLeft(foldInitial)(folder)
    val resultFuture = stream.fold(foldInitial)(folder)
    stream.pushAllAndEOF(values :_ *)
    resultFuture.futureValue should be (expectedOutput)
  }

  private def testMap[T, U](values: Seq[T], mapper: T => U): Unit = {
    val valuesToAssert = values.map(mapper)
    val stream = new PushStream[T]
    val mappedStream = stream.map(mapper)
    val observer = new ValuesAssertObserver[U](mappedStream, valuesToAssert : _*)
    stream.pushAllAndEOF(values : _*)
    observer.assertAllCallBacksFired()
  }

  private def testFlatMap[T, U](values: Seq[T], mapper: T => Seq[U]): Unit = {
    val valuesToAssert = values.flatMap(mapper)
    val stream = new PushStream[T]
    val mappedStream = stream.flatMap(mapper)
    val observer = new ValuesAssertObserver[U](mappedStream, valuesToAssert : _*)
    stream.pushAllAndEOF(values : _*)
    observer.assertAllCallBacksFired()
  }
}
