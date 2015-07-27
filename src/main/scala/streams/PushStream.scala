package streams

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/**
 * A push stream represents a stream of element that could be observed by a list of observers.
 * Observers are guaranteed to see the values in the order they were pushed.
 * An observer can be added anytime to the stream but values pushed prior to when the observer was added will not be
 * relayed by the stream to the observer.
 * @tparam T The type of elements in the stream
 */
class PushStream[T] {

  /** list of observers to which values need to be emitted */
  private val observers = new mutable.ListBuffer[Observer[T]]()

  /**
   * Registers an observer to the stream. All values pushed (using push function) prior to when this function was
   * called will not be visible to the passed observer.
   * @param observer observer that needs to be observed.
   */
  def observe(observer: Observer[T]): Unit = {
    observers.append(observer)
    observer.onAdded(this)
  }

  /**
   * Pushes an element in the stream. Observers will be notified immediately.
   * @param elem Element to be pushed.
   */
  def push(elem: T): Unit = {
    for (observer <- observers) {
      observer.onElement(this, elem)
    }
  }

  /**
   * Pushes all provided elements to the stream. Observers will be notified immediately in the order in which elements
   * are provided.
   * @param elements Elements to be pushed.
   */
  def pushAll(elements: T*): Unit = {
    for (element <- elements) {
      push(element)
    }
  }

  /**
   * Sends end of stream signal to all observers.
   */
  def pushEOF(): Unit = {
    for (observer <- observers) {
      observer.onStreamComplete(this)
    }
  }

  /**
   * Pushes all provided elements to the stream and completes the stream.
   * @param elements Elements to be pushed.
   */
  def pushAllAndEOF(elements: T*): Unit = {
    pushAll(elements : _*)
    pushEOF()
  }

  /**
   * Applies a function to each element of the stream.
   * @param iterator Function to apply to each element.
   */
  def foreach(iterator: T => Unit): Unit = {
    val observer = new Observer[T] {
      override def onElement(stream: PushStream[T], element: T): Unit = {
        iterator(element)
      }

      override def onStreamComplete(stream: PushStream[T]): Unit = {}
    }
    this.observe(observer)
  }

  /**
   * Maps each element to another in the stream.
   * @param mapper Function that maps each element to another.
   * @tparam U Type of mapped element.
   * @return New stream with mapped elements.
   */
  def map[U](mapper: T => U): PushStream[U] = {
    val observer = new MapperObserver[T, U, U](this, mapper, (stream, mappedValue) => {
      stream.push(mappedValue)
    })
    observer.mappedStream
  }

  /**
   * Maps each element to another in the stream keep track of state at each mapping.
   * @param initialState Initial state for first mapping.
   * @param mapper Maps (state, element) to (newState, mappedValue).
   * @tparam U Type of mapped value.
   * @tparam S Type of state.
   * @return New stream with mapped elements.
   */
  def mapWithState[U, S](initialState: S)(mapper: (S, T) => (S, U)): PushStream[U] = {
    val observer = new Observer[T] {
      val mappedStream = new PushStream[U]
      private var currentState = initialState
      override def onElement(stream: PushStream[T], element: T): Unit = {
        val (newState, mappedValue) = mapper(currentState, element)
        currentState = newState
        mappedStream.push(mappedValue)
      }

      override def onStreamComplete(stream: PushStream[T]): Unit = {
        mappedStream.pushEOF()
      }
    }
    this.observe(observer)
    observer.mappedStream
  }

  /**
   * Maps each element to another sequence and flattens the stream.
   * @param mapper Function that maps each element to another sequence.
   * @tparam U Type of element in the sequence.
   * @return New stream with mapped elements.
   */
  def flatMap[U](mapper: T => Seq[U]): PushStream[U] = {
    val observer = new MapperObserver[T, Seq[U], U](this, mapper, (stream, mappedValue) => {
      stream.pushAll(mappedValue: _*)
    })
    observer.mappedStream
  }

  /**
   * Applies a function to each value in the stream iteratively preserving the last computed value of the function.
   * @param initial Initial value which will be fed as last computed value while applying the function
   *                to the first element in the stream.
   * @param folder Function that needs to be applied to each element iteratively.
   *               The first parameter is the last computed value of the function (or initial value in case of first
   *               call) and the second parameter is the element being iterated.
   * @tparam U Type of computed value by folder function.
   * @return Future containing outputted value from the folder function applied on last element on stream.
   */
  def fold[U](initial: U)(folder: (U, T) => U): Future[U] = {
    val observer = new Observer[T] {
      val computedValuePromise = Promise[U]
      private var value = initial

      override def onElement(stream: PushStream[T], element: T): Unit = {
        value = folder(value, element)
      }

      override def onStreamComplete(stream: PushStream[T]): Unit = {
        computedValuePromise.success(value)
      }
    }
    this.observe(observer)
    observer.computedValuePromise.future
  }


  /** Helper for generating a new stream by applying a mapping function to an existing stream */
  private class MapperObserver[T, U, V](originalStream: PushStream[T], mapper: T => U,
                                        apply: (PushStream[V], U) => Unit) extends Observer[T] {
    val mappedStream = new PushStream[V]

    originalStream.observe(this)

    override def onElement(stream: PushStream[T], element: T): Unit = {
      val mappedValue = mapper(element)
      apply(mappedStream, mappedValue)
    }

    override def onStreamComplete(stream: PushStream[T]): Unit = {
      mappedStream.pushEOF()
    }
  }
}