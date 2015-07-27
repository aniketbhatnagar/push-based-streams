package examples

import streams.{TextFileSink, TextFileSource, Observer, PushStream}

import scala.collection.mutable.ListBuffer

/**
Part of our daily routine consists in efficiently processing time series. This process may

involve computing local information within a rolling time window of length T, such as the number of

points in the window, the minimum, maximum, or the rolling sum.

An example is given below, with T = 60 and

- T: number of seconds since epoch

- V: value (price ratio)

- N: number of observations in the current window

- RS: the current rolling sum in the current window

- minV: the minimum in the current window

- maxV: the maximum in the current window:

 T V N RS MinV MaxV

==================================================================================

1355270609 1.80215 1 1.80215 1.80215 1.80215

1355270621 1.80185 2 3.604 1.80185 1.80215

1355270646 1.80195 3 5.40595 1.80185 1.80215

1355270702 1.80225 2 3.6042 1.80195 1.80225

1355270702 1.80215 3 5.40635 1.80195 1.80225
......
 */
object WindowedReport {

  def main(args: Array[String]): Unit = {
    val (time, _) = timeAndRun {
      val filePath = this.getClass.getClassLoader.getResource("data.txt").getFile
      val outputPath = "output.txt"
      val source = new TextFileSource(filePath)
      val tsDataStream = source.stream.map(parseTSData)
      val reportStream = generateWindowedReport(tsDataStream, 60)
      val textReportStream = reportStream.map(reportData => reportData.toString)
      textReportStream.observe(new TextFileSink(outputPath))
      source.start()
    }
    println(s"Took ${time.toDouble / 1000} seconds")
  }

  def timeAndRun[T](func: => T): (Long, T) = {
    val start = System.currentTimeMillis()
    val output = func
    val diff = System.currentTimeMillis() - start
    (diff, output)
  }

  /** Parses a line from a file into TSData*/
  def parseTSData(line: String): TSData = {
    val splits = line.split("\t")
    TSData(splits(0).toLong, splits(1).toDouble)
  }

  /** generates report for a stream by windowing in the provided interval.
    * Big assumption is that the timestamp is monotonously increasing. */
  def generateWindowedReport(stream: PushStream[TSData], interval: Int): PushStream[TSReportData] = {
    val initialState = WindowState(ListBuffer(), 0, 0, -1, -1)
    stream.mapWithState(initialState)((oldState, tsData) => {
      val newState = if (oldState.startTimestamp == -1) {
        new WindowState(tsData)
      } else {
        oldState.absorb(tsData, interval)
      }
      (newState, new TSReportData(tsData, newState))
    })
  }

  /** Represents a time series data point */
  case class TSData(timestamp: Long, measurement: Double)

  /** Represents a data point in report */
  case class TSReportData(timestamp: Long, measurement: Double, numberOfObservations: Long, sum: Double, min: Double, max: Double) {
    def this(tsData: TSData, state: WindowState) = this(tsData.timestamp, tsData.measurement, state.numberOfObservations, state.sum, state.min, state.max)

    private def formatted(value: Double): String = {
      value.formatted("%.5f")
    }

    override def toString: String = {
      s"$timestamp\t${formatted(measurement)}\t$numberOfObservations\t${formatted(sum)}\t${formatted(min)}\t${formatted(max)}"
    }
  }

  /** Represents an intermittent state during processing of TSData points into TSReportData points */
  case class WindowState(bufferedData: ListBuffer[TSData], numberOfObservations: Long, sum: Double, min: Double, max: Double) {
    def this(data: TSData) = this(ListBuffer(data), 1, data.measurement, data.measurement, data.measurement)

    def absorb(data: TSData, interval: Int): WindowState = {
      var currentBuffer = bufferedData
      var skips = 0
      var skipSum = 0D
      while(currentBuffer.size > 0 && (data.timestamp - currentBuffer.head.timestamp) >= interval) {
        skips += 1
        skipSum += currentBuffer.head.measurement
        currentBuffer = currentBuffer.tail
      }
      val newBuffer = currentBuffer += data
      val (newMax, newMin) = if (skips > 0) {
        val values = newBuffer.map(_.measurement)
        (values.max, values.min)
      } else {
        (Math.max(max, data.measurement), Math.min(min, data.measurement))
      }
      WindowState(newBuffer, numberOfObservations - skips + 1, sum - skipSum + data.measurement, newMin, newMax)
    }

    def startTimestamp: Long = {
      if (bufferedData.size > 0) {
        bufferedData.head.timestamp
      } else {
        -1
      }
    }
  }
}
