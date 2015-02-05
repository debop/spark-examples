package learningsparkexamples.streaming

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class StreamingFunSuite extends AbstractSparkFunSuite {

  test("streaming log input") {
    val conf = new SparkConf().setMaster("local").setAppName("StreamingLogInput")

    // Create a StreamContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream from all the input on port 7777
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 7777)
    processLines(lines)

    // start our streaming context and wait for it to "finish"
    ssc.start()

    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
  }

  def processLines(lines: DStream[String]): Unit = {
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()
  }
}
