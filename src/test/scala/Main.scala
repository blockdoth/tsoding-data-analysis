
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.PrintWriter
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.reflect.io.Path


class StudentTest extends FunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val spark: SparkSession = SparkSession
    .builder
    .config("spark.driver.host", "localhost")
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.files.compress", "snappy")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "4g")
    .master("local[*]")
    .getOrCreate()


  // Used for conversions between dataframes and RDD's
  import spark.implicits._



  val paths = List(
    "#dirty-code.json",
    "#general-1.json",
    "#general-3.json",
    "#tsodinfeels.json"
//    "#bot-shrine.json",
//    "#politics.json",
//    "#debug-memes.json"
//    "*.json"

  ).map("data/" + _)

  val channels: RDD[ChannelExtract] = spark.sqlContext
    .read
    .option("multiLine", "true")
    .format("json")
    .json(paths: _*)
    .repartition(10)
    .as[ChannelExtract].rdd

//  channels.cache()

  test("hello world"){
    println("hello world")
  }


  test("Count reactions") {
    channels
      .flatMap(_.messages
        .flatMap(_.reactions)
      )
      .map(e => {
        val t = e.emoji.name
        println(t)
        t
      })
      .count()
  }

  test("Channel names") {
    channels
      .map(c => {
        val name = c.channel.name
        println(name)
        name
      })
      .toLocalIterator
      .toList
  }


  test("Penger Count") {
    channels
      .map(c => {
        val pengerCount = c.messages.count(_.content.contains("penger"))
        println(s"${c.channel.name} has ${pengerCount} penger mentions")
        pengerCount
      })
      .sum()
      .toLong
  }

  test("Users with more then x message in channel") {
    val threshold = 10

    channels
      .map(c => (c.channel.name,
        c.messages
          .map(_.author.name)
          .groupBy(identity)
          .mapValues(_.size)
          .count(_._2 > threshold)
      ))
      .toLocalIterator
      .toList
      .sortBy(_._2)
      .reverse
  }

  test("messagesByTimeOfDay") {
    val format = new SimpleDateFormat("HH:mm")
    val res = channels
      .flatMap(m => m.messages.map(t => format.format(t.timestamp)))
      .map(time => (time, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    val writer = new PrintWriter("data/messages_by_time_of_day.csv")
    writer.println("time,count")
    res.foreach { case (time, count) =>
      writer.println(s"$time,$count")
    }
    writer.close()

  }

  override def afterAll(): Unit = {
    //    Uncomment the line beneath if you want to inspect the Spark GUI in your browser, the url should be printed
    //    in the console during the start-up of the driver.
        Thread.sleep(9999999)
    //    You can uncomment the line below. Doing so will cause errors with `maven test`
//        spark.close()
  }

}