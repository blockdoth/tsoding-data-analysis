
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.PrintWriter
import java.text.SimpleDateFormat


class StudentTest extends FunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val spark: SparkSession = SparkSession
    .builder
    .config("spark.driver.host", "localhost")
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
//    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.driver.memory", "10g")
    .config("spark.executor.memory", "10g")
    .config("spark.sql.files.compress", "snappy")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "10g")
    .master("local[2]")
    .getOrCreate()


  // Used for conversions between dataframes and RDD's
  import spark.implicits._



  val paths = List(
//    "#annoyncements.json",
//    "#bot-shrine.json",
//    "#debug-memes.json",
//    "#dirty-code.json",
//    "#drawing.json",
//    "#fit.json",
//    "#food.json",
//    "#gaming.json",
//    "#gaming-minecraft.json",
//    "#general-1.json",
//    "#general-3.json",
//    "#introvert-party.json",
//    "#japanese-animations.json",
//    "#joins.json",
//    "#keyboards.json",
//    "#learning-foreign-languages.json",
//    "#music.json",
//    "#pets.json",
//    "#photos.json",
//    "#politics.json",
//    "#promo.json",
//    "#roles.json",
//    "#rules.json",
//    "#science.json",
//    "#tsodinfeels.json"
    "*.json"
  ).map("data/" + _)

  val channels: RDD[ChannelExtract] = spark.sqlContext
    .read
    .option("multiLine", "true")
    .format("json")
    .json(paths: _*)
    .repartition(16)
    .as[ChannelExtract].rdd

  channels.cache()

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
      .flatMap(m => m.messages
        .map(t => (format.format(t.timestamp), 1))
      )
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    res.take(5).foreach(println)
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
//        Thread.sleep(9999999)200
    //    You can uncomment the line below. Doing so will cause errors with `maven test`
//        spark.close()
  }

}