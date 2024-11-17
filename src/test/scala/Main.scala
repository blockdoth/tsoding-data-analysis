
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.time.ZoneId


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
//    "#annoyncements.json"
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
    val writer = new PrintWriter("computed/messages_by_time_of_day.csv")
    writer.println("time,count")
    res.foreach { case (time, count) =>
      writer.println(s"$time,$count")
    }
    writer.close()

  }

  test("messagesByDayOfWeek") {
    val format = new SimpleDateFormat("EEE")
    val res = channels
      .flatMap(m => m.messages
        .map(t => (format.format(t.timestamp), 1))
      )
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    res.take(5).foreach(println)
    val writer = new PrintWriter("computed/messages_by_day_of_week.csv")
    writer.println("day,count")
    res.foreach { case (time, count) =>
      writer.println(s"$time,$count")
    }
    writer.close()

  }

  def getStremDays (channels:RDD[ChannelExtract]) = channels
    .filter(_.channel.name == "annoyncements")
    .flatMap(_.messages)
    .filter(_.content.startsWith("\uD83D\uDD34 **LIVE ON TWITCH**"))
    .map(_.timestamp.toLocalDateTime.toLocalDate)
    .collect()

  test("get strem dates"){
    getStremDays(channels).foreach(println)
  }


  test("stremVSnonStrem") {
    val stremDays = getStremDays(channels)

    val timestamps = channels
      .flatMap(m => m.messages
        .map(t => t.timestamp)
      )

    val format = new SimpleDateFormat("HH:mm")
    val splitMessages = timestamps.map(m => {
        if (stremDays.contains(m.toLocalDateTime.toLocalDate))
          (format.format(m), (1, 0))
        else
          (format.format(m), (0, 1))
      })
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .sortByKey()
      .collect()

    val writer = new PrintWriter("computed/strem_vs_non_strem.csv")

    writer.println("time,stremday,nonstremday")
    splitMessages.foreach { case (time, (stremday, nonstremday)) =>
      writer.println(s"$time,$stremday,$nonstremday")
    }
    writer.close()
  }

  test("messageOverTimeForEachChannel") {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val messages = channels
      .flatMap(m => m.messages
        .map(t => ((m.channel.name, format.format(t.timestamp)), 1))
      )
      .reduceByKey(_ + _)
      .sortByKey()
      .groupBy(_._1)
      .map(g => (g._1._1, g._1._2, g._2.map(_._2).sum))
      .collect()


    val writer = new PrintWriter("computed/messagesOverTimeForEachChannel.csv")
    writer.println("time,channel,count")

    messages.foreach { case (channel, time, count) =>
      writer.println(s"$time,$channel,$count")
    }
    writer.close()
  }


    test("fitVsBooks") {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val messages = channels
        .filter(_.channel.name == "fit")
        .flatMap(m => m.messages
          .map(t => (format.format(t.timestamp), 1))
        )
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()

      val writer = new PrintWriter("computed/fit_vs_books.csv")
      writer.println("time,count")
      messages.foreach { case (time, count) =>
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