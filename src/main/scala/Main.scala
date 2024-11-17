
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration

object Main {



  def countReactions(channels: RDD[ChannelExtract]): Long = {
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

  def channelNames(channels: RDD[ChannelExtract]): List[String] = {
    channels
      .map(c => {
        val name = c.channel.name
        println(name)
        name
      })
      .toLocalIterator
      .toList
  }

  def pengerCount(channels: RDD[ChannelExtract]): Long = {
    channels
      .map(c => {
        val pengerCount = c.messages.count(_.content.contains("penger"))
        println(s"${c.channel.name} has ${pengerCount} penger mentions")
        pengerCount
      })
      .sum()
      .toLong
  }

  def userWithMoreThanXMessageInChannel(channels: RDD[ChannelExtract], threshold: Int): List[(String, Int)] = {
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


  def messagesByTimeOfDay(channels: RDD[ChannelExtract]): List[(Int, Int)] = {

    channels
      .flatMap(m => m.messages.map(t => new SimpleDateFormat("HHmm").format(t.timestamp).toInt))
      .groupBy(identity)
      .mapValues(_.size)
      .toLocalIterator
      .toList
      .sortBy(_._1)
  }

}
