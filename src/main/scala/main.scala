import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    // Example: Create a simple DataFrame and display it
    import spark.implicits._
    val data = Seq("Scala", "Spark", "SBT").toDF("Name")
    data.show()

    // Stop the SparkSession
    spark.stop()
  }
}