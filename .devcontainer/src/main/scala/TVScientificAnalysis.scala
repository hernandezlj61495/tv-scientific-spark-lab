// TV SCIENTIFIC SPACE VOTE ANALYTICS
import org.apache.spark.sql.SparkSession

object TVScientificAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark for space data processing
    val spark = SparkSession.builder()
      .appName("TVScientific Voting Analysis")
      .master("local[*]")
      .getOrCreate()

    // Create sample space voting data
    import spark.implicits._
    val voteData = Seq(
      ("Astronaut Cooking", 250, "USA"),
      ("Zero-G Experiments", 80, "JPN"),
      ("Space Gardening", 315, "MARS") // Yes, Mars colony votes!
    ).toDF("Show", "Votes", "Location")

    // TV Scientific business logic:
    val processedData = voteData
      .filter($"Votes" > 100)                 // Filter low-engagement
      .withColumn("AdValue", $"Votes" * 0.25) // Calculate ad revenue
      .filter($"Location" =!= "JPN")          // Exclude test region
    
    println("=== TV SCIENTIFIC SPACE VOTING REPORT ===")
    processedData.show()

    spark.stop()
  }
}
