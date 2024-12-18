import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.io._

object NetflixEDA {

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null  // Declare the Spark session variable
    try {
      // Initialize SparkSession
      spark = SparkSession.builder()
        .appName("Netflix Titles EDA")
        .config("spark.master", "local[*]") // Local mode
        .getOrCreate()

      // Set log level to reduce verbosity
      spark.sparkContext.setLogLevel("running")

      // Load the Netflix dataset
      val filePath = "file:///" + new File("./netflix_titles.csv").getAbsolutePath
      val netflixDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(filePath)

      // Create output directory
      val outputDir = new File("output")
      if (!outputDir.exists()) outputDir.mkdirs()

      // Save dataset schema
      val schemaFile = new PrintWriter(new File(outputDir, "schema.txt"))
      schemaFile.write("Dataset Schema:\n" + netflixDF.schema.treeString)
      schemaFile.close()

      // Save first 5 rows
      netflixDF.limit(5).coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "first_5_rows").getAbsolutePath)

      // Save basic statistics
      val statsDF = netflixDF.describe()
      statsDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "basic_statistics").getAbsolutePath)

      // Save total record count
      val totalRecords = netflixDF.count()
      val countFile = new PrintWriter(new File(outputDir, "total_records.txt"))
      countFile.write(s"Total Records: $totalRecords")
      countFile.close()

      // Save distribution by type (Movie/TV Show)
      val typeDistDF = netflixDF.groupBy("type")
        .count()
        .orderBy(desc("count"))
      typeDistDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "distribution_by_type").getAbsolutePath)

      // Save top 5 countries with the most titles
      val topCountriesDF = netflixDF.groupBy("country")
        .count()
        .orderBy(desc("count"))
        .limit(5)
      topCountriesDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "top_5_countries").getAbsolutePath)

      // Save null value counts
      val nullCounts = netflixDF.columns.map { col =>
        val nullCount = netflixDF.filter(netflixDF(col).isNull || netflixDF(col) === "").count()
        s"$col: $nullCount null values"
      }.mkString("\n")
      val nullFile = new PrintWriter(new File(outputDir, "null_values.txt"))
      nullFile.write(nullCounts)
      nullFile.close()

      // Save distribution of ratings
      val ratingDistDF = netflixDF.groupBy("rating")
        .count()
        .orderBy(desc("count"))
      ratingDistDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "distribution_of_ratings").getAbsolutePath)

      // Analyze and save average duration of Movies
      val cleanedDF = netflixDF.withColumn("duration_minutes",
        when(col("type") === "Movie", regexp_extract(col("duration"), "(\\d+)", 1).cast("int"))
          .otherwise(null)
      )
      val avgDurationMoviesDF = cleanedDF.filter(col("type") === "Movie")
        .select(avg("duration_minutes").as("avg_duration"))
      avgDurationMoviesDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "average_duration_movies").getAbsolutePath)

      // Save distribution of TV show seasons
      val tvShowDistDF = cleanedDF.filter(col("type") === "TV Show")
        .groupBy("duration")
        .count()
        .orderBy(desc("count"))
      tvShowDistDF.coalesce(1)
        .write.option("header", "true")
        .csv(new File(outputDir, "tv_show_distribution").getAbsolutePath)

      // Stop the SparkSession
      spark.stop()

      println(s"EDA outputs saved to the '${outputDir.getAbsolutePath}' folder.")
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
        if (spark != null) spark.stop()
        System.exit(1)
    } finally {
      if (spark != null) spark.stop()
    }
  }
}
