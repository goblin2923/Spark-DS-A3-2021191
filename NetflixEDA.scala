import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.io._

object NetflixEDA {

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Netflix Titles EDA")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    // Load the Netflix dataset
    val filePath = "path/to/netflix_titles.csv" // Replace with your dataset path
    val netflixDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(filePath)

    // Create output directory
    val outputDir = "output"
    new File(outputDir).mkdirs()

    // Save dataset schema
    val schemaFile = new PrintWriter(new File(s"$outputDir/schema.txt"))
    schemaFile.write("Dataset Schema:\n" + netflixDF.schema.treeString)
    schemaFile.close()

    // Save first 5 rows
    netflixDF.limit(5).coalesce(1)
      .write.option("header", "true")
      .csv(s"$outputDir/first_5_rows")

    // Save basic statistics
    val statsDF = netflixDF.describe()
    statsDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/basic_statistics")

    // Save total record count
    val totalRecords = netflixDF.count()
    val countFile = new PrintWriter(new File(s"$outputDir/total_records.txt"))
    countFile.write(s"Total Records: $totalRecords")
    countFile.close()

    // Save distribution by type (Movie/TV Show)
    val typeDistDF = netflixDF.groupBy("type")
      .count()
      .orderBy(desc("count"))
    typeDistDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/distribution_by_type")

    // Save top 5 countries with the most titles
    val topCountriesDF = netflixDF.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .limit(5)
    topCountriesDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/top_5_countries")

    // Save null value counts
    val nullCounts = netflixDF.columns.map { col =>
      val nullCount = netflixDF.filter(netflixDF(col).isNull || netflixDF(col) === "").count()
      s"$col: $nullCount null values"
    }.mkString("\n")
    val nullFile = new PrintWriter(new File(s"$outputDir/null_values.txt"))
    nullFile.write(nullCounts)
    nullFile.close()

    // Save distribution of ratings
    val ratingDistDF = netflixDF.groupBy("rating")
      .count()
      .orderBy(desc("count"))
    ratingDistDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/distribution_of_ratings")

    // Analyze and save average duration of Movies
    val cleanedDF = netflixDF.withColumn("duration_minutes",
      when(col("type") === "Movie", regexp_extract(col("duration"), "(\\d+)", 1).cast("int"))
        .otherwise(null)
    )
    val avgDurationMoviesDF = cleanedDF.filter(col("type") === "Movie")
      .select(avg("duration_minutes").as("avg_duration"))
    avgDurationMoviesDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/average_duration_movies")

    // Save distribution of TV show seasons
    val tvShowDistDF = cleanedDF.filter(col("type") === "TV Show")
      .groupBy("duration")
      .count()
      .orderBy(desc("count"))
    tvShowDistDF.coalesce(1).write.option("header", "true").csv(s"$outputDir/tv_show_distribution")

    // Stop the SparkSession
    spark.stop()

    println(s"EDA outputs saved to the '$outputDir' folder.")
  }
}
