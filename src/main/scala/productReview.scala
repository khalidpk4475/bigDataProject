
import java.util.Properties
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.TypesafeMap.Key
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.graphx.Graph
//import org.itc.com.analyzeData
//import org.itc.com.sql_Demo.{members_df, spark, sqlQuery}

import scala.collection.immutable.List
import scala.util.Try
object productReview extends App{
  //System.setProperty("hadoop.home.dir", "C:\\hadoop")

  Logger.getLogger("org").setLevel(Level.ERROR)
  val host = "local[1]"
  //val host = "yarn"
  val spark: SparkSession = SparkSession.builder()
    .master(host)
    .appName("AmazonProductReviewEDA")
    .getOrCreate()


  val data = spark.read.option("header", "true").csv("C:\\Users\\khali\\OneDrive\\Desktop\\Data_Sets\\Amazon_product_review\\New folder\\1429_1.csv")
  //val data = spark.read.option("header", "true").csv(args(0))

  val isNumeric = udf((value: String) => Try(value.toDouble).isSuccess)
  val dfWithoutNonIntegerValues = data.filter(isNumeric(col("rating")))

  val cleanData = dfWithoutNonIntegerValues.withColumn("reviews_date", to_date(col("reviews_date")))

  val finalData = cleanData.select("id", "name", "asins", "brand", "categories",
    "manufacturer", "reviews_date", "reviews_doRecommend", "reviews_id", "reviews_numHelpful",
    "Rating", "reviews_text", "reviews_title", "reviews_username").na.drop(Seq("name"))

  finalData.createTempView("reviews")

  val wordsDF = finalData.select(
    explode(split(lower(col("reviews_text")), "\\W+")).as("word")
  ).filter("word != ''") // Filter out empty words

  // Word count
  val wordCountDF = wordsDF.groupBy("word").count()

  // Define a list of stop words
  val stopWords = List("the", "and", "in", "of", "a", "is", "it", "to", "for", "this", "on", "with", "as",
    "i", "my", "that", "was", "you", "use", "but", "have", "t")

  // Filter out stop words from the word count DataFrame
  val filteredWordCountDF = wordCountDF.filter(!col("word").isin(stopWords: _*))

  // Sort the word count in descending order
  val sortedWordCountDF = filteredWordCountDF.orderBy(desc("count"))

  // Display the most frequent words
  val topFrequentWordsDF = sortedWordCountDF.limit(10)
  topFrequentWordsDF.show()


  val df_Poduct_Rating = spark.sql("select name as Product_Name, avg(rating) as Average_Rating, count(rating) " +
    "as Number_of_Reviews from reviews group by name order by Average_rating desc").show()

  /*
  val singlePartitionDataFrame = df_Poduct_Rating.coalesce(1)
  singlePartitionDataFrame.write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(("C:/data"))
  */


  spark.sql("select categories as Category_Name, avg(rating) as Average_Rating, count(rating) " +
    "as Number_of_Reviews from reviews group by categories order by Average_rating desc").show()

  spark.sql("select brand as Brand_Name, avg(rating) as Average_Rating, count(rating) " +
    "as Number_of_Reviews from reviews group by brand order by Average_rating desc").show()

  spark.sql("select reviews_username as User_Name, avg(rating) as Average_Rating, count(rating) " +
    "as Number_of_Reviews from reviews  group by reviews_username order by " +
    "Number_of_Reviews desc").na.drop(Seq("User_Name")).show()


  val productReviewsDF = finalData


  // track the product ratings over time and identify trends or seasonal variations in customer satisfaction
  // Daily ratings
  spark.sql("select reviews_date as Date, avg(rating) as Average_Rating, " +
    "count(rating) as Number_of_Reviews from reviews group by reviews_date order by Date"
  ).na.drop(Seq("Date")).show()


  // Weekly ratings
  val weeklyRatingsDF = productReviewsDF
    .groupBy(weekofyear(col("reviews_date")).alias("week"),
      date_format(col("reviews_date"), "yyyy").alias("year"))
    .agg("rating" -> "avg")
    .orderBy("year", "week")
  weeklyRatingsDF.na.drop(Seq("week")).show()

  // Monthly ratings
  val monthlyRatingsDF = productReviewsDF
    .groupBy(month(col("reviews_date")).alias("month"),
      date_format(col("reviews_date"), "yyyy").alias("year"))
    .agg("rating" -> "avg")
    .orderBy("year").na.drop(Seq("month"))
  monthlyRatingsDF.show()

  val yearlyRatingsDF = productReviewsDF
    .groupBy(date_format(col("reviews_date"), "yyyy").alias("year"))
    .agg("rating" -> "avg").alias("average_rating")
    .orderBy("year").na.drop(Seq("year"))
    .withColumnRenamed("avg(rating)", "average_rating")

  val singlePartitionDataFrame = yearlyRatingsDF.coalesce(1)
  singlePartitionDataFrame.write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(("C:/data1"))

  yearlyRatingsDF.show()
}
