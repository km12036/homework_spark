import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, desc, expr, lag, lead, mean, round, stddev, stddev_pop, stddev_samp, window}
import org.apache.spark.sql.expressions.Window

object stocks extends App {

  // Spark Assignment
  // Assignment Objectives
  // The file stock_prices.csv contains the daily closing price of a few stocks on the NYSE/NASDAQ

  import org.apache.log4j._
  Logger.getLogger("org").setLevel(Level.ERROR)

  val FPath="./src/resources/stock_prices.csv"
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()
  val df = session.read.option("header", "true").option("inferSchema", "true").format("csv").load(FPath)
  df.printSchema()

  // Compute the average daily return of every stock for every date. Print the results to screen
  // In other words, your output should have the columns:
  // date	average_return - yyyy-MM-dd	return of all stocks on that date

  val w = org.apache.spark.sql.expressions.Window.partitionBy("ticker").orderBy("date")


  val leadDf = df.withColumn("previous_close_data", lag("close", 1, 0).over(w))
  leadDf.show()

  val newDf = leadDf.withColumn("daily_return %",round((col("close")-col("previous_close_data"))/col("previous_close_data")*100,2))
    .na.drop()
  newDf.show()



  val dailyReturn = newDf.groupBy("date")
    .avg("daily_return %")


  val dailyFormated =  dailyReturn.select(col("date"),round(col("avg(daily_return %)"),2)
    .as("avg_daily_return %"))
    .orderBy("date")


  dailyFormated.show()

  // Save the results to the file as CSV

  val Path = "./src/resources/stock-CSV"
  dailyFormated.coalesce(1).write.option("header","true").format("csv").mode("overwrite").save(Path)

  // Which stock was traded most frequently - as measured by closing price * volume - on average?

  val frequentDf = df.withColumn("freq_trade_million",col("close")*col("volume")/1000000)
  val avgFreqDf = frequentDf.groupBy("ticker").avg("freq_trade_million")

  val mostFrequentlyTradedDf = avgFreqDf.select(col("ticker"),round(col("avg(freq_trade_million)"),2).as("traded most frequently, AVG_millions"))
    .orderBy(col("traded most frequently, AVG_millions").desc)
    .show(1)

  // Bonus Question
  // Which stock was the most volatile as measured by annualized standard deviation of daily returns?

  val stdev_df =  newDf.groupBy("ticker")
    .agg(stddev("daily_return %"))

  //standard deviation will return a low value that indicates low volatility

  stdev_df.select(col("ticker"),round(col("stddev_samp(daily_return %)"),2).as("st_dev of daily return"))
    .orderBy(col("stddev_samp(daily_return %)").desc)
    .show()
}
