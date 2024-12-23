import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import java.io.File


object CustomerTransactionAnalysis {
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Customer Transaction Analysis")
      .master("local[*]") // Local mode for testing
      .config("spark.sql.shuffle.partitions", "4") // Set shuffle partitions for performance
      .getOrCreate()

    // Step 2: Load Dataset with Enhanced Data Exploration
    val filePath = "online_retail.csv" // Replace with your dataset path
    val rawData = spark.read
      .option("header", "true") // CSV has headers
      .option("inferSchema", "true") // Infer schema automatically
      .csv(filePath)

    // Perform basic exploration to understand dataset structure
    println("Sample Data:")
    rawData.show(5)

    // Print the schema to see the types of data
    println("Schema of the Data:")
    rawData.printSchema()

    // Step 3: Clean and Prepare Data
    val cleanedData = rawData
      .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType)) // Cast UnitPrice to Double
      .withColumn("Quantity", col("Quantity").cast(IntegerType)) // Cast Quantity to Integer
      .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "d/M/yy H:mm")) // Convert InvoiceDate to timestamp
      .na.drop() // Drop rows with null values

    // Step 4: Add Data Enrichment - Total Amount Calculation
    val dataWithTotalAmount = cleanedData
      .withColumn("TotalAmount", col("UnitPrice") * col("Quantity")) // Calculate total amount per transaction


    // Step 5: Aggregations and Data Insights using Dataframe

    // Total Revenue
    val totalRevenue = dataWithTotalAmount
      .agg(round(sum("TotalAmount"),2).alias("TotalRevenue"))

    println("Total Revenue Generated:")
    totalRevenue.show()

    val topPopularItems = dataWithTotalAmount
      .groupBy("StockCode", "Description")
      .agg(
      sum("Quantity").alias("TotalQuantitySold"),
      round(sum("TotalAmount"), 2).alias("TotalRevenue")
      )
      .orderBy(desc("TotalQuantitySold"))
      .limit(10)
      .withColumn("Rank", row_number().over(Window.orderBy(desc("TotalQuantitySold"))))
      .select("Rank", "StockCode", "Description", "TotalQuantitySold", "TotalRevenue")

    println("Top 10 items in terms of popularity (including revenue):")
    topPopularItems.show()

    // Top 10 items in terms of revenue
    val topItemSales = dataWithTotalAmount
      .groupBy("StockCode", "Description")
      .agg(round(sum("TotalAmount"), 2).alias("TotalRevenue"))
      .orderBy(desc("TotalRevenue"))
      .limit(10)
      .withColumn("Rank", row_number().over(Window.orderBy(desc("TotalRevenue"))))
      .select("Rank", "StockCode", "Description", "TotalRevenue")

    println("Top 10 items in terms of Revenue:")
    topItemSales.show()

    // Top 10 spenders overall
    val topSpendersOverall = dataWithTotalAmount
      .groupBy("CustomerID", "Country")
      .agg(round(sum("TotalAmount"), 2).alias("TotalSpent"))
      .orderBy(desc("TotalSpent"))
      .limit(10)
      .withColumn("Rank", row_number().over(Window.orderBy(desc("TotalSpent"))))
      .select("Rank", "CustomerID", "Country", "TotalSpent")

    println("Top 10 Spenders Overall:")
    topSpendersOverall.show()


    // Step 6: SQL Query using SparkSQL
    cleanedData.createOrReplaceTempView("transactions")
    
    val sqlQuery = spark.sql(
      """
        |SELECT Country, CustomerID, ROUND(SUM(UnitPrice * Quantity), 2) AS TotalSpending
        |FROM transactions
        |GROUP BY Country, CustomerID
        |ORDER BY Country, TotalSpending DESC
      """.stripMargin)

    // Use window function to find the top spender in each country
    val highestSpenderByCountry = sqlQuery
      .withColumn("Rank", rank().over(Window.partitionBy("Country").orderBy(desc("TotalSpending"))))
      .filter(col("Rank") === 1)  // Keep only the top spender in each country
      .drop("Rank")

    println("SQL Query Results (Highest Spender in Each Country):")
    highestSpenderByCountry.show()



    // Step 7: Performance Optimization
    // Cache data for faster subsequent operations (useful for larger datasets)
    dataWithTotalAmount.cache()
    
    // Step 8: Export Results
    val outputDir = "./outputCSV"
    new File(outputDir).mkdirs()

    // Save the cleaned data to a single CSV file in the output directory
    cleanedData.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite") //use append to append new output data into csv
      .csv(s"$outputDir/cleaned_data")

    // Save the total revenue to a single CSV file in the output directory
    totalRevenue.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$outputDir/total_revenue")

      // Save the top popular items to a single CSV file in the output directory
      topPopularItems.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(s"$outputDir/top10_popular_items")

      // Save the top revenue items to a single CSV file in the output directory
      topItemSales.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(s"$outputDir/top10_revenue_items")

    // Save the top customers to a single CSV file in the output directory
    topSpendersOverall.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$outputDir/top10_spenders_overall")

    // Save the total customers to a single CSV file in the output directory
    highestSpenderByCountry.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$outputDir/top_spenders_by_country_SQL")

    // Step 9: Stop Spark Session
    spark.stop()
  }
}
