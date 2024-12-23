import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CustomerTransactionAnalysis {
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Customer Transaction Analysis")
      .master("local[*]") // Local mode for testing
      .getOrCreate()

    // Step 2: Load Dataset
    val filePath = "online_retail.csv" // Replace with your dataset path
    val rawData = spark.read
      .option("header", "true") // CSV has headers
      .option("inferSchema", "true") // Infer schema automatically
      .csv(filePath)

    println("Sample Data:")
    rawData.show(5)

    // Step 3: Clean and Prepare Data
    val cleanedData = rawData
      .withColumn("Amount", col("Amount").cast(DoubleType)) // Cast Amount to Double
      .withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd")) // Convert date
      .na.drop() // Drop rows with null values

    println("Cleaned Data:")
    cleanedData.show(5)

    // Step 4: Total Revenue
    val totalRevenue = cleanedData.agg(sum("Amount").alias("TotalRevenue"))
    println("Total Revenue:")
    totalRevenue.show()

    // Step 5: Top Customers by Spending
    val topCustomers = cleanedData.groupBy("CustomerID")
      .agg(sum("Amount").alias("TotalSpent"))
      .orderBy(desc("TotalSpent"))

    println("Top 10 Customers:")
    topCustomers.show(10)

    // Step 6: Most Popular Categories
    val popularCategories = cleanedData.groupBy("Category")
      .agg(count("ProductID").alias("TotalProductsSold"))
      .orderBy(desc("TotalProductsSold"))

    println("Top 10 Popular Categories:")
    popularCategories.show(10)

    // Step 7: Export Results (Optional)
    val outputDir = "path/to/output" // Replace with your desired output directory
    topCustomers.write.option("header", "true").csv(s"$outputDir/top_customers")
    popularCategories.write.option("header", "true").csv(s"$outputDir/popular_categories")
    totalRevenue.write.option("header", "true").csv(s"$outputDir/total_revenue")

    // Step 8: Use Spark SQL (Optional)
    cleanedData.createOrReplaceTempView("transactions")
    val sqlQuery = spark.sql(
      """
        |SELECT Category, COUNT(ProductID) AS TotalProducts
        |FROM transactions
        |GROUP BY Category
        |ORDER BY TotalProducts DESC
        |LIMIT 10
        |""".stripMargin)

    println("SQL Query Results (Top Categories):")
    sqlQuery.show()

    // Stop Spark Session
    spark.stop()
  }
}
