# mini-project-spark-scala

**Customer Transaction Analysis – Mini Project**  
**Date**: December 2024  
**Technologies Used**: Apache Spark, Scala, Spark SQL, DataFrames, RDDs  
**Dataset**: [Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail)  
**Source**: UCI Machine Learning Repository  
**Description**: This dataset contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail. The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

---

## Overview

This mini-project focuses on analyzing customer transaction datasets using **Apache Spark** and **Scala**. The project aims to process large datasets (540k entries), clean and transform the data, and derive key business metrics that help stakeholders make data-driven decisions. The results are exported into **CSV format**, which is compatible with visualization tools like **Power BI** and **AWS QuickSight**.

By leveraging Spark's distributed computing capabilities, the project can efficiently handle large-scale transaction data, offering insights into total revenue, top customer spending, popular products, and more. 

---

## Features

- **Data Processing Pipeline**: Built a scalable pipeline using Apache Spark to load, clean, and transform transaction data.
  
- **Key Metrics Identification**: Extracted key business metrics such as total revenue, top customer spending, and popular products.
  
- **Data Analysis with Spark SQL and DataFrames**: Utilized Spark SQL and DataFrames for fast and efficient querying and manipulation of data.
  
- **Optimized for Performance**: Implemented optimizations (like data coalescing) to handle large datasets efficiently, ensuring fast processing times and reliable results.
  
- **Data Export**: Exported cleaned and processed data into CSV format, enabling easy integration with external visualization tools such as Power BI and AWS QuickSight.

---

## Get Started Guide

### 1. **Prerequisites**
Before you begin, ensure you have the following tools installed on your machine:

- **Java** (Java 8 or higher)  
  - Download and install Java from [Oracle's official website](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or via a package manager.
  
- **Apache Spark** (v3.0 or higher)  
  - Download and install Apache Spark from the [official Spark website](https://spark.apache.org/downloads.html). Follow the installation instructions for your operating system.

- **Scala** (v2.12 or higher)  
  - Download and install Scala from the [official Scala website](https://www.scala-lang.org/download/).

- **SBT** (Scala Build Tool)  
  - Download and install SBT from [here](https://www.scala-sbt.org/download.html) to compile and run the Scala project.

Of note: This project was developed with Java 11 to work with Apache Scala 3.x.x

### 2. **Install Hadoop (Optional for Local Setup)**

Although this project runs locally without Hadoop in a cluster, Hadoop's `winutils.exe` may be required for operations in Windows like writing output to csv. If you encounter issues related to Hadoop, follow these steps:

- Download the Hadoop binaries for Windows from [here](https://github.com/steveloughran/winutils).
- Set the environment variable `HADOOP_HOME` to the directory containing the downloaded Hadoop binaries.
- Ensure `winutils.exe` is located in the `bin/` directory inside `HADOOP_HOME`.

### 3. **Clone the Repository**

Clone the repository to your local machine:


### 4. **Run the Project**

Once everything is set up, you can run the project using `sbt run`:


### 5. **Access Output Data**

After the job completes, you will find the processed data printed in terminal and exported into the following output directories in CSV format:

- `./outputCSV/cleaned_data` – The cleaned transaction data.
- `./outputCSV/total_revenue` – The total revenue.
- `./outputCSV/top10_popular_items` – The top 10 popular items.
- `./outputCSV/top10_revenue_items` – The top 10 revenue-generating items.
- `./outputCSV/top10_spenders_overall` – The top 10 customers by spending overall.
- `./outputCSV/top_spenders_by_country_SQL` – The top spenders by country (SQL query results).


These files will be created in your specified output directory (by default, it's set to `./outputCSV`), and you can open them in any tool that supports CSV format, such as Excel, Power BI, AWS QuickSight, etc.


