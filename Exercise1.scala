package com.spark_scala_task

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.text.SimpleDateFormat

object Exercise1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Exercise1")
      .master("local[*]")
      .getOrCreate()

    // This import is needed to use the $-notation and For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /*
     * udf to pick month and year from the "Transaction_amount" column for partitioning
     */
    val format: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy") //Input date format
    val formatterM: SimpleDateFormat = new SimpleDateFormat("MMMM") //Output Month format
    val formatterYr: SimpleDateFormat = new SimpleDateFormat("yyyy") //Output Year format

    /*
     * udf to convert date into year
     */
    val yearConversion = udf((t1: String) =>
      if (t1 != null)
        formatterYr.format(format.parse(t1))
      else "")

    /*
     * udf to convert date into month
     */
    val monthConversion = udf((t1: String) =>
      if (t1 != null)
        formatterM.format(format.parse(t1)).toString()
      else "")

    /*
     * udf to remove comma for the called column "Transaction_amount"
     */
    val removeComma = udf((d: String) =>
      if (d != null)
        d.replaceAll(",", "").toString
      else "")

    /* 
       * udf to remove or trim spaces for the called column "Product"
       */
    val removeSpace = udf((d: String) =>
      if (d != null)
        d.trim().toString
      else "")

    // Load the .csv file into the dataframe
    val DF = spark.read.format("csv")
      .option("header", "true")
      .load("C:\\Users\\aditya.a.gaurav\\Documents\\SparkAssignments\\New folder\\Spark_scala_screening_assignment.csv")

    // Print the schema in a tree format    
    //peopleDF.printSchema()
    // Above defined udf's will be called for implementation and schema type casting for one column is done to Integer type in order to perform aggregation 
    val DFClean = DF.withColumn("Transaction_amount", removeComma(col("Transaction_amount")))
      .withColumn("Product", removeSpace(col("Product")))
      .withColumn("Transaction_amount", col("Transaction_amount").cast(IntegerType))

    // The above cleaned dataframe is persisted as it will be implemented further also.                      
    DFClean.persist()

    // Case1: Product with highest transaction amount is filtered.
    val DFMaxAmt = DFClean.groupBy("Product")
      .agg(max("Transaction_amount").as("MAX_Transaction_amount"))
      .orderBy(desc("MAX_Transaction_amount"))
      .limit(1).show()

    // Case2: Mastercard is famous in which country
    val DFMasterCard = DFClean.filter("Payment_Type = 'Mastercard'")
      .groupBy("Country")
      .agg(count("Country").as("Max_Cnt_In_Cntry"))
      .orderBy(desc("Max_Cnt_In_Cntry"))
      .limit(1).show()

    // Case3: Month wise transaction details for 2009
    // A dataframe is created by calling the udf to create 2 more columns of year and month to create partition based on year and month for the transaction
    val DFMonth = DFClean.withColumn("Year", yearConversion(col("Transaction_date"))).withColumn("Month", monthConversion(col("Transaction_date")))
    DFMonth.repartition(1)
      .write.format("csv")
      .option("header", "true")
      .mode("overwrite")
      .partitionBy("Year", "Month")
      .csv("C:\\Users\\aditya.a.gaurav\\Documents\\SparkAssignments\\New folder\\Case3")

    // Case4: Count of users with each type of cards
    val DFUserCntPerCards = DFClean.groupBy("Payment_Type")
      .agg(count("Payment_Type").as("User_Count_Per_Card"))
      .show()
  }
}