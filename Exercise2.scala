package com.spark_scala_task

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

object Exercise2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Exercise2")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val rdd = sc.textFile("C:\\Users\\aditya.a.gaurav\\Documents\\SparkAssignments\\mail_sample2.txt")

    // The rdd will be cached as the same will be used in further processing of the code also
    rdd.cache()

    // Initialization of the few variables
    var From = ""
    var Sent = ""
    var To = ""
    var Cc = ""
    var Subject = ""
    var Body = ""

    // Extraction of "From", "Sent", "To", "Cc", "Subject", "Body" from the mail and assigning it to the initialized variables 
    val From_coll = rdd.filter(a => a.split(": ")(0).equals("From")).collect()
    if (From_coll.length > 0) {
      From = From_coll.apply(0).replaceAll("From: ", "")
    }

    val Sent_coll = rdd.filter(a => a.split(": ")(0).equals("Sent")).collect()
    if (Sent_coll.length > 0) {
      Sent = Sent_coll.apply(0).replaceAll("Sent: ", "")
    }

    val To_coll = rdd.filter(a => a.split(": ")(0).equals("To")).collect()
    if (To_coll.length > 0) {
      To = To_coll.apply(0).replaceAll("To: ", "")
    }

    val CC_coll = rdd.filter(a => a.split(": ")(0).equals("Cc")).collect()
    if (CC_coll.length > 0) {
      Cc = CC_coll.apply(0).replaceAll("Cc: ", "")
    }

    val Subject_coll = rdd.filter(a => a.split(": ")(0).equals("Subject")).collect()
    if (Subject_coll.length > 0) {
      Subject = Subject_coll.apply(0).replaceAll("Subject: ", "")
    }

    val Body_coll = rdd.filter(a => !(a.split(": ")(0).equals("Subject")
      || a.split(": ")(0).equals("From")
      || a.split(": ")(0).equals("Sent")
      || a.split(": ")(0).equals("To")
      || a.split(": ")(0).equals("Cc"))).collect()

    if (Body_coll.length > 0) {
      Body = Body_coll.mkString("|")
    }

    // Assigning schema/header to the extracted data after conversion from rdd to dataframe
    val DF = sc.parallelize(Seq((From, Sent, To, Cc, Subject, Body)))
      .toDF("From", "Sent_Time&Date", "To", "Cc", "Subject", "Body_OfMail")

    //DF.show()
    // Write the processed dataframe into csv file in specified location
    DF.repartition(1)
      .write.format("csv")
      .option("header", "true")
      .mode("overwrite")
      .csv("C:\\Users\\aditya.a.gaurav\\Documents\\SparkAssignments\\New folder\\Ex2New")
  }
}