package com.spark.apiExamples;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

/*
val file = sc.textFile("catalina.out")
val errors = file.filter(line => line.contains("ERROR"))
*/
public class Examplecontainsfilter {
  public static void main(String[] args) {
    String logFile = "/tmp/mugunthan/README.md"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(line -> line.contains("a")).count();

    long numBs = logData.filter(line -> line.contains("b")).count();


    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}
