
 


package com.spark.apiExamples;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;




import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import com.google.common.collect.Lists;
import scala.Tuple2;


/*
val file = sc.textFile("catalina.out")
val errors = file.filter(line => line.contains("ERROR"))

scala> val parallel = sc.parallelize(1 to 9)
parallel: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[477] at parallelize at <console>:12
 
scala> val par2 = sc.parallelize(5 to 15)
par2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[478] at parallelize at <console>:12
 
scala> parallel.union(par2).distinct.collect
res412: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)


*/
public class Exampleunion {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("union Application");
    JavaSparkContext sc = new JavaSparkContext(conf);

    //List<Integer> data = ;
    //List<Integer> data2 = ;

    JavaRDD<Integer> parll1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaRDD<Integer> parll2 = sc.parallelize(Arrays.asList(5, 6,7, 8, 9, 10));

    System.out.println(parll1.union(parll2).distinct().collect());
    System.out.println(parll2.intersection(parll1).collect());

  }
}
