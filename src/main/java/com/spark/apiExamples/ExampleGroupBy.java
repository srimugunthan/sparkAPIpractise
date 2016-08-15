package com.spark.apiExamples;

// http://backtobazics.com/big-data/spark/apache-spark-groupby-example/
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ExampleGroupBy {
    public static void main(String[] args) throws Exception {
        
        JavaSparkContext sc = new JavaSparkContext();
        
        // Parallelized with 2 partitions
        JavaRDD<String> rddX = sc.parallelize(
                Arrays.asList("Joseph", "Jimmy", "Tina",
                        "Thomas", "James", "Cory",
                        "Christine", "Jackeline", "Juan"), 3);
        
        JavaPairRDD<Character, Iterable<String>> rddY = rddX.groupBy(word -> word.charAt(0));
        
        System.out.println(rddY.collect());
    }
}

