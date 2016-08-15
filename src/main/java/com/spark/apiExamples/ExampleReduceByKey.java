package com.spark.apiExamples;

//http://backtobazics.com/big-data/spark/apache-spark-groupby-example/
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ExampleReduceByKey {
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext();
        
        //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceSumFunc = (accum, n) -> (accum + n);
        
        
        // Parallelized with 2 partitions
        JavaRDD<String> x = sc.parallelize(
                        Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b"),
                        3);
        
        // PairRDD parallelized with 3 partitions
        // mapToPair function will map JavaRDD to JavaPairRDD
        JavaPairRDD<String, Integer> rddX = 
                        x.mapToPair(e -> new Tuple2<String, Integer>(e, 1));
        
        // New JavaPairRDD 
        JavaPairRDD<String, Integer> rddY = rddX.reduceByKey(reduceSumFunc);
        
        //Print tuples
        for(Tuple2<String, Integer> element : rddY.collect()){
            System.out.println("("+element._1+", "+element._2+")");
        }


    }
}
