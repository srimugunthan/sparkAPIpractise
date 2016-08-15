

package com.spark.apiExamples;


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
scala> sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect
res200: Array[Int] = Array(1, 1, 1, 2, 2, 2, 3, 3, 3)
 
scala> sc.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect
res201: Array[List[Int]] = Array(List(1, 1, 1), List(2, 2, 2), List(3, 3, 3))

 
*/

	

public class ExamplemapFlatmap {
    public static void main(final String[] args) {
        final SparkConf sparkConf = new SparkConf();
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<Integer> parallelizeRDD = javaSparkContext.parallelize();
//	long count = parallelizeRDD.count();
  //      System.out.println("Count of items is"+ count);

	//System.out.println(parallelizeRDD.flatMap(x->Arrays.asList(x,x,x).iterator()).collect());
	//System.out.println(parallelizeRDD.map(x->Arrays.asList(x,x,x)).collect());


	System.out.println("  ====");

	System.out.println(javaSparkContext.parallelize(Arrays.asList(1,2,3)).flatMap(x->Arrays.asList(x,x,x)).collect());
	System.out.println(javaSparkContext.parallelize(Arrays.asList(1,2,3)).map(x->Arrays.asList(x,x,x)).collect());

///	System.out.println(javaSparkContext.parallelize(Lists.newArrayList(1,2,3)).flatMap(x->Arrays.asList(x,x,x).iterator()).collect());
	//System.out.println(javaSparkContext.parallelize(Arrays.asList(1,2,3)).map(x->Arrays.asList(x,x,x)).collect());

    }
}
