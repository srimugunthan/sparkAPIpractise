
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
scala> val names1 = sc.parallelize(List("abe", "abby", "apple")).map(a => (a, 1))
names1: org.apache.spark.rdd.RDD[(String, Int)] = MappedRDD[1441] at map at <console>:14
 
scala> val names2 = sc.parallelize(List("apple", "beatty", "beatrice")).map(a => (a, 1))
names2: org.apache.spark.rdd.RDD[(String, Int)] = MappedRDD[1443] at map at <console>:14
 
scala> names1.join(names2).collect
res735: Array[(String, (Int, Int))] = Array((apple,(1,1)))
[(apple,(1,1))]
 
scala> names1.leftOuterJoin(names2).collect
res736: Array[(String, (Int, Option[Int]))] = Array((abby,(1,None)), (apple,(1,Some(1))), (abe,(1,None)))
[(abe,(1,Optional.absent())), (abby,(1,Optional.absent())), (apple,(1,Optional.of(1)))] 
scala> names1.rightOuterJoin(names2).collect
res737: Array[(String, (Option[Int], Int))] = Array((apple,(Some(1),1)), (beatty,(None,1)), (beatrice,(None,1)))
[(apple,(Optional.of(1),1)), (beatrice,(Optional.absent(),1)), (beatty,(Optional.absent(),1))] 
*/

	

public class Examplejoin {
    public static void main(final String[] args) {
        final SparkConf sparkConf = new SparkConf();
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> parallelizeRDD = javaSparkContext.parallelize(Lists.newArrayList("abe", "abby", "apple"));
	JavaRDD<String> parallelizeRDD2 = javaSparkContext.parallelize(Lists.newArrayList("apple", "beatty", "beatrice"));
	long count = parallelizeRDD.count();
        System.out.println("Count of items is"+ count);
	//JavaRDD<String> names1 = parallelizeRDD.map(a ->   Tuple2<String, Integer>(a,1));
	JavaPairRDD<String, Integer>  names1 = parallelizeRDD.mapToPair(a ->  new Tuple2<String, Integer>(a,1));
	JavaPairRDD<String, Integer>  names2 = parallelizeRDD2.mapToPair(a ->  new Tuple2<String, Integer>(a,1));




	System.out.println(names1.join(names2).collect());
	System.out.println(names1.leftOuterJoin(names2).collect());
	System.out.println(names1.rightOuterJoin(names2).collect());
	
    }
}
