package org.spark.masterbigdata.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers with accumulator using Apache Spark
 *
 * @author Antonio Suárez
 */
public class AddNumbersWithAccumulator {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Add Numbers With Accumulator").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};

        List<Integer> integerList = Arrays.asList(numbers);

        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);

        LongAccumulator accumulator = sparkContext.sc().longAccumulator();

        int sum = distributedList.reduce((a,b) -> {
            accumulator.add(1);
            return a + b;
        });

        System.out.println("The sum is: " + sum);
        System.out.println("Number of reduces: " + accumulator.value());

        sparkContext.stop();
    }
}
