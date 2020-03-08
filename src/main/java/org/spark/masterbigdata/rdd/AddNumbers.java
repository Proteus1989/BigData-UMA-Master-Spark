package org.spark.masterbigdata.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers using Apache Spark
 *
 * @author Antonio Suárez
 */
public class AddNumbers {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Add numbers").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};

        List<Integer> integerList = Arrays.asList(numbers);

        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);

        int sum = distributedList.reduce(Integer::sum);

        System.out.println("The sum is: " + sum);

        sparkContext.stop();
    }
}
