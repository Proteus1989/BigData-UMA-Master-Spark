package org.spark.masterbigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Program that gets the maximum number of a list using Apache Spark
 *
 * @author Antonio Suárez
 */
public class MaxNumber {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Max Number").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};

        List<Integer> integerList = Arrays.asList(numbers);

        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);

        int max = distributedList.max(Comparator.naturalOrder());

        System.out.println("The max is: " + max);

        sparkContext.stop();
    }
}
