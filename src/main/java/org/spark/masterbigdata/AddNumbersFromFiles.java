package org.spark.masterbigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers from file using Apache Spark
 *
 * @author Antonio Suárez
 */
public class AddNumbersFromFiles {
    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Missing filepath argument\nPlease, edit a new run configuration setting './src/main/resources/numbers.txt' in program arguments field");

        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1: create a SparkConf object
        SparkConf sparkConf = new SparkConf().setAppName("Add numbers").setMaster("local[4]");

        // Step 2: create a Java Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        int sum = sparkContext.textFile(args[0]).map(Integer::valueOf).reduce(Integer::sum);

        // Step 6: print the sum
        System.out.println("The sum is: " + sum);

        // Step 7: stop the spark context
        sparkContext.stop();
    }
}
