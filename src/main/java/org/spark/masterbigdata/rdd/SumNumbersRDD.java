package org.spark.masterbigdata.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Program that sums a list of numbers from file using Apache Spark
 *
 * @author Antonio Suárez
 */
public class SumNumbersRDD {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Add numbers").setMaster("local[8]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        long start = System.currentTimeMillis();
        int sum = sparkContext.textFile("./src/main/resources/numbers.txt").map(Integer::valueOf).reduce(Integer::sum);

        System.out.println("RDD: " + sum);
        System.out.println("Total time: " + (System.currentTimeMillis() - start));

        sparkContext.stop();
    }
}
