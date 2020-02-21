package org.spark.masterbigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

/**
 * Program that reads a CSV and gets 20 elements of first column using Apache Spark
 *
 * @author Antonio Suárez
 */
public class ReadCSVFile {
    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Missing filepath argument\nPlease, edit a new run configuration setting './src/main/resources/Film_Locations_in_San_Francisco.csv' in program arguments field");

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Read CSV file").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile(args[0]);

        JavaRDD<String[]> fields = lines.map(line -> line.split(","));

        JavaRDD<String> filmsNames = fields.map(array -> array[0]);

        List<String> results = filmsNames
                //.distinct() // Filters duplicates
                .sortBy(s -> s, false, 1)
                .take(20);

        results.forEach(System.out::println);

        sparkContext.stop();
    }
}
