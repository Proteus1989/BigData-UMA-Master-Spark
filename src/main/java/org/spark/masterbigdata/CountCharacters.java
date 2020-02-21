package org.spark.masterbigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Program that filters words and count them using Apache Spark.
 * Also save in disk filtered words
 *
 * @author Antonio Suárez
 */
public class CountCharacters {
    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Missing filepath argument\nPlease, edit a new run configuration setting './src/main/resources/characters.txt' in program arguments field");

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Filter words").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile(args[0]).persist(StorageLevel.MEMORY_ONLY());

        long countAs = lines.filter(line -> line.contains("a")).count();
        long countBs = lines.filter(line -> line.contains("b")).count();

        System.out.println("As: " + countAs);
        System.out.println("Bs: " + countBs);

        lines.filter(line -> line.contains("c")).saveAsTextFile("./outputs/CountCharacters/output_cs"); // Needs hadoop

        sparkContext.stop();
    }
}
