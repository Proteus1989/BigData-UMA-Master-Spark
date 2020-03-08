package org.spark.masterbigdata.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Program that shows 20 more used words in a file using Apache Spark
 *
 * @author Antonio Suárez
 */
public class WordCount {
    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Missing filepath argument\nPlease, edit a new run configuration setting './src/main/resources/quijote.txt' in program arguments field");

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("Word Count").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> groupedPairs = pairs.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> reversePairs = groupedPairs.mapToPair(groupedPair -> new Tuple2<>(groupedPair._2, groupedPair._1));

        List<Tuple2<Integer, String>> output = reversePairs.sortByKey(false).take(20);

        output.forEach(System.out::println);

        sparkContext.stop();
    }
}
