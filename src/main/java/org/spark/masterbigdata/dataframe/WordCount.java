package org.spark.masterbigdata.dataframe;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WordCount {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark program to count words")
                .master("local[8]")
                .getOrCreate();

        Logger.getLogger("").setLevel(Level.OFF);

        Dataset<Row> dataframe = sparkSession
                .read()
                .text("./src/main/resources/quijote.txt");

        String[] filters = new String[]{"que", "los", "con", "por", "las", "del", "como", "para", "una", "sin", "que,", "sus" };

        dataframe
                .flatMap(new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterator<String> call(Row row) throws Exception {
                        return Arrays.asList(row.getString(0).split(" ")).iterator();
                    }
                }, Encoders.STRING())
                .groupBy("value")
                .count()
                .filter(functions.length(functions.col("value")).gt(2))
                .filter(functions.not(functions.col("value").isInCollection(Arrays.asList(filters))))
                .filter(functions.col("count").gt(50))
                .sort(functions.col("count").desc())
                .show();


    }
}
