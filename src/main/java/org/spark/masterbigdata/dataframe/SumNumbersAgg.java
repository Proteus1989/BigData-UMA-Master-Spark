package org.spark.masterbigdata.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SumNumbersAgg {
    public static void main(String[] args) {


        // Creates the session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Basic operations in Spark Dataframe")
                .master("local[8]")
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.OFF);

        // Load the CSV as dataframe
        Dataset<Row> dataframe = sparkSession
                .read()
                .option("header", "false")
                .option("inferschema", "true")
                .option("delimiter", ",")
                .text("./src/main/resources/numbers.txt");

        long start = System.currentTimeMillis();
        System.out.println("Aggregate: " + dataframe.select(functions.sum("value")).collectAsList().get(0).get(0));

        System.out.println("Total time: " + (System.currentTimeMillis() - start));

        sparkSession.close();
    }
}
