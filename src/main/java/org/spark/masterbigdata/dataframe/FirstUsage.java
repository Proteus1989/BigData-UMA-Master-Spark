package org.spark.masterbigdata.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;

public class FirstUsage {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark program to process a CSV file")
                .master("local[8]")
                .getOrCreate();

        Logger.getLogger("").setLevel(Level.OFF);

        Dataset<Row> dataframe = sparkSession
                .read()
                .option("header", "true")
                .option("inferschema", "true")
                .csv("./src/main/resources/Film_Locations_in_San_Francisco.csv");

        dataframe.printSchema();
        dataframe.show();
    }
}
