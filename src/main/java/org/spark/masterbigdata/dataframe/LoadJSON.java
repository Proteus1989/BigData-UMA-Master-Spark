package org.spark.masterbigdata.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoadJSON {
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
                .option("header", "true")
                .option("inferschema", "true")
                .option("delimiter", ",")
                .json("./src/main/resources/restaurants.json");

        // Print inferred schema
        dataframe.printSchema();
        // Print 20 first rows
        dataframe.show();

        dataframe.groupBy(functions.col("borough"))
                .count()
                .show();

        StructType structType = new StructType()
                .add("borough", "String")
                .add("cuisine", "String")
                .add("name", "String")
                .add("restaurant_id", "String");

        dataframe = sparkSession
                .read()
                .schema(structType)
                .option("header", "true")
                .option("delimiter", ",")
                .json("./src/main/resources/restaurants.json");

        dataframe.show();

        dataframe
                .groupBy("borough")
                .count()
                .sort(functions.col("count").desc())
                .show();

        System.out.println(dataframe
                .filter(functions.col("borough").equalTo("Manhattan"))
                .count());

        System.out.println(dataframe
                .where("borough == 'Manhattan'")
                .count());

        dataframe.createOrReplaceTempView("restaurants");

        Dataset<Row> newDataframe = sparkSession.sql("SELECT * FROM restaurants WHERE borough = 'Manhattan'");

        newDataframe.show();
    }
}
