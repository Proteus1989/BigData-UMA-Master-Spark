package org.spark.masterbigdata.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyBasicOperations {
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
                .csv("./src/main/resources/datos.csv");

        // Print inferred schema
        dataframe.printSchema();
        // Print 20 first rows
        dataframe.show();

        System.out.println("Data types:");
        Arrays.stream(dataframe.dtypes()).forEach(System.out::println);

        dataframe.describe().show();

        dataframe.explain();

        dataframe.select("name").show();

        dataframe.select(dataframe.col("Age").$greater(60)).show();

        dataframe.select(dataframe.col("Name").startsWith("L")).show();

        dataframe.withColumn("Senior",
                dataframe.col("Age").$greater(45)
        ).show();

        dataframe.withColumnRenamed("HasACar", "Owner").show();

        dataframe.drop("BirthDate").show();

        JavaRDD<Row> rdd = dataframe.javaRDD().persist(StorageLevel.MEMORY_ONLY());

        rdd.collect().forEach(System.out::println);

        double sum_of_weight = rdd
                .mapToDouble(row -> (double) row.get(2))
                .sum();

        System.out.println(sum_of_weight);
    }
}
