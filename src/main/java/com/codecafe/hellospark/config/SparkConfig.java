package com.codecafe.hellospark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Bean
    public SparkSession sparkSession() {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]") // Set the Spark master URL or remove this line for cluster deployment
                .setAppName("DataAggregator")
                .set("spark.mongodb.input.uri", mongoUri);


        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}