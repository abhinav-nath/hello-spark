package com.codecafe.hellospark.spark;

import com.codecafe.hellospark.models.AggregatedData;
import com.codecafe.hellospark.models.AggregationItem;
import com.codecafe.hellospark.models.OrderEventDataset;
import com.codecafe.hellospark.models.Payload;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.UUID.randomUUID;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;


@Slf4j
@Component
public class DataAggregator {

    private final MongoTemplate mongoTemplate;
    private final SparkSession sparkSession;
    private final int numberOfMinutes = 2;
    @Value("${spring.data.mongodb.database}")
    private String databaseName;
    @Value("${app.spark.mongodb.source.collection}")
    private String sourceCollection;
    @Value("${app.spark.mongodb.target.collection}")
    private String targetCollection;

    public DataAggregator(MongoTemplate mongoTemplate, SparkSession sparkSession) {
        this.mongoTemplate = mongoTemplate;
        this.sparkSession = sparkSession;
    }

    private static Dataset<Row> getRowDataset(Dataset<OrderEventDataset> eventsDS, String groupByColumn) {
        return eventsDS
                .groupBy(col(groupByColumn).as("name"))
                .agg(count("*").as("count"))
                .select(col("name"), col("count"));
    }

    @Scheduled(fixedRate = numberOfMinutes, timeUnit = TimeUnit.MINUTES)
    public void runSparkJob() {
        log.info("DataAggregator Spark Job Triggered");

        Instant startTime = Instant.now().minus(numberOfMinutes, MINUTES);
        Instant endTime = Instant.now();

        if (isCollectionEmpty()) {
            log.info("The orderEvents collection is empty. No data to aggregate.");
            return;
        }

        Encoder<OrderEventDataset> orderEventDatasetEncoder = Encoders.bean(OrderEventDataset.class);
        Encoder<Payload> payloadEncoder = Encoders.bean(Payload.class);

        Dataset<OrderEventDataset> eventsDS = sparkSession.read()
                .format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.input.database", databaseName)
                .option("spark.mongodb.input.collection", sourceCollection)
                .load()
                .filter(col("timestamp").between(startTime, endTime))
                .select("partitionKey", "timestamp", "payload")
                .as(orderEventDatasetEncoder);

        eventsDS.show(false);

        if (eventsDS.isEmpty()) {
            log.info("No new data found in last {} minutes. No data to aggregate.", numberOfMinutes);
            return;
        }

        eventsDS.createOrReplaceTempView("orderEvents");

        Dataset<Row> customerCountriesDS = getRowDataset(eventsDS, "payload.country");
        Dataset<Row> paymentChannelsDS = getRowDataset(eventsDS, "payload.paymentChannel");
        Dataset<Row> cardIssuerCountriesDS = getRowDataset(eventsDS, "payload.cardIssuerCountry");

        AggregatedData aggregatedData = AggregatedData.builder()
                .partitionKey(randomUUID().toString())
                .startTime(startTime.toString())
                .endTime(endTime.toString())
                .countries(buildListOfAggregationItems(customerCountriesDS))
                .paymentChannels(buildListOfAggregationItems(paymentChannelsDS))
                .cardIssuerCountries(buildListOfAggregationItems(cardIssuerCountriesDS))
                .build();

        mongoTemplate.save(aggregatedData, targetCollection);
    }

    private boolean isCollectionEmpty() {
        return sparkSession.read()
                .format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.input.database", databaseName)
                .option("spark.mongodb.input.collection", sourceCollection)
                .load()
                .isEmpty();
    }

    private List<AggregationItem> buildListOfAggregationItems(Dataset<Row> dataset) {
        dataset.show(false);

        List<AggregationItem> aggregationItems = new ArrayList<>();
        dataset.collectAsList().forEach(row -> {
            AggregationItem aggregationItem = AggregationItem.builder()
                    .name(row.getAs("name"))
                    .value(row.getAs("count"))
                    .build();
            aggregationItems.add(aggregationItem);
        });

        return aggregationItems;
    }

}