package com.codecafe.hellospark.spark;

import static com.codecafe.hellospark.models.FieldNames.CARD_ISSUING_COUNTRY;
import static com.codecafe.hellospark.models.FieldNames.CHANNEL;
import static com.codecafe.hellospark.models.FieldNames.CURRENCY;
import static com.codecafe.hellospark.models.FieldNames.OUTLET_REFERENCE;
import static com.codecafe.hellospark.models.FieldNames.SHOPPER_LOCATION;
import static com.codecafe.hellospark.models.FieldNames.SHOPPER_TYPE;
import static com.codecafe.hellospark.models.FieldNames.TIMESTAMP;
import static com.mongodb.client.model.Filters.lte;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;

import com.codecafe.hellospark.models.JobDetails;
import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class DataAggregator {

  static final String DATABASE_NAME = "analytics-service";
  static final String SOURCE_COLLECTION = "transactionData";
  static final String JOB_DETAILS_COLLECTION = "jobDetails";
  static final String TARGET_COLLECTION = "aggregatedData";
  private final int timeToAdd = 30;
  private final int transactionDataTtlInDays = 30;
  private final SparkSession sparkSession;
  private final MongoCollection<Document> transactionDataCollection;
  private final MongoCollection<Document> jobDetailsCollection;

  @Value("${spring.data.mongodb.uri}")
  private final String mongoUri;

  public DataAggregator(String mongoUri) {
    this.sparkSession = SparkSession.active();
    this.mongoUri = mongoUri;

    MongoClient mongoClient = MongoClients.create(mongoUri);
    MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
    transactionDataCollection = database.getCollection(SOURCE_COLLECTION);
    jobDetailsCollection = database.getCollection(JOB_DETAILS_COLLECTION);
  }

  @VisibleForTesting
  public static void startSparkSession(String mongoUri) {
    SparkSession.builder()
      .master("local[1]")
      .appName("AnalyticsServiceSparkJob")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.mongodb.input.uri", mongoUri)
      .config("spark.mongodb.output.uri", mongoUri)
      .config("spark.mongodb.input.database", DATABASE_NAME)
      .config("spark.mongodb.output.database", DATABASE_NAME)
      .config("spark.sql.shuffle.partitions", "25")
      .getOrCreate();
  }

  @VisibleForTesting
  public static void stopSparkSession() {
    SparkSession.active().stop();
  }

  public void process() {
    log("DataAggregator Spark Job Triggered");

    JobDetails latestJobDetails = null;

    try {
      latestJobDetails = getLatestJobEndTime();
      TimeSlidingWindow timeSlidingWindow = new TimeSlidingWindow(latestJobDetails.getEndTime(), timeToAdd, MINUTES);

      while (timeSlidingWindow.getEndTime().isBefore(Instant.now())) {
        Instant startTime = timeSlidingWindow.getStartTime();
        Instant endTime = timeSlidingWindow.getEndTime();
        log("Processing data created between " + startTime + " and " + endTime);

        Dataset<Row> transactionDataDf = loadDataFromMongo(startTime, endTime);

        if (transactionDataDf.isEmpty()) {
          log("The transactionData collection is empty. No data to aggregate.");
        } else {
          transactionDataDf.show(false);
          Dataset<Row> aggregatedDataDf = groupAndAggregate(transactionDataDf, startTime, endTime);
          saveToMongo(aggregatedDataDf);
          purgeHistoricalRecords(startTime);
        }
        latestJobDetails.setEndTime(endTime);
        timeSlidingWindow.slide();
      }
    } catch (Exception exception) {
      exception.printStackTrace();
      sparkSession.stop();
    }
  }

  private Dataset<Row> loadDataFromMongo(Instant startTime, Instant endTime) {
    log("Loading data from MongoDB");

    Dataset<Row> data = sparkSession.read()
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", mongoUri)
      .option("database", DATABASE_NAME)
      .option("collection", SOURCE_COLLECTION)
      .load();
    return data.isEmpty() ? data : data.filter(col(TIMESTAMP).between(startTime, endTime));
  }

  private Dataset<Row> groupAndAggregate(Dataset<Row> transactionDataDf, Instant jobStartTime, Instant jobEndTime) {
    log("Performing aggregations");

    List<Dataset<Row>> dataframesToJoin = new ArrayList<>();

    // Aggregate shopperLocations
    Dataset<Row> shopperLocationsDf = aggregateBy(SHOPPER_LOCATION, "shopperLocations",
      transactionDataDf);
    if (!shopperLocationsDf.isEmpty()) {
      dataframesToJoin.add(shopperLocationsDf);
    }

    // Aggregate cardIssuingCountries
    Dataset<Row> cardIssuingCountriesDf = aggregateBy(CARD_ISSUING_COUNTRY, "cardIssuingCountries",
      transactionDataDf);
    if (!cardIssuingCountriesDf.isEmpty()) {
      dataframesToJoin.add(cardIssuingCountriesDf);
    }

    // Aggregate shopperTypes
    Dataset<Row> shopperTypesDf = aggregateBy(SHOPPER_TYPE, "shopperTypes",
      transactionDataDf);
    if (!shopperTypesDf.isEmpty()) {
      dataframesToJoin.add(shopperTypesDf);
    }

    // Use reduce to perform inner joins on the list of DataFrames
    Dataset<Row> df = dataframesToJoin.stream()
      .reduce((joinedDf, nextDf) -> {
        if (joinedDf.isEmpty()) {
          return nextDf;
        }

        nextDf = nextDf
          .withColumnRenamed("outletId", "next_outletId")
          .withColumnRenamed("currency", "next_currency")
          .withColumnRenamed("channel", "next_channel");

        joinedDf = joinedDf.join(nextDf,
          joinedDf.col("outletId").equalTo(nextDf.col("next_outletId"))
            .and(joinedDf.col("currency").equalTo(nextDf.col("next_currency")))
            .and(joinedDf.col("channel").equalTo(nextDf.col("next_channel"))),
          "full_outer");

        joinedDf = joinedDf
          .withColumn("outletId",
            coalesce(col("outletId"), col("next_outletId")))
          .withColumn("currency",
            coalesce(col("currency"), col("next_currency")))
          .withColumn("channel",
            coalesce(col("channel"), col("next_channel")))
          .drop("next_outletId", "next_currency", "next_channel");

        return joinedDf;
      })
      .orElse(sparkSession.emptyDataFrame());

    log("After 1st aggregation");
    df.show(false);

    if (!df.isEmpty()) {
      // Group and aggregate to get paymentTransactions
      df = df.groupBy(
          col("outletId"),
          col("currency")
        )
        .agg(
          collect_list(
            buildStructForPaymentTransactions(df)
          ).alias("paymentTransactions")
        )
        .select(
          col("outletId").alias("outletId"),
          lit(jobStartTime.toString()).alias("startTime"),
          lit(jobEndTime.toString()).alias("endTime"),
          col("paymentTransactions")
        );

      log("After 2nd aggregation");
      df.show(false);

      return df;
    }

    return sparkSession.emptyDataFrame();
  }

  @NotNull
  private Column buildStructForPaymentTransactions(Dataset<Row> df) {
    boolean hasShopperLocations = Arrays.asList(df.columns()).contains("shopperLocations");
    boolean hasCardIssuingCountries = Arrays.asList(df.columns()).contains("cardIssuingCountries");
    boolean hasShopperTypes = Arrays.asList(df.columns()).contains("shopperTypes");

    String structExpression = "named_struct(" +
      "'currency', currency, 'channel', channel";

    if (hasShopperLocations) {
      structExpression += ", 'shopperLocations', shopperLocations";
    }

    if (hasCardIssuingCountries) {
      structExpression += ", 'cardIssuingCountries', cardIssuingCountries";
    }

    if (hasShopperTypes) {
      structExpression += ", 'shopperTypes', shopperTypes";
    }

    structExpression += ")";

    return expr(structExpression);
  }

  private Dataset<Row> aggregateBy(String columnName, String aggregatedFieldName, Dataset<Row> transactionDataDf) {
    if (Arrays.asList(transactionDataDf.columns()).contains(columnName)) {

      Dataset<Row> filteredTransactionDataDf = transactionDataDf.filter(col(columnName).isNotNull());

      if (filteredTransactionDataDf.isEmpty()) {
        return sparkSession.emptyDataFrame();
      }

      Dataset<Row> dataset = filteredTransactionDataDf
        .groupBy(
          col(OUTLET_REFERENCE).alias("outletId"),
          col(CURRENCY).alias("currency"),
          col(CHANNEL).alias("channel"),
          col(columnName)
        )
        .agg(count("*").alias("count"))
        .groupBy(
          col("outletId"),
          col("currency"),
          col("channel")
        )
        .agg(
          collect_list(
            struct(
              col(columnName).alias("name"),
              col("count").alias("count")
            )
          ).alias(aggregatedFieldName)
        );
      log(aggregatedFieldName);
      dataset.show(false);
      return dataset;
    }
    return sparkSession.emptyDataFrame();
  }

  private void saveToMongo(Dataset<Row> aggregatedDataDf) {
    log("Saving aggregated data to MongoDB");
    long start = System.currentTimeMillis();

    if (!aggregatedDataDf.isEmpty()) {
      aggregatedDataDf.write()
        .format("mongo")
        .option("uri", mongoUri)
        .option("database", DATABASE_NAME)
        .option("collection", TARGET_COLLECTION)
        .option("createCollection", "true")
        .mode("append")
        .save();
    }
    log("Time to save to MongoDB: " + (System.currentTimeMillis() - start));
  }

  private JobDetails getLatestJobEndTime() {
    Document doc = jobDetailsCollection.find()
      .sort(Sorts.descending("endTime"))
      .first();

    if (doc == null) {
      return new JobDetails(Instant.now().minus(1, HOURS).truncatedTo(HOURS));
    }

    String id = String.valueOf(doc.get("_id"));
    Instant endTime = ((Date) doc.get("endTime")).toInstant();
    return new JobDetails(id, endTime);
  }

  private void saveJobDetails(JobDetails jobDetail) {
    if (jobDetail == null) {
      return;
    }
    log("Saving job details to Mongo with last successful run time " + jobDetail.getEndTime());

    String id = jobDetail.getId() == null ? String.valueOf(new ObjectId(new Date())) : jobDetail.getId();
    UpdateResult result = jobDetailsCollection.updateOne(
      new Document().append("_id", id),
      Updates.set("endTime", jobDetail.getEndTime()),
      new UpdateOptions().upsert(true));
    if ((result.getMatchedCount() == 1 && result.getModifiedCount() == 1) || result.getUpsertedId() != null) {
      log("Saved job details");
    }
  }

  private void purgeHistoricalRecords(Instant startTime) {
    log("Deleting transactionData records older than " + transactionDataTtlInDays + " days");
    Bson deleteRecordsFilter = lte(TIMESTAMP, startTime.minus(transactionDataTtlInDays, DAYS));
    try {
      DeleteResult result = transactionDataCollection.deleteMany(deleteRecordsFilter);
      log("Deleted document count: " + result.getDeletedCount());
    } catch (MongoException me) {
      System.err.println("Unable to delete due to an error: " + me);
    }
  }

  private void log(String msg) {
    System.out.println(DataAggregator.class.getCanonicalName() + " " + msg);
  }

}
