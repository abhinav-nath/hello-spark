package com.codecafe.hellospark.spark;

import static com.codecafe.hellospark.models.FieldNames.TIMESTAMP;
import static com.mongodb.client.model.Filters.lte;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import java.time.Instant;
import java.util.Date;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
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

public class DataAggregatorV2 {

  static final String DATABASE_NAME = "analytics-service";
  static final String SOURCE_COLLECTION = "transactionData";
  static final String JOB_DETAILS_COLLECTION = "jobDetails";
  static final String TARGET_COLLECTION = "aggregatedData";
  private final SparkSession sparkSession;
  private final MongoCollection<Document> transactionDataCollection;
  private final MongoCollection<Document> jobDetailsCollection;

  @Value("${spring.data.mongodb.uri}")
  private final String mongoUri;

  public DataAggregatorV2(String mongoUri) {
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
    log("DataAggregatorV2 Spark Job Triggered");

    JobDetails latestJobDetails;

    try {
      latestJobDetails = getLatestJobEndTime();
      int timeToAdd = 30;
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

    String[] filters = "cha,cur,sloc,sty,cic".split(",");

    Column nameCol = lit("");
    for (String filter : filters) {
      if (nameCol.toString().isEmpty()) {
        nameCol = concat(lit(filter + ":"), col(filter));
      } else {
        nameCol = concat_ws(",", nameCol,
          concat(lit(filter + ":"), col(filter)));
      }
    }

    Dataset<Row> df = transactionDataDf
      .groupBy("or", filters)
      .agg(count("*").as("count"))
      .withColumn("name", nameCol)
      .groupBy("or")
      .agg(collect_list(struct("name", "count")).as("filters"))
      .withColumn("outletId", col("or"))
      .withColumn("startTime", lit(jobStartTime.toString()))
      .withColumn("endTime", lit(jobEndTime.toString()))
      .select("outletId", "startTime", "endTime", "filters");

    log("After aggregation");
    df.show(false);

    return df;
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
    int transactionDataTtlInDays = 30;
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
    System.out.println(DataAggregatorV2.class.getCanonicalName() + " " + msg);
  }

}
