package com.codecafe.hellospark.spark;

import static com.codecafe.hellospark.models.Channel.ECOM;
import static com.codecafe.hellospark.models.Country.IN;
import static com.codecafe.hellospark.models.Country.US;
import static com.codecafe.hellospark.models.ShopperType.RETURNING;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;

import org.bson.UuidRepresentation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;

import com.codecafe.hellospark.models.AggregatedDataV2;
import com.codecafe.hellospark.models.AggregationItem;
import com.codecafe.hellospark.models.JobDetails;
import com.codecafe.hellospark.models.TransactionData;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DataAggregatorV2Test {

  private MongoServer server;
  private MongoOperations mongoOperations;
  private DataAggregatorV2 sparkDataAggregator;

  @BeforeEach
  public void setup() {
    server = new MongoServer(new MemoryBackend());
    InetSocketAddress serverAddress = server.bind();
    final String mongoUri = String.format("mongodb://%s:%d", serverAddress.getHostName(), serverAddress.getPort());
    MongoClient client = MongoClients.create(MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(mongoUri))
      .uuidRepresentation(UuidRepresentation.STANDARD)
      .build());
    SimpleMongoClientDatabaseFactory dbFactory = new SimpleMongoClientDatabaseFactory(client, DataAggregator.DATABASE_NAME);
    mongoOperations = new MongoTemplate(dbFactory);
    DataAggregatorV2.startSparkSession(mongoUri);
    sparkDataAggregator = new DataAggregatorV2(mongoUri);
  }

  @AfterEach
  public void tearDown() {
    DataAggregatorV2.stopSparkSession();
    server.shutdown();
  }

  @Test
  public void shouldNotProcessWhenNoDataIsFoundInSourceCollectionInTimeWindow() {
    insertTransactionData(TransactionData.builder()
      .outletId("outlet1")
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(1, MINUTES))
      .build());

    sparkDataAggregator.process();

    assertEquals(0, findAllAggregatedDataV2().size());
  }

  @Test
  public void shouldAggregateTransactionDataAsPerTimeRange() {
    final String outletId1 = "outlet1";
    final String outletId2 = "outlet2";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData3 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData4 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(US)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData5 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(US)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();
    TransactionData transactionData6 = TransactionData.builder()
      .outletId(outletId2)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(US)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(transactionData3);
    insertTransactionData(transactionData4);
    insertTransactionData(transactionData5);
    insertTransactionData(transactionData6);

    sparkDataAggregator.process();

    List<AggregatedDataV2> aggregatedData1 = findAggregatedDataV2ByOutletId(outletId1);
    assertNotNull(aggregatedData1);
    assertEquals(2, aggregatedData1.size());
    assertEquals(outletId1, aggregatedData1.get(0).getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData1.get(0).getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData1.get(0).getEndTime()));

    AggregationItem filter1 = aggregatedData1.get(0).getFilters().get(0);
    assertEquals("cha:ECOM,cur:INR,sloc:IN,sty:RETURNING,cic:IN", filter1.getName());
    assertEquals(2, filter1.getCount());

    filter1 = aggregatedData1.get(0).getFilters().get(1);
    assertEquals("cha:ECOM,cur:INR,sloc:IN,sty:RETURNING", filter1.getName());
    assertEquals(1, filter1.getCount());

    filter1 = aggregatedData1.get(0).getFilters().get(2);
    assertEquals("cha:ECOM,cur:INR,sloc:US,sty:RETURNING,cic:IN", filter1.getName());
    assertEquals(1, filter1.getCount());

    AggregationItem filter2 = aggregatedData1.get(1).getFilters().get(0);
    assertEquals("cha:ECOM,cur:INR,sloc:US,sty:RETURNING,cic:IN", filter2.getName());
    assertEquals(1, filter2.getCount());

    List<AggregatedDataV2> aggregatedData2 = findAggregatedDataV2ByOutletId(outletId2);
    assertEquals(1, aggregatedData2.size());
    assertEquals(outletId2, aggregatedData2.get(0).getOutletId());

    AggregationItem filter3 = aggregatedData2.get(0).getFilters().get(0);
    assertEquals("cha:ECOM,cur:INR,sloc:US,sty:RETURNING,cic:IN", filter3.getName());
    assertEquals(1, filter3.getCount());
  }

  private List<AggregatedDataV2> findAllAggregatedDataV2() {
    return mongoOperations.findAll(AggregatedDataV2.class, DataAggregatorV2.TARGET_COLLECTION);
  }

  private List<AggregatedDataV2> findAggregatedDataV2ByOutletId(String outletId) {
    return mongoOperations.find(Query.query(where("outletId").is(outletId)), AggregatedDataV2.class, DataAggregatorV2.TARGET_COLLECTION);
  }

  private void insertTransactionData(TransactionData transactionData) {
    mongoOperations.insert(transactionData, DataAggregatorV2.SOURCE_COLLECTION);
  }

  private void insertJobDetails(JobDetails jobDetails) {
    mongoOperations.insert(jobDetails, DataAggregatorV2.JOB_DETAILS_COLLECTION);
  }

}
