package com.codecafe.hellospark.spark;

import static com.codecafe.hellospark.models.Channel.ECOM;
import static com.codecafe.hellospark.models.Channel.POS;
import static com.codecafe.hellospark.models.Country.AE;
import static com.codecafe.hellospark.models.Country.IN;
import static com.codecafe.hellospark.models.Country.US;
import static com.codecafe.hellospark.models.ShopperType.NEW;
import static com.codecafe.hellospark.models.ShopperType.RETURNING;
import static java.lang.System.out;
import static java.lang.System.setOut;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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

import com.codecafe.hellospark.models.AggregatedData;
import com.codecafe.hellospark.models.AggregationItem;
import com.codecafe.hellospark.models.JobDetails;
import com.codecafe.hellospark.models.PaymentTransaction;
import com.codecafe.hellospark.models.TransactionData;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DataAggregatorTest {

  private MongoServer server;
  private MongoOperations mongoOperations;
  private DataAggregator sparkDataAggregator;

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
    DataAggregator.startSparkSession(mongoUri);
    sparkDataAggregator = new DataAggregator(mongoUri);
  }

  @AfterEach
  public void tearDown() {
    DataAggregator.stopSparkSession();
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

    assertEquals(0, findAllAggregatedData().size());
  }

  @Test
  public void shouldAggregateTransactionDataAsPerTimeRange() {
    final String outletId1 = "outlet1";

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
      .shopperLocation(US)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();
    TransactionData recordToBeDeleted = TransactionData.builder()
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(30, DAYS))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(recordToBeDeleted);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedData = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedData);
    assertEquals(2, aggregatedData.size());
    assertEquals(outletId1, aggregatedData.get(0).getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.get(0).getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(0).getEndTime()));
    assertEquals(1, aggregatedData.get(0).getPaymentTransactions().size());

    PaymentTransaction paymentTransaction = aggregatedData.get(0).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    AggregationItem shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(IN.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());

    assertEquals(outletId1, aggregatedData.get(1).getOutletId());
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(1).getStartTime()));
    assertEquals(timestamp.plus(60, MINUTES), Instant.parse(aggregatedData.get(1).getEndTime()));
    assertEquals(1, aggregatedData.get(1).getPaymentTransactions().size());

    paymentTransaction = aggregatedData.get(1).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(US.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());
    assertEquals(2, findAllTransactionData().size());
  }

  @Test
  public void shouldAggregateTransactionDataBasedOnPreviousJobDetailsRecord() {
    insertJobDetails(JobDetails.builder().endTime(now().minus(1, HOURS)).build());

    TransactionData transactionData = TransactionData.builder()
      .outletId("outlet1")
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(now().minus(31, MINUTES))
      .build();

    TransactionData recordToBeDeleted = TransactionData.builder()
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(30, DAYS))
      .build();
    TransactionData recordNotToBeDeleted = TransactionData.builder()
      .outletId("OldRecord")
      .timestamp(now().plus(5, MINUTES).truncatedTo(HOURS).minus(30, DAYS))
      .build();

    insertTransactionData(transactionData);
    insertTransactionData(recordToBeDeleted);
    insertTransactionData(recordNotToBeDeleted);

    sparkDataAggregator.process();

    assertEquals(1, findAllAggregatedData().size());
    assertEquals("outlet1", findAllTransactionData().get(0).getOutletId());
    assertEquals("OldRecord", findAllTransactionData().get(1).getOutletId());
  }

  @Test
  public void shouldNotProcessWhenNoDataIsFoundInSourceCollection() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayOutputStream);
    setOut(printStream);

    try {
      sparkDataAggregator.process();
    } finally {
      out.flush();
      setOut(out);
      assertTrue(byteArrayOutputStream.toString()
        .contains("The transactionData collection is empty. No data to aggregate."));
    }
  }

  @Test
  public void shouldAggregateOnlyShopperLocations() {
    final String outletId1 = "outlet1";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(IN)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperLocation(US)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();
    TransactionData recordToBeDeleted = TransactionData.builder()
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(30, DAYS))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(recordToBeDeleted);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedData = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedData);
    assertEquals(2, aggregatedData.size());
    assertEquals(outletId1, aggregatedData.get(0).getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.get(0).getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(0).getEndTime()));
    assertEquals(1, aggregatedData.get(0).getPaymentTransactions().size());

    PaymentTransaction paymentTransaction = aggregatedData.get(0).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    AggregationItem shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(IN.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());

    assertEquals(outletId1, aggregatedData.get(1).getOutletId());
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(1).getStartTime()));
    assertEquals(timestamp.plus(60, MINUTES), Instant.parse(aggregatedData.get(1).getEndTime()));
    assertEquals(1, aggregatedData.get(1).getPaymentTransactions().size());

    paymentTransaction = aggregatedData.get(1).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(US.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());
    assertEquals(2, findAllTransactionData().size());
  }

  @Test
  public void shouldNotAggregateNullValues() {
    final String outletId1 = "outlet1";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .cardIssuingCountry(IN)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .timestamp(timestamp.plus(31, MINUTES))
      .build();
    TransactionData recordToBeDeleted = TransactionData.builder()
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(30, DAYS))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(recordToBeDeleted);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedData = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedData);
    assertEquals(1, aggregatedData.size());
    assertEquals(outletId1, aggregatedData.get(0).getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.get(0).getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(0).getEndTime()));
    assertEquals(1, aggregatedData.get(0).getPaymentTransactions().size());

    PaymentTransaction paymentTransaction = aggregatedData.get(0).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    AggregationItem cardIssuingCountry = paymentTransaction.getCardIssuingCountries().get(0);
    assertEquals(IN.name(), cardIssuingCountry.getName());
    assertEquals(new Long(1), cardIssuingCountry.getCount());
    assertEquals(2, findAllTransactionData().size());
  }

  @Test
  public void shouldAggregateShopperLocationsCardIssuingCountriesAndShopperTypesWithDifferentChannels() {
    final String outletId1 = "outlet1";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .shopperLocation(AE)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(POS)
      .shopperLocation(AE)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData recordToBeDeleted = TransactionData.builder()
      .timestamp(now().minus(1, HOURS).truncatedTo(HOURS).minus(30, DAYS))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(recordToBeDeleted);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedDataList = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedDataList);
    assertEquals(1, aggregatedDataList.size());

    AggregatedData aggregatedData = aggregatedDataList.get(0);

    assertEquals(outletId1, aggregatedData.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.getEndTime()));

    assertEquals(2, aggregatedData.getPaymentTransactions().size());
    PaymentTransaction paymentTransaction1 = aggregatedData.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction1.getCurrency());
    assertEquals("ECOM", paymentTransaction1.getChannel());

    AggregationItem shopperLocations = paymentTransaction1.getShopperLocations().get(0);
    assertEquals(AE.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());

    AggregationItem cardIssuingCountries = paymentTransaction1.getCardIssuingCountries().get(0);
    assertEquals(AE.name(), cardIssuingCountries.getName());
    assertEquals(new Long(1), cardIssuingCountries.getCount());

    AggregationItem shopperTypes = paymentTransaction1.getShopperTypes().get(0);
    assertEquals(NEW.name(), shopperTypes.getName());
    assertEquals(new Long(1), shopperTypes.getCount());

    PaymentTransaction paymentTransaction2 = aggregatedData.getPaymentTransactions().get(1);
    assertEquals("AED", paymentTransaction2.getCurrency());
    assertEquals("POS", paymentTransaction2.getChannel());

    shopperLocations = paymentTransaction2.getShopperLocations().get(0);
    assertEquals(AE.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());

    cardIssuingCountries = paymentTransaction2.getCardIssuingCountries().get(0);
    assertEquals(AE.name(), cardIssuingCountries.getName());
    assertEquals(new Long(1), cardIssuingCountries.getCount());

    shopperTypes = paymentTransaction2.getShopperTypes().get(0);
    assertEquals(NEW.name(), shopperTypes.getName());
    assertEquals(new Long(1), shopperTypes.getCount());
    assertEquals(2, findAllTransactionData().size());
  }

  @Test
  public void shouldAggregateShopperDataForSameOutletIntoDifferentRecordsIfCurrenciesAreDifferent() {
    final String outletId = "outlet1";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .shopperLocation(AE)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId)
      .orderReference(randomUUID().toString())
      .currency("USD")
      .channel(ECOM)
      .shopperLocation(AE)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedDataList1 = findAggregatedDataByOutletId(outletId);
    assertNotNull(aggregatedDataList1);
    assertEquals(2, aggregatedDataList1.size());

    AggregatedData aggregatedData1 = aggregatedDataList1.get(0);

    assertEquals(outletId, aggregatedData1.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData1.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData1.getEndTime()));

    assertEquals(1, aggregatedData1.getPaymentTransactions().size());
    PaymentTransaction paymentTransaction = aggregatedData1.getPaymentTransactions().get(0);
    assertEquals("USD", paymentTransaction.getCurrency());
    assertEquals("ECOM", paymentTransaction.getChannel());

    List<AggregationItem> shopperLocations = paymentTransaction.getShopperLocations();
    assertEquals(1, shopperLocations.size());
    assertEquals(AE.name(), shopperLocations.get(0).getName());
    assertEquals(new Long(1), shopperLocations.get(0).getCount());

    List<AggregationItem> cardIssuingCountries = paymentTransaction.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(AE.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(1), cardIssuingCountries.get(0).getCount());

    List<AggregationItem> shopperTypes = paymentTransaction.getShopperTypes();
    assertEquals(1, shopperTypes.size());
    assertEquals(NEW.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());

    AggregatedData aggregatedData2 = aggregatedDataList1.get(1);

    paymentTransaction = aggregatedData2.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction.getCurrency());
    assertEquals("ECOM", paymentTransaction.getChannel());

    shopperLocations = paymentTransaction.getShopperLocations();
    assertEquals(1, shopperLocations.size());
    assertEquals(AE.name(), shopperLocations.get(0).getName());
    assertEquals(new Long(1), shopperLocations.get(0).getCount());

    cardIssuingCountries = paymentTransaction.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(AE.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(1), cardIssuingCountries.get(0).getCount());

    shopperTypes = paymentTransaction.getShopperTypes();
    assertEquals(1, shopperTypes.size());
    assertEquals(NEW.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());
  }

  @Test
  public void shouldAggregateOtherFieldsWhenMerchantDoesNotSendShopperLocation() {
    final String outletId1 = "outlet1";
    final String outletId2 = "outlet2";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .cardIssuingCountry(AE)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData3 = TransactionData.builder()
      .outletId(outletId2)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .shopperType(RETURNING)
      .cardIssuingCountry(IN)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(transactionData3);

    sparkDataAggregator.process();

    // validate data for outlet1

    List<AggregatedData> aggregatedDataList1 = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedDataList1);
    assertEquals(1, aggregatedDataList1.size());

    AggregatedData aggregatedData = aggregatedDataList1.get(0);

    assertEquals(outletId1, aggregatedData.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.getEndTime()));

    assertEquals(1, aggregatedData.getPaymentTransactions().size());
    PaymentTransaction paymentTransaction = aggregatedData.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction.getCurrency());
    assertEquals("ECOM", paymentTransaction.getChannel());

    assertNull(paymentTransaction.getShopperLocations());

    List<AggregationItem> cardIssuingCountries = paymentTransaction.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(AE.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(2), cardIssuingCountries.get(0).getCount());

    List<AggregationItem> shopperTypes = paymentTransaction.getShopperTypes();
    assertEquals(2, shopperTypes.size());
    assertEquals(NEW.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());

    assertEquals(RETURNING.name(), shopperTypes.get(1).getName());
    assertEquals(new Long(1), shopperTypes.get(1).getCount());

    // validate data for outlet2

    List<AggregatedData> aggregatedDataList2 = findAggregatedDataByOutletId(outletId2);
    assertNotNull(aggregatedDataList2);
    assertEquals(1, aggregatedDataList2.size());

    aggregatedData = aggregatedDataList2.get(0);

    assertEquals(outletId2, aggregatedData.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.getEndTime()));

    assertEquals(1, aggregatedData.getPaymentTransactions().size());
    paymentTransaction = aggregatedData.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction.getCurrency());
    assertEquals("ECOM", paymentTransaction.getChannel());

    assertNull(paymentTransaction.getShopperLocations());

    cardIssuingCountries = paymentTransaction.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(IN.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(1), cardIssuingCountries.get(0).getCount());

    shopperTypes = paymentTransaction.getShopperTypes();
    assertEquals(1, shopperTypes.size());
    assertEquals(RETURNING.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());
  }

  @Test
  public void shouldAggregateShopperTypesForAnOutletForWhichShopperLocationsAndCardIssuingCountriesAreNotPresent() {
    final String outletId1 = "outlet1";
    final String outletId2 = "outlet2";

    Instant timestamp = now().minus(1, HOURS).truncatedTo(HOURS);

    TransactionData transactionData1 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .shopperLocation(AE)
      .cardIssuingCountry(AE)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData2 = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(POS)
      .shopperLocation(US).cardIssuingCountry(AE).shopperType(RETURNING)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();
    TransactionData transactionData3 = TransactionData.builder()
      .outletId(outletId2)
      .orderReference(randomUUID().toString())
      .currency("AED")
      .channel(ECOM)
      .shopperType(NEW)
      .timestamp(timestamp.plus(29, MINUTES))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(transactionData3);

    sparkDataAggregator.process();

    // validate data for outlet1

    List<AggregatedData> aggregatedDataList1 = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedDataList1);
    assertEquals(1, aggregatedDataList1.size());

    AggregatedData aggregatedData = aggregatedDataList1.get(0);

    assertEquals(outletId1, aggregatedData.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.getEndTime()));

    assertEquals(2, aggregatedData.getPaymentTransactions().size());
    PaymentTransaction paymentTransaction1 = aggregatedData.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction1.getCurrency());
    assertEquals("ECOM", paymentTransaction1.getChannel());

    List<AggregationItem> shopperLocations = paymentTransaction1.getShopperLocations();
    assertEquals(1, shopperLocations.size());
    assertEquals(AE.name(), shopperLocations.get(0).getName());
    assertEquals(new Long(1), shopperLocations.get(0).getCount());

    List<AggregationItem> cardIssuingCountries = paymentTransaction1.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(AE.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(1), cardIssuingCountries.get(0).getCount());

    List<AggregationItem> shopperTypes = paymentTransaction1.getShopperTypes();
    assertEquals(1, shopperTypes.size());
    assertEquals(NEW.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());

    PaymentTransaction paymentTransaction2 = aggregatedData.getPaymentTransactions().get(1);
    assertEquals("AED", paymentTransaction2.getCurrency());
    assertEquals("POS", paymentTransaction2.getChannel());

    shopperLocations = paymentTransaction2.getShopperLocations();
    assertEquals(1, shopperLocations.size());
    assertEquals(US.name(), shopperLocations.get(0).getName());
    assertEquals(new Long(1), shopperLocations.get(0).getCount());

    cardIssuingCountries = paymentTransaction2.getCardIssuingCountries();
    assertEquals(1, cardIssuingCountries.size());
    assertEquals(AE.name(), cardIssuingCountries.get(0).getName());
    assertEquals(new Long(1), cardIssuingCountries.get(0).getCount());

    shopperTypes = paymentTransaction2.getShopperTypes();
    assertEquals(1, shopperTypes.size());
    assertEquals(RETURNING.name(), shopperTypes.get(0).getName());
    assertEquals(new Long(1), shopperTypes.get(0).getCount());

    // validate data for outlet2

    List<AggregatedData> aggregatedDataList2 = findAggregatedDataByOutletId(outletId2);
    assertNotNull(aggregatedDataList2);
    assertEquals(1, aggregatedDataList2.size());

    aggregatedData = aggregatedDataList2.get(0);

    assertEquals(outletId2, aggregatedData.getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.getEndTime()));

    assertEquals(1, aggregatedData.getPaymentTransactions().size());
    paymentTransaction1 = aggregatedData.getPaymentTransactions().get(0);
    assertEquals("AED", paymentTransaction1.getCurrency());
    assertEquals("ECOM", paymentTransaction1.getChannel());

    assertNull(paymentTransaction1.getShopperLocations());
    assertNull(paymentTransaction1.getCardIssuingCountries());

    shopperLocations = paymentTransaction1.getShopperTypes();
    assertEquals(1, shopperLocations.size());
    assertEquals(NEW.name(), shopperLocations.get(0).getName());
    assertEquals(new Long(1), shopperLocations.get(0).getCount());
  }

  @Test
  public void shouldAggregateOnlyWhenReadyForAggregationIsSet() {
    final String outletId1 = "outlet1";

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
      .shopperLocation(US)
      .cardIssuingCountry(IN)
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();
    TransactionData transactionNotReady = TransactionData.builder()
      .outletId(outletId1)
      .orderReference(randomUUID().toString())
      .channel(ECOM)
      .currency("INR")
      .shopperType(RETURNING)
      .timestamp(timestamp.plus(31, MINUTES))
      .build();

    insertTransactionData(transactionData1);
    insertTransactionData(transactionData2);
    insertTransactionData(transactionNotReady);

    sparkDataAggregator.process();

    List<AggregatedData> aggregatedData = findAggregatedDataByOutletId(outletId1);
    assertNotNull(aggregatedData);
    assertEquals(2, aggregatedData.size());
    assertEquals(outletId1, aggregatedData.get(0).getOutletId());
    assertEquals(timestamp, Instant.parse(aggregatedData.get(0).getStartTime()));
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(0).getEndTime()));
    assertEquals(1, aggregatedData.get(0).getPaymentTransactions().size());

    PaymentTransaction paymentTransaction = aggregatedData.get(0).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    AggregationItem shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(IN.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());

    assertEquals(outletId1, aggregatedData.get(1).getOutletId());
    assertEquals(timestamp.plus(30, MINUTES), Instant.parse(aggregatedData.get(1).getStartTime()));
    assertEquals(timestamp.plus(60, MINUTES), Instant.parse(aggregatedData.get(1).getEndTime()));
    assertEquals(1, aggregatedData.get(1).getPaymentTransactions().size());

    paymentTransaction = aggregatedData.get(1).getPaymentTransactions().get(0);
    assertEquals("INR", paymentTransaction.getCurrency());
    assertEquals(ECOM.name(), paymentTransaction.getChannel());

    shopperLocations = paymentTransaction.getShopperLocations().get(0);
    assertEquals(US.name(), shopperLocations.getName());
    assertEquals(new Long(1), shopperLocations.getCount());
    assertEquals(3, findAllTransactionData().size());
  }

  private List<AggregatedData> findAllAggregatedData() {
    return mongoOperations.findAll(AggregatedData.class, DataAggregator.TARGET_COLLECTION);
  }

  private List<TransactionData> findAllTransactionData() {
    return mongoOperations.findAll(TransactionData.class, DataAggregator.SOURCE_COLLECTION);
  }

  private List<AggregatedData> findAggregatedDataByOutletId(String outletId) {
    return mongoOperations.find(Query.query(where("outletId").is(outletId)), AggregatedData.class, DataAggregator.TARGET_COLLECTION);
  }

  private void insertTransactionData(TransactionData transactionData) {
    mongoOperations.insert(transactionData, DataAggregator.SOURCE_COLLECTION);
  }

  private void insertJobDetails(JobDetails jobDetails) {
    mongoOperations.insert(jobDetails, DataAggregator.JOB_DETAILS_COLLECTION);
  }

}
