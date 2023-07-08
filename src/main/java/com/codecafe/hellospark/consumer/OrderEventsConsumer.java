package com.codecafe.hellospark.consumer;

import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

import com.codecafe.hellospark.models.OrderEvent;
import com.codecafe.hellospark.models.Payload;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
@Service
public class OrderEventsConsumer {

  private final MongoTemplate mongoTemplate;
  private final ObjectMapper objectMapper;
  private final ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;
  @Value("${app.kafka.bootstrap-servers}")
  private String kafkaBootstrapServers;
  @Value("${app.kafka.consumer-group}")
  private String consumerGroup;

  @Autowired
  public OrderEventsConsumer(MongoTemplate mongoTemplate, ObjectMapper objectMapper, ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory) {
    this.mongoTemplate = mongoTemplate;
    this.objectMapper = objectMapper;
    this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
  }

  @PostConstruct
  public void setupKafkaListener() {
    ConsumerFactory<? super String, ? super String> consumerFactory = kafkaListenerContainerFactory.getConsumerFactory();
    Map<String, Object> props = new HashMap<>(consumerFactory.getConfigurationProperties());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

    kafkaListenerContainerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
  }

  @KafkaListener(topics = "${app.kafka.topic}")
  public void consumeOrderEvent(ConsumerRecord<String, String> record) {
    String json = record.value();
    try {
      Payload payload = objectMapper.readValue(json, Payload.class);
      log.info("Received order event: {}", payload);
      OrderEvent orderEvent = OrderEvent.builder()
        .partitionKey(String.valueOf(UUID.randomUUID()))
        .payload(payload)
        .timestamp(Instant.now())
        .build();
      mongoTemplate.save(orderEvent, "orderEvents");
    } catch (IOException e) {
      log.error("Error parsing order event JSON: {}", json, e);
    }
  }
}
