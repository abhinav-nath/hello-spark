package com.codecafe.hellospark.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@Configuration
@EnableMongoRepositories(basePackages = "com.spikes.analytics.repository")
public class MongoDBConfig extends AbstractMongoClientConfiguration {

  @Value("${spring.data.mongodb.database}")
  private String databaseName;

  @Value("${spring.data.mongodb.uri}")
  private String mongoUri;

  @Override
  protected String getDatabaseName() {
    return databaseName;
  }

  @Override
  @Bean
  public MongoClient mongoClient() {
    ConnectionString connectionString = new ConnectionString(mongoUri);
    MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
      .applyConnectionString(connectionString)
      .build();
    return MongoClients.create(mongoClientSettings);
  }
}
