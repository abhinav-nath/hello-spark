package com.codecafe.hellospark.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "aggregatedData")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregatedData {

    private static final String PARTITION_KEY_FIELD = "partitionKey";

    @Id
    @Field("_id")
    private String id;

    @Field(PARTITION_KEY_FIELD)
    @Indexed(name = "partitionKey_1")
    private String partitionKey;

    @Field
    private String startTime;

    @Field
    private String endTime;

    @Field
    private List<AggregationItem> countries;

    @Field
    private List<AggregationItem> paymentChannels;

    @Field
    private List<AggregationItem> cardIssuerCountries;

}