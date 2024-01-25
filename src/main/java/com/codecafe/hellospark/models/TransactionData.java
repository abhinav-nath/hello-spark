package com.codecafe.hellospark.models;

import static com.codecafe.hellospark.models.FieldNames.CARD_ISSUING_COUNTRY;
import static com.codecafe.hellospark.models.FieldNames.CHANNEL;
import static com.codecafe.hellospark.models.FieldNames.CURRENCY;
import static com.codecafe.hellospark.models.FieldNames.ID;
import static com.codecafe.hellospark.models.FieldNames.OUTLET_REFERENCE;
import static com.codecafe.hellospark.models.FieldNames.SHOPPER_LOCATION;
import static com.codecafe.hellospark.models.FieldNames.SHOPPER_TYPE;
import static com.codecafe.hellospark.models.FieldNames.TIMESTAMP;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionData {

  @JsonProperty
  @Field(ID)
  @Id
  private String orderReference;

  @JsonProperty
  @Field(OUTLET_REFERENCE)
  private String outletId;

  @JsonProperty
  @Field(CURRENCY)
  private String currency;

  @JsonProperty
  @Field(TIMESTAMP)
  @Indexed
  private Instant timestamp;

  @JsonProperty
  @Field(CHANNEL)
  private Channel channel;

  @JsonProperty
  @Field(CARD_ISSUING_COUNTRY)
  private Country cardIssuingCountry;

  @JsonProperty
  @Field(SHOPPER_LOCATION)
  private Country shopperLocation;

  @JsonProperty
  @Field(SHOPPER_TYPE)
  private ShopperType shopperType;

}
