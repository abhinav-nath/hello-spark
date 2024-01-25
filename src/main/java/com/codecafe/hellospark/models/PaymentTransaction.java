package com.codecafe.hellospark.models;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentTransaction {

  private String currency;
  private String channel;
  private List<AggregationItem> shopperLocations;
  private List<AggregationItem> cardIssuingCountries;
  private List<AggregationItem> shopperTypes;

}
