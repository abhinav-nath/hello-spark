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
public class AggregatedData {

  private String outletId;
  private String startTime;
  private String endTime;
  private List<PaymentTransaction> paymentTransactions;

}
