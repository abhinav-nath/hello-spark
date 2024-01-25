package com.codecafe.hellospark.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggregatedDataV2 {

  private String outletId;
  private String startTime;
  private String endTime;
  private List<AggregationItem> filters;

}
