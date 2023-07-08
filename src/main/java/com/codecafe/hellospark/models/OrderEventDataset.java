package com.codecafe.hellospark.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEventDataset {

  private String partitionKey;
  private Instant timestamp;
  private Payload payload;

}
