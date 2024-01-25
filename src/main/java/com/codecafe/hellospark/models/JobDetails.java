package com.codecafe.hellospark.models;

import java.time.Instant;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class JobDetails {

  private String id;
  private Instant endTime;

  public JobDetails() {
  }

  public JobDetails(Instant endTime) {
    this.endTime = endTime;
  }

  public JobDetails(String id, Instant endTime) {
    this.id = id;
    this.endTime = endTime;
  }

}
