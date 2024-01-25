package com.codecafe.hellospark.spark;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import lombok.Getter;

@Getter
public class TimeSlidingWindow {

  private final int timeToAdd;
  private final ChronoUnit timeUnit;
  private Instant startTime;
  private Instant endTime;

  TimeSlidingWindow(Instant startTime, int timeToAdd, ChronoUnit timeUnit) {
    this.timeToAdd = timeToAdd;
    this.timeUnit = timeUnit;
    this.startTime = startTime;
    this.endTime = this.startTime.plus(this.timeToAdd, this.timeUnit);
  }

  public void slide() {
    startTime = endTime;
    endTime = startTime.plus(timeToAdd, timeUnit);
  }

}
