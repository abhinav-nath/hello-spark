package com.codecafe.hellospark.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Repository;

import com.codecafe.hellospark.models.AggregatedData;
import com.codecafe.hellospark.models.JobDetails;
import com.google.common.annotations.VisibleForTesting;

@Repository
public class JobDetailsRepository {

  private final MongoOperations mongoOperations;

  @Autowired
  JobDetailsRepository(MongoOperations mongoOperations) {
    this.mongoOperations = mongoOperations;
  }

  public void create(JobDetails jobDetails) {
    mongoOperations.insert(jobDetails);
  }

  public List<JobDetails> findAll() {
    return mongoOperations.findAll(JobDetails.class);
  }

  @VisibleForTesting
  public void removeAll() {
    mongoOperations.remove(AggregatedData.class);
  }

}
