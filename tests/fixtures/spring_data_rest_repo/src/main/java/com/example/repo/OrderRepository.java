package com.example.repo;

import java.util.Date;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.data.rest.core.annotation.RestResource;

@RepositoryRestResource(collectionResourceRel = "order", path = "order")
public interface OrderRepository extends PagingAndSortingRepository<Order, Long> {

  @Query("SELECT max(o.updated) FROM Order o")
  Date lastUpdate();

  @RestResource(exported = false)
  @Query("SELECT o FROM Order o WHERE o.internalFlag = true")
  Order internalOnly();

}
