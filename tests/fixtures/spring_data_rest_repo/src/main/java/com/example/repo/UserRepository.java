package com.example.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Integer> {

  @Query("SELECT u FROM User u WHERE LOWER(u.username) = LOWER(:username)")
  User findByUsernameCaseInsensitive(@Param("username") String username);

}
