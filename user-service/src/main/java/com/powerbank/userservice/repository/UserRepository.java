package com.powerbank.userservice.repository;

import com.powerbank.userservice.domain.User;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, UUID> {

    Optional<User> findByPhone(String phone);
}
