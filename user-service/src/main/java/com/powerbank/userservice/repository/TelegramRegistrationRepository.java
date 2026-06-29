package com.powerbank.userservice.repository;

import com.powerbank.userservice.domain.TelegramRegistration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TelegramRegistrationRepository extends JpaRepository<TelegramRegistration, UUID> {
    Optional<TelegramRegistration> findByPhone(String phone);
}
