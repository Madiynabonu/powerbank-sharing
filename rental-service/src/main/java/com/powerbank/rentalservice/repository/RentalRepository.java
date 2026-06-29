package com.powerbank.rentalservice.repository;

import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.domain.RentalStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RentalRepository extends JpaRepository<Rental, UUID> {

    Page<Rental> findByUserIdOrderByCreatedAtDesc(UUID userId, Pageable pageable);

    List<Rental> findByStatus(RentalStatus status);
}
