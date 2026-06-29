package com.powerbank.paymentservice.repository;

import com.powerbank.paymentservice.domain.Card;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CardRepository extends JpaRepository<Card, UUID> {

    /**
     * Locks the card row (SELECT ... FOR UPDATE) so concurrent charges against
     * the same card are serialized — this is what makes debit/credit atomic and
     * prevents lost updates / double-spend.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from Card c where c.id = :id")
    Optional<Card> findByIdForUpdate(@Param("id") UUID id);
}
