package com.powerbank.stationservice.repository;

import com.powerbank.stationservice.domain.PowerBank;
import com.powerbank.stationservice.domain.PowerBankStatus;
import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PowerBankRepository extends JpaRepository<PowerBank, UUID> {

    /**
     * Lock the first available (DOCKED) powerbank in a station to avoid two
     * concurrent rentals picking the same slot.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select p from PowerBank p
            where p.station.id = :stationId
              and p.status = 'DOCKED'
            order by p.slotNumber asc
            """)
    List<PowerBank> findDockedByStationForUpdate(@Param("stationId") UUID stationId);

    Optional<PowerBank> findByIdAndStatus(UUID id, PowerBankStatus status);
}
