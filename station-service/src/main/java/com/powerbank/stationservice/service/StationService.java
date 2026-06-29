package com.powerbank.stationservice.service;

import com.powerbank.stationservice.domain.PowerBank;
import com.powerbank.stationservice.domain.PowerBankStatus;
import com.powerbank.stationservice.domain.Station;
import com.powerbank.stationservice.domain.StationStatus;
import com.powerbank.stationservice.messaging.event.AcquireLockCommand;
import com.powerbank.stationservice.messaging.event.AcquireLockResult;
import com.powerbank.stationservice.messaging.event.EjectCommand;
import com.powerbank.stationservice.messaging.event.EjectResult;
import com.powerbank.stationservice.repository.PowerBankRepository;
import com.powerbank.stationservice.repository.StationRepository;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Business logic for station operations. All IoT calls are asynchronous —
 * the caller publishes a command and receives a result event; this service
 * simulates the hardware delay with a Thread.sleep inside a virtual thread
 * (or a scheduled task) rather than blocking the Kafka consumer.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class StationService {

    private final StationRepository stationRepository;
    private final PowerBankRepository powerBankRepository;

    @Value("${app.station.lock-sim-delay-ms}")
    private long lockSimDelayMs;

    @Value("${app.station.eject-sim-delay-ms}")
    private long ejectSimDelayMs;

    @Value("${app.station.failure-rate}")
    private double failureRate;

    // ---------------------------------------------------------------------- gRPC read side

    public Optional<Station> findById(String id) {
        try {
            return stationRepository.findById(java.util.UUID.fromString(id));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public List<Station> findNearby(double lat, double lng, int radiusM, int limit) {
        int pageSize = (limit > 0 && limit <= 100) ? limit : 20;
        return stationRepository.findNearby(lat, lng, radiusM, StationStatus.ONLINE,
                PageRequest.of(0, pageSize));
    }

    // ---------------------------------------------------------------------- lock simulation

    /**
     * Simulates the station locking a slot so no other rental can claim the same
     * powerbank. If successful, the selected powerbank transitions to EJECTING and
     * the result carries its ID + slot number.
     *
     * This runs in the Kafka listener thread; it calls Thread.sleep to model the
     * async IoT delay (real hardware would respond asynchronously).
     */
    @Transactional
    public AcquireLockResult simulateLock(AcquireLockCommand cmd) {
        log.info("Simulating cabinet lock for rental={} station={}", cmd.rentalId(), cmd.stationId());
        sleep(lockSimDelayMs);

        if (shouldFail()) {
            log.warn("Simulated lock FAILURE for rental={}", cmd.rentalId());
            return new AcquireLockResult(cmd.rentalId(), cmd.stationId(), null, null, false, "STATION_LOCK_FAILED");
        }

        Station station = stationRepository.findById(cmd.stationId()).orElse(null);
        if (station == null || station.getStatus() != StationStatus.ONLINE) {
            return new AcquireLockResult(cmd.rentalId(), cmd.stationId(), null, null, false, "STATION_NOT_FOUND");
        }

        List<PowerBank> available = powerBankRepository.findDockedByStationForUpdate(cmd.stationId());
        if (available.isEmpty()) {
            return new AcquireLockResult(cmd.rentalId(), cmd.stationId(), null, null, false, "NO_POWERBANK_AVAILABLE");
        }

        PowerBank pb = available.get(0);
        pb.setStatus(PowerBankStatus.EJECTING);
        powerBankRepository.save(pb);
        log.info("Locked powerbank={} slot={} for rental={}", pb.getId(), pb.getSlotNumber(), cmd.rentalId());

        return new AcquireLockResult(cmd.rentalId(), cmd.stationId(), pb.getId(), pb.getSlotNumber(), true, null);
    }

    // ---------------------------------------------------------------------- eject simulation

    @Transactional
    public EjectResult simulateEject(EjectCommand cmd) {
        log.info("Simulating eject powerbank={} rental={}", cmd.powerbankId(), cmd.rentalId());
        sleep(ejectSimDelayMs);

        if (shouldFail()) {
            log.warn("Simulated eject FAILURE for rental={}", cmd.rentalId());
            // roll back the powerbank status to DOCKED so it can be retried
            powerBankRepository.findById(cmd.powerbankId()).ifPresent(pb -> {
                pb.setStatus(PowerBankStatus.DOCKED);
                powerBankRepository.save(pb);
            });
            return new EjectResult(cmd.rentalId(), cmd.stationId(), cmd.powerbankId(),
                    cmd.slotNumber(), false, "EJECT_FAILED");
        }

        PowerBank pb = powerBankRepository.findByIdAndStatus(cmd.powerbankId(), PowerBankStatus.EJECTING)
                .orElse(null);
        if (pb == null) {
            return new EjectResult(cmd.rentalId(), cmd.stationId(), cmd.powerbankId(),
                    cmd.slotNumber(), false, "POWERBANK_NOT_IN_EJECTING_STATE");
        }

        pb.setStatus(PowerBankStatus.IN_RENTAL);
        pb.setStation(null);
        pb.setSlotNumber(null);
        powerBankRepository.save(pb);
        log.info("Ejected powerbank={} rental={}", pb.getId(), cmd.rentalId());

        return new EjectResult(cmd.rentalId(), cmd.stationId(), pb.getId(), cmd.slotNumber(), true, null);
    }

    // ---------------------------------------------------------------------- return simulation

    @Transactional
    public void returnPowerBank(java.util.UUID powerbankId, java.util.UUID stationId) {
        Station station = stationRepository.findById(stationId).orElseThrow(
                () -> new IllegalArgumentException("Station not found: " + stationId));
        PowerBank pb = powerBankRepository.findById(powerbankId).orElseThrow(
                () -> new IllegalArgumentException("PowerBank not found: " + powerbankId));

        // find a free slot
        long occupiedSlots = station.getPowerBanks().stream()
                .filter(p -> p.getSlotNumber() != null).count();
        int freeSlot = (int) (occupiedSlots + 1);

        pb.setStation(station);
        pb.setSlotNumber(freeSlot);
        pb.setStatus(PowerBankStatus.DOCKED);
        powerBankRepository.save(pb);
        log.info("Returned powerbank={} to station={} slot={}", powerbankId, stationId, freeSlot);
    }

    // ---------------------------------------------------------------------- helpers

    private boolean shouldFail() {
        return ThreadLocalRandom.current().nextDouble() < failureRate;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
