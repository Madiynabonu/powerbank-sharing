package com.powerbank.rentalservice.service;

import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.domain.RentalStatus;
import com.powerbank.rentalservice.messaging.event.AcquireLockCommand;
import com.powerbank.rentalservice.messaging.event.AcquireLockResult;
import com.powerbank.rentalservice.messaging.event.CancelPaymentCommand;
import com.powerbank.rentalservice.messaging.event.EjectCommand;
import com.powerbank.rentalservice.messaging.event.EjectResult;
import com.powerbank.rentalservice.messaging.event.PaymentRequest;
import com.powerbank.rentalservice.messaging.event.PaymentResult;
import com.powerbank.rentalservice.messaging.event.ReturnPowerbankCommand;
import com.powerbank.rentalservice.repository.RentalRepository;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Finite State Machine for the rental lifecycle (see sequence diagram in DECISIONS.md):
 *
 *   WAITING
 *     → [publish acquire-cabinet-lock-event] → STATION_LOCK
 *   STATION_LOCK
 *     → [lock success] → [publish payment-request] → PROCESSING_PAYMENT
 *     → [lock failure] → FAILED
 *   PROCESSING_PAYMENT
 *     → [payment SUCCEEDED] → [publish eject-powerbank-event] → PROCESSING_PAYMENT (still, waiting for eject)
 *       ... in practice eject comes back and we → IN_THE_LEASE
 *     → [payment FAILED] → FAILED
 *   IN_THE_LEASE
 *     → [finish] → FINISHED
 *
 * Note: eject result comes on a different topic, handled in processEjectResult().
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RentalService {

    private final RentalRepository rentalRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.acquire-cabinet-lock}")
    private String acquireLockTopic;

    @Value("${app.kafka.topics.eject-powerbank}")
    private String ejectTopic;

    @Value("${app.kafka.topics.payment-request}")
    private String paymentRequestTopic;

    @Value("${app.kafka.topics.cancel-payment-command}")
    private String cancelPaymentTopic;

    @Value("${app.kafka.topics.return-powerbank-command}")
    private String returnPowerbankTopic;

    @Value("${app.rental.recurrent-charge-amount}")
    private BigDecimal recurrentChargeAmount;

    // ---------------------------------------------------------------- create

    @Transactional
    public Rental create(UUID userId, UUID stationId, UUID cardId) {
        Rental rental = Rental.create(userId, stationId, cardId);
        rentalRepository.save(rental);
        log.info("Rental {} created for user={} station={}", rental.getId(), userId, stationId);

        // Step 1: ask station to lock a cabinet
        AcquireLockCommand cmd = new AcquireLockCommand(
                rental.getId().toString(), rental.getId(), stationId);
        kafkaTemplate.send(acquireLockTopic, rental.getId().toString(), cmd);
        rental.transitionTo(RentalStatus.STATION_LOCK);
        rentalRepository.save(rental);

        return rental;
    }

    // ---------------------------------------------------------------- FSM transitions

    @Transactional
    public void processLockResult(AcquireLockResult result) {
        Rental rental = findByIdOrLog(result.rentalId());
        if (rental == null) return;
        if (rental.getStatus() != RentalStatus.STATION_LOCK) {
            log.warn("Unexpected lock result for rental {} in state {}", rental.getId(), rental.getStatus());
            return;
        }

        if (!result.success()) {
            rental.fail(result.errorReason());
            rentalRepository.save(rental);
            log.warn("Rental {} FAILED at lock: {}", rental.getId(), result.errorReason());
            return;
        }

        rental.setPowerbankId(result.powerbankId());
        rental.setSlotNumber(result.slotNumber());
        rental.transitionTo(RentalStatus.PROCESSING_PAYMENT);
        rentalRepository.save(rental);
        log.info("Rental {} lock ok powerbank={} -> sending payment", rental.getId(), result.powerbankId());

        // Step 2: request payment (idempotencyKey ensures at-most-once charge)
        PaymentRequest pr = new PaymentRequest(
                "rental-start-" + rental.getId(),
                rental.getId(),
                rental.getCardId(),
                rental.getUserId(),
                recurrentChargeAmount,
                "UZS",
                "CHARGE");
        kafkaTemplate.send(paymentRequestTopic, rental.getId().toString(), pr);
    }

    @Transactional
    public void processPaymentResult(PaymentResult result) {
        Rental rental = findByIdOrLog(result.rentalId());
        if (rental == null) return;

        if ("FAILED".equals(result.status())) {
            rental.fail(result.failureReason());
            rentalRepository.save(rental);
            log.warn("Rental {} FAILED at payment: {}", rental.getId(), result.failureReason());
            return;
        }

        if ("SUCCEEDED".equals(result.status())
                && rental.getStatus() == RentalStatus.PROCESSING_PAYMENT) {
            log.info("Rental {} payment ok -> ejecting powerbank", rental.getId());
            // Step 3: eject the powerbank
            EjectCommand eject = new EjectCommand(
                    rental.getId().toString(),
                    rental.getId(),
                    rental.getStationId(),
                    rental.getPowerbankId(),
                    rental.getSlotNumber());
            kafkaTemplate.send(ejectTopic, rental.getId().toString(), eject);
            // state stays PROCESSING_PAYMENT until eject result arrives
            rentalRepository.save(rental);
        }
    }

    @Transactional
    public void processEjectResult(EjectResult result) {
        Rental rental = findByIdOrLog(result.rentalId());
        if (rental == null) return;

        if (!result.success()) {
            rental.fail(result.errorReason());
            rentalRepository.save(rental);
            log.warn("Rental {} FAILED at eject: {}", rental.getId(), result.errorReason());
            // Payment already SUCCEEDED at this point — issue a cancellation refund
            CancelPaymentCommand cancel = new CancelPaymentCommand(
                    "rental-start-" + rental.getId(), rental.getId());
            kafkaTemplate.send(cancelPaymentTopic, rental.getId().toString(), cancel);
            return;
        }

        rental.transitionTo(RentalStatus.IN_THE_LEASE);
        rental.setStartedAt(OffsetDateTime.now());
        rentalRepository.save(rental);
        log.info("Rental {} IN_THE_LEASE powerbank={}", rental.getId(), result.powerbankId());
    }

    // ---------------------------------------------------------------- finish

    @Transactional
    public Rental finish(UUID rentalId, UUID returnStationId) {
        Rental rental = rentalRepository.findById(rentalId)
                .orElseThrow(() -> new IllegalArgumentException("Rental not found: " + rentalId));

        if (rental.getStatus() != RentalStatus.IN_THE_LEASE) {
            throw new IllegalStateException("Cannot finish rental in state " + rental.getStatus());
        }

        rental.setReturnStationId(returnStationId);
        rental.transitionTo(RentalStatus.FINISHED);
        rental.setFinishedAt(OffsetDateTime.now());
        rentalRepository.save(rental);

        // Notify station-service to dock the powerbank back into the return station
        ReturnPowerbankCommand returnCmd = new ReturnPowerbankCommand(
                rental.getId(), rental.getPowerbankId(), returnStationId);
        kafkaTemplate.send(returnPowerbankTopic, rental.getId().toString(), returnCmd);

        log.info("Rental {} FINISHED, powerbank {} returning to station {}",
                rentalId, rental.getPowerbankId(), returnStationId);
        return rental;
    }

    // ---------------------------------------------------------------- recurrent charge

    /**
     * Called by the Quartz scheduler periodically. Charges the card for each
     * active rental.
     */
    @Transactional
    public void chargeActiveRentals() {
        List<Rental> active = rentalRepository.findByStatus(RentalStatus.IN_THE_LEASE);
        for (Rental rental : active) {
            PaymentRequest pr = new PaymentRequest(
                    "recurrent-" + rental.getId() + "-" + System.currentTimeMillis(),
                    rental.getId(),
                    rental.getCardId(),
                    rental.getUserId(),
                    recurrentChargeAmount,
                    "UZS",
                    "CHARGE");
            kafkaTemplate.send(paymentRequestTopic, rental.getId().toString(), pr);
            log.debug("Sent recurrent charge for rental {}", rental.getId());
        }
    }

    // ---------------------------------------------------------------- queries

    public Optional<Rental> findById(UUID id) {
        return rentalRepository.findById(id);
    }

    public Page<Rental> findHistory(UUID userId, int page, int pageSize) {
        int size = (pageSize > 0 && pageSize <= 100) ? pageSize : 20;
        return rentalRepository.findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(page, size));
    }

    // ---------------------------------------------------------------- helpers

    private Rental findByIdOrLog(UUID rentalId) {
        return rentalRepository.findById(rentalId)
                .orElseGet(() -> {
                    log.warn("Rental not found: {}", rentalId);
                    return null;
                });
    }
}
