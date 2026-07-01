package com.powerbank.rentalservice.service;

import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.domain.RentalStatus;
import com.powerbank.rentalservice.exception.InvalidRentalStateException;
import com.powerbank.rentalservice.exception.RentalNotFoundException;
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
 * FSM orchestrator for the rental lifecycle:
 *
 *   WAITING → STATION_LOCK → PROCESSING_PAYMENT → IN_THE_LEASE → FINISHED
 *                                                              ↘ FAILED (any step)
 *
 * State transitions are driven by Kafka result events from station-service and payment-service.
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

    @Transactional
    public Rental create(UUID userId, UUID stationId, UUID cardId) {
        Rental rental = Rental.create(userId, stationId, cardId);
        rentalRepository.save(rental);
        log.info("Rental {} created for user={} station={}", rental.getId(), userId, stationId);

        AcquireLockCommand cmd = new AcquireLockCommand(
                rental.getId().toString(), rental.getId(), stationId);
        kafkaTemplate.send(acquireLockTopic, rental.getId().toString(), cmd);
        rental.transitionTo(RentalStatus.STATION_LOCK);
        rentalRepository.save(rental);

        return rental;
    }

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

        // idempotencyKey = "rental-start-{id}" ensures at-most-once charge even on Kafka redelivery
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
            if (rental.getStatus() == RentalStatus.FINISHED || rental.getStatus() == RentalStatus.FAILED) {
                log.info("Late payment failure ignored — rental {} already in terminal state {}",
                        rental.getId(), rental.getStatus());
                return;
            }
            rental.fail(result.failureReason());
            rentalRepository.save(rental);
            log.warn("Rental {} FAILED at payment: {}", rental.getId(), result.failureReason());
            return;
        }

        if ("SUCCEEDED".equals(result.status())
                && rental.getStatus() == RentalStatus.PROCESSING_PAYMENT) {
            log.info("Rental {} payment ok -> ejecting powerbank", rental.getId());
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
            // payment already SUCCEEDED — issue a cancellation so the card balance is restored
            kafkaTemplate.send(cancelPaymentTopic, rental.getId().toString(),
                    new CancelPaymentCommand("rental-start-" + rental.getId(), rental.getId()));
            return;
        }

        rental.transitionTo(RentalStatus.IN_THE_LEASE);
        rental.setStartedAt(OffsetDateTime.now());
        rentalRepository.save(rental);
        log.info("Rental {} IN_THE_LEASE powerbank={}", rental.getId(), result.powerbankId());
    }

    @Transactional
    public Rental finish(UUID rentalId, UUID returnStationId) {
        Rental rental = rentalRepository.findById(rentalId)
                .orElseThrow(() -> new RentalNotFoundException(rentalId));

        if (rental.getStatus() != RentalStatus.IN_THE_LEASE) {
            throw new InvalidRentalStateException(rental.getStatus());
        }

        rental.setReturnStationId(returnStationId);
        rental.transitionTo(RentalStatus.FINISHED);
        rental.setFinishedAt(OffsetDateTime.now());
        rentalRepository.save(rental);

        kafkaTemplate.send(returnPowerbankTopic, rental.getId().toString(),
                new ReturnPowerbankCommand(rental.getId(), rental.getPowerbankId(), returnStationId));

        log.info("Rental {} FINISHED, powerbank {} returning to station {}",
                rentalId, rental.getPowerbankId(), returnStationId);
        return rental;
    }

    /**
     * Called by the Quartz scheduler. Idempotency key includes timestamp so each interval
     * generates a distinct charge — intentional, not a bug.
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

    public Optional<Rental> findById(UUID id) {
        return rentalRepository.findById(id);
    }

    public Page<Rental> findHistory(UUID userId, int page, int pageSize) {
        int size = (pageSize > 0 && pageSize <= 100) ? pageSize : 20;
        return rentalRepository.findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(page, size));
    }

    private Rental findByIdOrLog(UUID rentalId) {
        return rentalRepository.findById(rentalId)
                .orElseGet(() -> {
                    log.warn("Rental not found: {}", rentalId);
                    return null;
                });
    }
}
