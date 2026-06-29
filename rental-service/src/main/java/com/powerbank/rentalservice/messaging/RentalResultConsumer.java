package com.powerbank.rentalservice.messaging;

import com.powerbank.rentalservice.messaging.event.AcquireLockResult;
import com.powerbank.rentalservice.messaging.event.EjectResult;
import com.powerbank.rentalservice.messaging.event.PaymentResult;
import com.powerbank.rentalservice.service.RentalService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Drives the FSM by routing Kafka results to the right RentalService method. */
@Component
@Slf4j
@RequiredArgsConstructor
public class RentalResultConsumer {

    private final RentalService rentalService;

    @KafkaListener(
            topics = "${app.kafka.topics.acquire-cabinet-lock-result}",
            containerFactory = "lockResultListenerFactory")
    public void onLockResult(AcquireLockResult result) {
        log.info("Lock result rental={} success={}", result.rentalId(), result.success());
        rentalService.processLockResult(result);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-result}",
            containerFactory = "paymentResultListenerFactory")
    public void onPaymentResult(PaymentResult result) {
        log.info("Payment result rental={} status={}", result.rentalId(), result.status());
        rentalService.processPaymentResult(result);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.eject-powerbank-result}",
            containerFactory = "ejectResultListenerFactory")
    public void onEjectResult(EjectResult result) {
        log.info("Eject result rental={} success={}", result.rentalId(), result.success());
        rentalService.processEjectResult(result);
    }
}
