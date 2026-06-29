package com.powerbank.paymentservice.messaging;

import com.powerbank.paymentservice.domain.Payment;
import com.powerbank.paymentservice.messaging.event.PaymentEvent;
import com.powerbank.paymentservice.messaging.event.PaymentResult;
import java.time.OffsetDateTime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes two events per payment:
 * - payment-result (keyed by rentalId): targeted reply to rental-service; rentalId key keeps
 *   all results for one rental on the same partition, in order.
 * - payment-events (keyed by cardId): append-only audit stream; cardId key keeps a card's
 *   history ordered on one partition.
 */
@Component
@Slf4j
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentResultTopic;
    private final String paymentEventsTopic;

    public PaymentEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.payment-result}") String paymentResultTopic,
            @Value("${app.kafka.topics.payment-events}") String paymentEventsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentResultTopic = paymentResultTopic;
        this.paymentEventsTopic = paymentEventsTopic;
    }

    public void publish(Payment payment) {
        PaymentResult result = new PaymentResult(
                payment.getRentalId(),
                payment.getId(),
                payment.getIdempotencyKey(),
                payment.getType().name(),
                payment.getStatus().name(),
                payment.getFailureReason());
        String resultKey = payment.getRentalId() != null
                ? payment.getRentalId().toString()
                : payment.getId().toString();
        kafkaTemplate.send(paymentResultTopic, resultKey, result);

        PaymentEvent event = new PaymentEvent(
                payment.getId(),
                payment.getCardId(),
                payment.getUserId(),
                payment.getRentalId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getType().name(),
                payment.getStatus().name(),
                OffsetDateTime.now());
        kafkaTemplate.send(paymentEventsTopic, payment.getCardId().toString(), event);

        log.debug("Published payment-result + payment-events for payment {}", payment.getId());
    }
}
