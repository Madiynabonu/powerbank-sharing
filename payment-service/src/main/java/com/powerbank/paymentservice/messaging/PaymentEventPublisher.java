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
 * Publishes the targeted reply ({@code payment-result}, keyed by rentalId so a
 * rental's events stay ordered) and the audit event ({@code payment-events},
 * keyed by cardId so a card's history stays ordered). See DECISIONS.md for why
 * the key choice matters for ordering.
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
        // key = rentalId: every result for one rental lands on the same partition, in order
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
        // key = cardId: a card's payment history stays ordered on one partition
        kafkaTemplate.send(paymentEventsTopic, payment.getCardId().toString(), event);

        log.debug("Published payment-result + payment-events for payment {}", payment.getId());
    }
}
