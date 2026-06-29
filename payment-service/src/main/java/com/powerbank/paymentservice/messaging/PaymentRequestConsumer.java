package com.powerbank.paymentservice.messaging;

import com.powerbank.paymentservice.domain.Payment;
import com.powerbank.paymentservice.messaging.event.PaymentRequest;
import com.powerbank.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Entry point for charge/refund commands. The DB transaction commits inside
 * {@link PaymentService#process}, then we publish to Kafka. If the publish
 * fails the consumer offset is not committed and the record is redelivered;
 * because {@code process} is idempotent the retry returns the same Payment and
 * re-publishes — at-least-once with no double charge. (Outbox would close the
 * remaining gap — see DECISIONS.md.)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentRequestConsumer {

    private final PaymentService paymentService;
    private final PaymentEventPublisher publisher;

    @KafkaListener(
            topics = "${app.kafka.topics.payment-request}",
            containerFactory = "paymentRequestListenerFactory")
    public void onPaymentRequest(PaymentRequest request) {
        log.info("Received payment-request key={} type={} amount={} rental={}",
                request.idempotencyKey(), request.type(), request.amount(), request.rentalId());
        Payment payment = paymentService.process(request);
        publisher.publish(payment);
    }
}
