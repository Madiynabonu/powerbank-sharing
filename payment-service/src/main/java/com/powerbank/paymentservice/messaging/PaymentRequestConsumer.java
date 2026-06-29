package com.powerbank.paymentservice.messaging;

import com.powerbank.paymentservice.domain.Payment;
import com.powerbank.paymentservice.messaging.event.PaymentRequest;
import com.powerbank.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Processes charge/refund commands. PaymentService.process() is idempotent, so
 * Kafka redelivery (offset not committed on DB failure) re-publishes the same
 * result — at-least-once with no double charge. The remaining DB-commit/Kafka-publish
 * gap is acknowledged in DECISIONS.md (Outbox pattern).
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
