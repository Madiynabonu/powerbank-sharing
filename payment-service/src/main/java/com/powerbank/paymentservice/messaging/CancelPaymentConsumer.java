package com.powerbank.paymentservice.messaging;

import com.powerbank.paymentservice.messaging.event.CancelPaymentCommand;
import com.powerbank.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class CancelPaymentConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.cancel-payment-command}",
            containerFactory = "cancelPaymentListenerFactory")
    public void onCancelPayment(CancelPaymentCommand command) {
        log.info("Received cancel-payment command rental={} key={}",
                command.rentalId(), command.originalIdempotencyKey());
        paymentService.cancel(command.originalIdempotencyKey());
    }
}
