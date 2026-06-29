package com.powerbank.paymentservice.messaging;

import com.powerbank.paymentservice.messaging.event.CardCommand;
import com.powerbank.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Handles card-binding commands (Kafka-only integration with this service). */
@Component
@Slf4j
@RequiredArgsConstructor
public class CardCommandConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.card-command}",
            containerFactory = "cardCommandListenerFactory")
    public void onCardCommand(CardCommand command) {
        log.info("Received card-command type={} card={}", command.commandType(), command.cardId());
        if ("BIND".equalsIgnoreCase(command.commandType())) {
            paymentService.bindCard(command.cardId(), command.userId(), command.maskedPan(),
                    command.initialBalance(), command.currency());
        } else {
            log.warn("Unknown card-command type: {}", command.commandType());
        }
    }
}
