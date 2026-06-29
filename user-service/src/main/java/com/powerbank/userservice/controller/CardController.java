package com.powerbank.userservice.controller;

import com.powerbank.userservice.messaging.event.CardCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Card binding API — lets an authenticated user register a card in payment-service
 * via the card-command Kafka topic.
 */
@RestController
@RequestMapping("/v1/cards")
@RequiredArgsConstructor
@Slf4j
public class CardController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.card-command}")
    private String cardCommandTopic;

    public record BindCardRequest(
            @NotNull UUID cardId,
            @NotNull @Size(min = 16, max = 19) String maskedPan,
            BigDecimal initialBalance,
            String currency
    ) {}

    public record BindCardResponse(UUID cardId, String status) {}

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public BindCardResponse bindCard(
            @Valid @RequestBody BindCardRequest request,
            @AuthenticationPrincipal Jwt jwt) {

        UUID userId = UUID.fromString(jwt.getSubject());
        CardCommand command = new CardCommand(
                "BIND",
                request.cardId(),
                userId,
                request.maskedPan(),
                request.initialBalance() == null ? BigDecimal.ZERO : request.initialBalance(),
                request.currency() == null ? "UZS" : request.currency());

        kafkaTemplate.send(cardCommandTopic, request.cardId().toString(), command);
        log.info("Sent BIND card command cardId={} userId={}", request.cardId(), userId);

        return new BindCardResponse(request.cardId(), "BINDING");
    }
}
