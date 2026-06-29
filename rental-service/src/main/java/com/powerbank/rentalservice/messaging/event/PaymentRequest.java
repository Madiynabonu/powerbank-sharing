package com.powerbank.rentalservice.messaging.event;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRequest(
        String idempotencyKey,
        UUID rentalId,
        UUID cardId,
        UUID userId,
        BigDecimal amount,
        String currency,
        String type
) {
}
