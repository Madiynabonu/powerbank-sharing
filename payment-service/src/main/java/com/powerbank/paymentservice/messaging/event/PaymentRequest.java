package com.powerbank.paymentservice.messaging.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Command consumed from the {@code payment-request} topic. Produced by
 * rental-service when it needs to charge (or refund) a card.
 *
 * @param idempotencyKey de-dup key; one Payment per key (see DECISIONS.md)
 * @param type           CHARGE or REFUND
 */
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
