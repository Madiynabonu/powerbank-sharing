package com.powerbank.paymentservice.messaging.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Domain event emitted to the {@code payment-events} topic on every payment
 * status transition (required by the task, section 2.3). This is the audit /
 * integration stream; {@link PaymentResult} is the targeted reply to rental.
 */
public record PaymentEvent(
        UUID paymentId,
        UUID cardId,
        UUID userId,
        UUID rentalId,
        BigDecimal amount,
        String currency,
        String type,
        String status,
        OffsetDateTime occurredAt
) {
}
