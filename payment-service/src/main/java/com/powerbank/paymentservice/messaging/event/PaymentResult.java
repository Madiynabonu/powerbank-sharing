package com.powerbank.paymentservice.messaging.event;

import java.util.UUID;

/**
 * Reply published to the {@code payment-result} topic and consumed by
 * rental-service to drive its state machine.
 */
public record PaymentResult(
        UUID rentalId,
        UUID paymentId,
        String idempotencyKey,
        String type,
        String status,
        String failureReason
) {
}
