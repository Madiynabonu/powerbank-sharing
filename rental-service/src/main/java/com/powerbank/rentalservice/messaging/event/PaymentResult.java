package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record PaymentResult(
        UUID rentalId,
        UUID paymentId,
        String idempotencyKey,
        String type,
        String status,
        String failureReason
) {
}
