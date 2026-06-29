package com.powerbank.paymentservice.messaging.event;

import java.util.UUID;

/**
 * Command published by rental-service when a rental fails after payment already succeeded.
 * payment-service reverses the charge and marks the original payment as CANCELLED.
 */
public record CancelPaymentCommand(
        String originalIdempotencyKey,  // idempotency key of the charge to cancel
        UUID rentalId
) {}
