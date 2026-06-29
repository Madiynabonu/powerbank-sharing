package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record CancelPaymentCommand(
        String originalIdempotencyKey,
        UUID rentalId
) {}
