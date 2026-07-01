package com.powerbank.rentalservice.exception;

import java.util.UUID;

public class RentalNotFoundException extends RuntimeException {

    private final UUID rentalId;

    public RentalNotFoundException(UUID rentalId) {
        super("Rental not found: " + rentalId);
        this.rentalId = rentalId;
    }

    public UUID getRentalId() {
        return rentalId;
    }
}