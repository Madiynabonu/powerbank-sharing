package com.powerbank.rentalservice.exception;

import com.powerbank.rentalservice.domain.RentalStatus;

public class InvalidRentalStateException extends RuntimeException {

    private final RentalStatus status;

    public InvalidRentalStateException(RentalStatus status) {
        super("Cannot finish rental in state " + status);
        this.status = status;
    }

    public RentalStatus getStatus() {
        return status;
    }
}