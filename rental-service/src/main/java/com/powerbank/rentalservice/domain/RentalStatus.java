package com.powerbank.rentalservice.domain;

/**
 * State machine states visible in the sequence diagram from the task spec:
 * WAITING → STATION_LOCK → PROCESSING_PAYMENT → IN_THE_LEASE → FINISHED
 * Any transition can also go → FAILED.
 */
public enum RentalStatus {
    WAITING,             // created, waiting to lock station
    STATION_LOCK,        // lock command sent, waiting for station response
    PROCESSING_PAYMENT,  // station locked, payment command sent
    IN_THE_LEASE,        // payment succeeded, powerbank ejected, active rental
    FINISHED,            // returned
    FAILED               // any step failed
}
