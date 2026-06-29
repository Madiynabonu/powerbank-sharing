package com.powerbank.stationservice.domain;

public enum PowerBankStatus {
    DOCKED,     // in station, available for rent
    EJECTING,   // being ejected (async in-flight)
    IN_RENTAL   // taken by a user
}
