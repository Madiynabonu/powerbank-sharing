package com.powerbank.rentalservice.scheduler;

import com.powerbank.rentalservice.service.RentalService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.springframework.stereotype.Component;

/**
 * Quartz job that fires periodically and charges all active rentals.
 * Quartz is used instead of @Scheduled to get persistent, clustered scheduling
 * backed by the same PostgreSQL DB — so a restart does not skip a charge cycle.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class RecurrentPaymentJob implements Job {

    private final RentalService rentalService;

    @Override
    public void execute(JobExecutionContext context) {
        log.info("RecurrentPaymentJob fired");
        rentalService.chargeActiveRentals();
    }
}
