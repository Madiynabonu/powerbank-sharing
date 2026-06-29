package com.powerbank.rentalservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.powerbank.rentalservice.domain.Rental;
import com.powerbank.rentalservice.domain.RentalStatus;
import com.powerbank.rentalservice.messaging.event.AcquireLockResult;
import com.powerbank.rentalservice.messaging.event.PaymentResult;
import com.powerbank.rentalservice.repository.RentalRepository;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class RentalServiceFsmTest {

    @Mock
    private RentalRepository rentalRepository;
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private RentalService rentalService;

    private final UUID userId = UUID.randomUUID();
    private final UUID stationId = UUID.randomUUID();
    private final UUID cardId = UUID.randomUUID();
    private final UUID powerbankId = UUID.randomUUID();

    @BeforeEach
    void setup() {
        ReflectionTestUtils.setField(rentalService, "acquireLockTopic", "acquire-cabinet-lock-event");
        ReflectionTestUtils.setField(rentalService, "ejectTopic", "eject-powerbank-event");
        ReflectionTestUtils.setField(rentalService, "paymentRequestTopic", "payment-request");
        ReflectionTestUtils.setField(rentalService, "recurrentChargeAmount", new java.math.BigDecimal("1000"));
    }

    @Test
    void createRentalPublishesLockCommandAndTransitionsToStationLock() {
        when(rentalRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Rental rental = rentalService.create(userId, stationId, cardId);

        assertThat(rental.getStatus()).isEqualTo(RentalStatus.STATION_LOCK);
        verify(kafkaTemplate).send(anyString(), anyString(), any()); // lock command sent
    }

    @Test
    void successfulLockTransitionsToProcessingPaymentAndSendsPaymentRequest() {
        Rental rental = Rental.create(userId, stationId, cardId);
        rental.transitionTo(RentalStatus.STATION_LOCK);
        when(rentalRepository.findById(rental.getId())).thenReturn(Optional.of(rental));
        when(rentalRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        AcquireLockResult result = new AcquireLockResult(rental.getId(), stationId, powerbankId, 2, true, null);
        rentalService.processLockResult(result);

        assertThat(rental.getStatus()).isEqualTo(RentalStatus.PROCESSING_PAYMENT);
        assertThat(rental.getPowerbankId()).isEqualTo(powerbankId);
        verify(kafkaTemplate).send(anyString(), anyString(), any()); // payment-request sent
    }

    @Test
    void failedLockTransitionsToFailed() {
        Rental rental = Rental.create(userId, stationId, cardId);
        rental.transitionTo(RentalStatus.STATION_LOCK);
        when(rentalRepository.findById(rental.getId())).thenReturn(Optional.of(rental));
        when(rentalRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        AcquireLockResult result = new AcquireLockResult(rental.getId(), stationId, null, null, false, "NO_POWERBANK_AVAILABLE");
        rentalService.processLockResult(result);

        assertThat(rental.getStatus()).isEqualTo(RentalStatus.FAILED);
        assertThat(rental.getFailureReason()).isEqualTo("NO_POWERBANK_AVAILABLE");
    }

    @Test
    void successfulPaymentSendsEjectCommand() {
        Rental rental = Rental.create(userId, stationId, cardId);
        rental.transitionTo(RentalStatus.PROCESSING_PAYMENT);
        rental.setPowerbankId(powerbankId);
        rental.setSlotNumber(1);
        when(rentalRepository.findById(rental.getId())).thenReturn(Optional.of(rental));
        when(rentalRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PaymentResult result = new PaymentResult(rental.getId(), UUID.randomUUID(), "key", "CHARGE", "SUCCEEDED", null);
        rentalService.processPaymentResult(result);

        verify(kafkaTemplate).send(anyString(), anyString(), any()); // eject command
    }
}
