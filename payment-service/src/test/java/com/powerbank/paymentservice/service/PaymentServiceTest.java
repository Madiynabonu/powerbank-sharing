package com.powerbank.paymentservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.powerbank.paymentservice.domain.Card;
import com.powerbank.paymentservice.domain.Payment;
import com.powerbank.paymentservice.domain.PaymentStatus;
import com.powerbank.paymentservice.messaging.event.PaymentRequest;
import com.powerbank.paymentservice.repository.CardRepository;
import com.powerbank.paymentservice.repository.PaymentRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;
    @Mock
    private CardRepository cardRepository;

    @InjectMocks
    private PaymentService paymentService;

    private final UUID cardId = UUID.randomUUID();
    private final UUID rentalId = UUID.randomUUID();

    private PaymentRequest charge(String key, BigDecimal amount) {
        return new PaymentRequest(key, rentalId, cardId, UUID.randomUUID(), amount, "UZS", "CHARGE");
    }

    private Card card(BigDecimal balance) {
        return new Card(cardId, UUID.randomUUID(), "8600 **** **** 0000", balance, "UZS");
    }

    @Test
    void chargeSucceedsAndDebitsCard() {
        Card card = card(new BigDecimal("1000.00"));
        when(paymentRepository.findByIdempotencyKey("k1")).thenReturn(Optional.empty());
        when(cardRepository.findByIdForUpdate(cardId)).thenReturn(Optional.of(card));
        when(paymentRepository.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

        Payment result = paymentService.process(charge("k1", new BigDecimal("300.00")));

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(card.getBalance()).isEqualByComparingTo("700.00");
    }

    @Test
    void chargeFailsWhenInsufficientFundsAndDoesNotTouchBalance() {
        Card card = card(new BigDecimal("100.00"));
        when(paymentRepository.findByIdempotencyKey("k2")).thenReturn(Optional.empty());
        when(cardRepository.findByIdForUpdate(cardId)).thenReturn(Optional.of(card));
        when(paymentRepository.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

        Payment result = paymentService.process(charge("k2", new BigDecimal("300.00")));

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(result.getFailureReason()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(card.getBalance()).isEqualByComparingTo("100.00");
    }

    @Test
    void duplicateKeyReturnsExistingPaymentWithoutChargingAgain() {
        Payment existing = new Payment();
        existing.setId(UUID.randomUUID());
        existing.setIdempotencyKey("dup");
        existing.setAmount(new BigDecimal("300.00"));
        existing.setStatus(PaymentStatus.SUCCEEDED);
        when(paymentRepository.findByIdempotencyKey("dup")).thenReturn(Optional.of(existing));

        Payment result = paymentService.process(charge("dup", new BigDecimal("300.00")));

        assertThat(result).isSameAs(existing);
        // critical: a duplicate must NOT lock/debit the card a second time
        verify(cardRepository, never()).findByIdForUpdate(any());
        verify(paymentRepository, never()).saveAndFlush(any());
    }
}
