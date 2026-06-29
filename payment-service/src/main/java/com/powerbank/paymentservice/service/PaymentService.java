package com.powerbank.paymentservice.service;

import com.powerbank.paymentservice.domain.Card;
import com.powerbank.paymentservice.domain.CardStatus;
import com.powerbank.paymentservice.domain.Payment;
import com.powerbank.paymentservice.domain.PaymentStatus;
import com.powerbank.paymentservice.domain.PaymentType;
import com.powerbank.paymentservice.messaging.event.PaymentRequest;
import com.powerbank.paymentservice.repository.CardRepository;
import com.powerbank.paymentservice.repository.PaymentRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Owns the financial state machine for a single {@link Payment}.
 *
 * <p>Atomicity: the whole charge/refund runs in ONE DB transaction, and the
 * card row is taken under a pessimistic write lock, so concurrent operations on
 * the same card are serialized and the balance can never go negative or be lost.
 *
 * <p>Idempotency: {@code idempotency_key} is UNIQUE. A repeat request returns
 * the already-persisted Payment instead of creating a new one. The check-then-
 * insert race is closed by catching the unique-constraint violation.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final CardRepository cardRepository;

    /**
     * Process a charge or refund idempotently. Always returns a persisted
     * Payment (existing one on a duplicate key).
     */
    @Transactional
    public Payment process(PaymentRequest request) {
        Optional<Payment> existing = paymentRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existing.isPresent()) {
            return handleDuplicate(existing.get(), request);
        }
        try {
            return executeNew(request);
        } catch (DataIntegrityViolationException raceLost) {
            // a concurrent request with the same key won the insert — return its payment
            log.warn("Idempotency race for key={}, returning the winning payment", request.idempotencyKey());
            return paymentRepository.findByIdempotencyKey(request.idempotencyKey())
                    .orElseThrow(() -> raceLost);
        }
    }

    private Payment handleDuplicate(Payment existing, PaymentRequest request) {
        if (existing.getAmount().compareTo(request.amount()) != 0) {
            // Same key, different amount: idempotency means "same key == same operation".
            // We DO NOT apply the new amount; we return the original and flag it.
            log.warn("Idempotency conflict: key={} first amount={} now amount={} -> returning original",
                    request.idempotencyKey(), existing.getAmount(), request.amount());
        } else {
            log.info("Duplicate payment-request key={} -> returning existing payment {}",
                    request.idempotencyKey(), existing.getId());
        }
        return existing;
    }

    private Payment executeNew(PaymentRequest request) {
        PaymentType type = PaymentType.valueOf(request.type());

        Payment payment = new Payment();
        payment.setId(UUID.randomUUID());
        payment.setIdempotencyKey(request.idempotencyKey());
        payment.setCardId(request.cardId());
        payment.setRentalId(request.rentalId());
        payment.setUserId(request.userId());
        payment.setAmount(request.amount());
        payment.setCurrency(request.currency() == null ? "UZS" : request.currency());
        payment.setType(type);
        payment.setStatus(PaymentStatus.PENDING);

        Card card = cardRepository.findByIdForUpdate(request.cardId()).orElse(null);
        if (card == null) {
            return fail(payment, "CARD_NOT_FOUND");
        }
        if (card.getStatus() != CardStatus.ACTIVE) {
            return fail(payment, "CARD_NOT_ACTIVE");
        }
        if (request.amount() == null || request.amount().signum() <= 0) {
            return fail(payment, "INVALID_AMOUNT");
        }

        if (type == PaymentType.CHARGE) {
            if (card.getBalance().compareTo(request.amount()) < 0) {
                return fail(payment, "INSUFFICIENT_FUNDS");
            }
            card.setBalance(card.getBalance().subtract(request.amount()));
        } else { // REFUND
            card.setBalance(card.getBalance().add(request.amount()));
        }

        cardRepository.save(card);
        payment.setStatus(PaymentStatus.SUCCEEDED);
        // unique-constraint violation on idempotency_key surfaces here / on flush
        Payment saved = paymentRepository.saveAndFlush(payment);
        log.info("Payment {} {} amount={} card={} -> SUCCEEDED (balance now {})",
                saved.getId(), type, saved.getAmount(), card.getId(), card.getBalance());
        return saved;
    }

    private Payment fail(Payment payment, String reason) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setFailureReason(reason);
        Payment saved = paymentRepository.saveAndFlush(payment);
        log.warn("Payment {} {} amount={} -> FAILED ({})",
                saved.getId(), saved.getType(), saved.getAmount(), reason);
        return saved;
    }

    /** Bind / register an emulated card (modelled here without a real acquirer). */
    @Transactional
    public Card bindCard(UUID cardId, UUID userId, String maskedPan, BigDecimal initialBalance, String currency) {
        return cardRepository.findById(cardId).orElseGet(() -> {
            Card card = new Card(cardId, userId, maskedPan,
                    initialBalance == null ? BigDecimal.ZERO : initialBalance,
                    currency == null ? "UZS" : currency);
            Card saved = cardRepository.save(card);
            log.info("Bound card {} for user {}", saved.getId(), userId);
            return saved;
        });
    }
}
