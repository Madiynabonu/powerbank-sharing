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

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final CardRepository cardRepository;

    /**
     * Idempotent charge/refund. On duplicate idempotency_key the existing payment
     * is returned; the check-then-insert race is closed by catching the unique-constraint
     * violation and re-querying for the winner.
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
            log.warn("Idempotency race for key={}, returning the winning payment", request.idempotencyKey());
            return paymentRepository.findByIdempotencyKey(request.idempotencyKey())
                    .orElseThrow(() -> raceLost);
        }
    }

    private Payment handleDuplicate(Payment existing, PaymentRequest request) {
        if (existing.getAmount().compareTo(request.amount()) != 0) {
            // Same key + different amount: per Stripe semantics we return the original unchanged.
            log.warn("Idempotency conflict: key={} first amount={} now amount={} -> returning original",
                    request.idempotencyKey(), existing.getAmount(), request.amount());
        } else {
            log.info("Duplicate payment-request key={} -> returning existing payment {}",
                    request.idempotencyKey(), existing.getId());
        }
        return existing;
    }

    private Payment executeNew(PaymentRequest request) {
        PaymentType type;
        try {
            type = PaymentType.valueOf(request.type());
        } catch (IllegalArgumentException e) {
            Payment payment = buildPayment(request, PaymentType.CHARGE);
            return fail(payment, "INVALID_PAYMENT_TYPE");
        }

        Payment payment = buildPayment(request, type);

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
        } else {
            card.setBalance(card.getBalance().add(request.amount()));
        }

        cardRepository.save(card);
        payment.setStatus(PaymentStatus.SUCCEEDED);
        // saveAndFlush so the unique-constraint violation on idempotency_key surfaces here, not at commit
        Payment saved = paymentRepository.saveAndFlush(payment);
        log.info("Payment {} {} amount={} card={} -> SUCCEEDED (balance now {})",
                saved.getId(), type, saved.getAmount(), card.getId(), card.getBalance());
        return saved;
    }

    /**
     * Reverse a SUCCEEDED payment. Refunds the card balance and creates a CANCEL record.
     * No-op if payment is already terminal or not found.
     */
    @Transactional
    public void cancel(String originalIdempotencyKey) {
        Optional<Payment> opt = paymentRepository.findByIdempotencyKey(originalIdempotencyKey);
        if (opt.isEmpty()) {
            log.warn("cancel: no payment found for key={}", originalIdempotencyKey);
            return;
        }
        Payment original = opt.get();
        if (original.getStatus() == PaymentStatus.FAILED
                || original.getType() == PaymentType.CANCEL) {
            log.info("cancel: payment {} already terminal, skipping", original.getId());
            return;
        }
        if (original.getStatus() != PaymentStatus.SUCCEEDED) {
            log.warn("cancel: payment {} is {}, nothing to reverse", original.getId(), original.getStatus());
            return;
        }

        Card card = cardRepository.findByIdForUpdate(original.getCardId()).orElse(null);
        if (card == null) {
            log.error("cancel: card {} not found for payment {}", original.getCardId(), original.getId());
            return;
        }
        card.setBalance(card.getBalance().add(original.getAmount()));
        cardRepository.save(card);

        original.setStatus(PaymentStatus.FAILED);
        original.setFailureReason("CANCELLED");
        paymentRepository.save(original);

        Payment reversal = new Payment();
        reversal.setId(UUID.randomUUID());
        reversal.setIdempotencyKey("cancel-" + originalIdempotencyKey);
        reversal.setCardId(original.getCardId());
        reversal.setRentalId(original.getRentalId());
        reversal.setUserId(original.getUserId());
        reversal.setAmount(original.getAmount());
        reversal.setCurrency(original.getCurrency());
        reversal.setType(PaymentType.CANCEL);
        reversal.setStatus(PaymentStatus.SUCCEEDED);
        paymentRepository.saveAndFlush(reversal);
        log.info("Cancelled payment {} -> reversal {} card balance now {}",
                original.getId(), reversal.getId(), card.getBalance());
    }

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

    private Payment buildPayment(PaymentRequest request, PaymentType type) {
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
        return payment;
    }

    private Payment fail(Payment payment, String reason) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setFailureReason(reason);
        Payment saved = paymentRepository.saveAndFlush(payment);
        log.warn("Payment {} {} amount={} -> FAILED ({})",
                saved.getId(), saved.getType(), saved.getAmount(), reason);
        return saved;
    }
}
