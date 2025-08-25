package com.acme.txconsumer.model;

import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * DTO matching producer's JSON.
 */
@Jacksonized
@Builder
public record TransactionEvent(UUID transactionId, String accountId, BigDecimal amount, String currency, String type,
                               Instant occurredAt, String description) {
}
