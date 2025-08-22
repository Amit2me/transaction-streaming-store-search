package com.acme.txproducer.model;
import lombok.Builder; import lombok.extern.jackson.Jacksonized;
import java.math.BigDecimal; import java.time.Instant; import java.util.UUID;
/** Immutable transaction event published to Kafka. */
@Jacksonized @Builder
public record TransactionEvent(UUID transactionId, String accountId, BigDecimal amount, String currency, String type, Instant occurredAt, String description) {}
