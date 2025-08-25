package com.acme.txconsumer.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Cassandra table mapping for transactions. Idempotency via primary key.
 */
@Table("transactions")
public class TransactionEntity {
    @Id
    @PrimaryKey
    private UUID transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String type;
    private Instant occurredAt;
    private String description;

    public TransactionEntity() {
    }

    public TransactionEntity(UUID transactionId, String accountId, BigDecimal amount, String currency, String type, Instant occurredAt, String description) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.type = type;
        this.occurredAt = occurredAt;
        this.description = description;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(UUID v) {
        this.transactionId = v;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String v) {
        this.accountId = v;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal v) {
        this.amount = v;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String v) {
        this.currency = v;
    }

    public String getType() {
        return type;
    }

    public void setType(String v) {
        this.type = v;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant v) {
        this.occurredAt = v;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String v) {
        this.description = v;
    }
}
