package com.acme.txproducer.web;

import com.acme.txproducer.kafka.TxPublisher;
import com.acme.txproducer.model.TransactionEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

/**
 * HTTP endpoints to publish transactions.
 */
@RestController
@RequestMapping(path = "/api/tx", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class TxController {
    private final TxPublisher publisher;
    private final Random rnd = new Random();

    @PostMapping("/one")
    public Mono<String> one() {
        return publisher.publish(randomEvent()).thenReturn("{\"status\":\"ok\"}");
    }

    @PostMapping("/burst")
    public Mono<String> burst(@RequestParam(defaultValue = "100") int count) {
        return Flux.range(1, count).flatMap(i -> publisher.publish(randomEvent()), 64).then(Mono.just("{\"status\":\"ok\",\"sent\":" + count + "}"));
    }

    @PostMapping("/stream")
    public Mono<String> stream(@RequestParam(defaultValue = "200") int count, @RequestParam(defaultValue = "50") int ratePerSec) {
        long periodMs = Math.max(1, 1000L / Math.max(1, ratePerSec));
        return Flux.interval(Duration.ofMillis(periodMs)).take(count).flatMap(t -> publisher.publish(randomEvent()), 8)
                .then(Mono.just("{\"status\":\"ok\",\"sent\":" + count + ",\"rate\":" + ratePerSec + "}"));
    }

    private TransactionEvent randomEvent() {
        UUID txId = UUID.randomUUID();
        String acct = "ACC-" + (1000 + rnd.nextInt(9000));
        BigDecimal amount = BigDecimal.valueOf(1 + rnd.nextInt(5000)).movePointLeft(2);
        String currency = "USD";
        String type = rnd.nextBoolean() ? "DEBIT" : "CREDIT";
        String desc = type.equals("DEBIT") ? "Purchase" : "Refund";
        return TransactionEvent.builder().transactionId(txId).accountId(acct).amount(amount).currency(currency)
                .type(type).occurredAt(Instant.now()).description(desc).build();
    }
}
