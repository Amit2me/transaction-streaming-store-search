package com.acme.txproducer.kafka;

import com.acme.txproducer.metrics.ProducerMetrics;
import com.acme.txproducer.model.TransactionEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Service
@Slf4j
public class TxPublisher {
    private final KafkaTemplate<String, TransactionEvent> template;
    private final ProducerMetrics metrics;

    @Value("${app.kafka.topic}")
    private String topic;

    public Mono<Void> publish(TransactionEvent ev) {
        String key = ev.transactionId().toString();
        metrics.amountSummary.record(ev.amount().doubleValue());

        java.util.Map<String, String> capturedMdc = org.slf4j.MDC.getCopyOfContextMap();

        return Mono.fromCallable(Timer::start)
                .flatMap(sample -> {
                    ProducerRecord<String, TransactionEvent> rec = new ProducerRecord<>(topic, key, ev);
                    CompletableFuture<SendResult<String, TransactionEvent>> fut = template.send(rec);
                    fut.whenComplete((res, ex) -> {
                        var prev = MDC.getCopyOfContextMap();
                        try {
                            if (capturedMdc != null) MDC.setContextMap(capturedMdc);
                            else MDC.clear();
                            if (ex != null) {
                                metrics.publishedFail.increment();
                                log.warn("Kafka send failed for key {}: {}", key, ex.toString());
                            } else {
                                metrics.publishedOk.increment();
                                log.debug("Kafka send ok: p={}, off={}", res.getRecordMetadata().partition(), res.getRecordMetadata().offset());
                            }
                        } finally {
                            if (prev != null) org.slf4j.MDC.setContextMap(prev);
                            else org.slf4j.MDC.clear();
                        }
                    });
                    return Mono.fromFuture(fut).doFinally(sig -> sample.stop(metrics.publishTimer)).then();
                });
    }
}
