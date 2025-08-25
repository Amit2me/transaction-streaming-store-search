package com.acme.txconsumer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class ConsumerMetrics {
    public final Counter consumed;
    public final Counter savedOk;
    public final Counter saveFail;
    public final Timer saveTimer;

    public ConsumerMetrics(MeterRegistry reg) {
        this.consumed = Counter.builder("tx_consumed_total").description("Kafka consumed events").register(reg);
        this.savedOk  = Counter.builder("tx_saved_total").description("Cassandra saves ok").register(reg);
        this.saveFail = Counter.builder("tx_save_fail_total").description("Cassandra save failures").register(reg);
        this.saveTimer= Timer.builder("tx_save_seconds").description("Cassandra save latency").register(reg);
    }
}
