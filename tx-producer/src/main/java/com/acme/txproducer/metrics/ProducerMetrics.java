package com.acme.txproducer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class ProducerMetrics {
    public final Counter publishedOk;
    public final Counter publishedFail;
    public final Timer publishTimer;
    public final DistributionSummary amountSummary;

    public ProducerMetrics(MeterRegistry reg) {
        this.publishedOk   = Counter.builder("tx_published_total").description("Tx events successfully published").register(reg);
        this.publishedFail = Counter.builder("tx_publish_fail_total").description("Tx publish failures").register(reg);
        this.publishTimer  = Timer.builder("tx_publish_seconds").description("Publish latency").register(reg);
        this.amountSummary = DistributionSummary.builder("tx_amount_usd").description("Tx amount USD").publishPercentileHistogram().register(reg);
    }
}
