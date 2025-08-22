package com.acme.txconsumer.kafka;
import org.apache.kafka.common.TopicPartition; import org.springframework.context.annotation.Bean; import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate; import org.springframework.kafka.listener.DefaultErrorHandler; import org.springframework.kafka.listener.DeadLetterPublishingRecoverer; import org.springframework.util.backoff.FixedBackOff;
/** Retries failed records then routes to <topic>.DLT */
@Configuration
public class ConsumerErrorConfig {
  @Bean public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Object,Object> template){
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, ex) -> new TopicPartition(record.topic()+".DLT", record.partition()));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3));
  }
}
