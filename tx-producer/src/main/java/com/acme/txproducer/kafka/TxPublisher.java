package com.acme.txproducer.kafka;
import com.acme.txproducer.model.TransactionEvent;
import lombok.RequiredArgsConstructor; import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value; import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult; import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono; import java.util.concurrent.CompletableFuture;
/** Publishes TransactionEvent to Kafka using idempotent producer settings. */
@Slf4j @Service @RequiredArgsConstructor
public class TxPublisher {
  private final KafkaTemplate<String, TransactionEvent> template;
  @Value("${app.kafka.topic}") private String topic;
  public Mono<Void> publish(TransactionEvent ev){
    String key = ev.transactionId().toString();
    ProducerRecord<String, TransactionEvent> rec = new ProducerRecord<>(topic, key, ev);
    CompletableFuture<SendResult<String, TransactionEvent>> fut = template.send(rec);
    fut.whenComplete((res, ex)->{ if(ex!=null) log.warn("Kafka send failed for key {}: {}", key, ex.toString());
      else log.debug("Kafka send ok: p={}, off={}", res.getRecordMetadata().partition(), res.getRecordMetadata().offset());});
    return Mono.fromFuture(fut).then();
  }
}
