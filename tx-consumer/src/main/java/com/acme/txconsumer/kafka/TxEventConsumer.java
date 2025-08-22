package com.acme.txconsumer.kafka;
import com.acme.txconsumer.entity.TransactionEntity; import com.acme.txconsumer.model.TransactionEvent; import com.acme.txconsumer.repo.TransactionRepository;
import lombok.RequiredArgsConstructor; import lombok.extern.slf4j.Slf4j; import org.springframework.kafka.annotation.KafkaListener; import org.springframework.kafka.support.KafkaHeaders; import org.springframework.messaging.handler.annotation.Header; import org.springframework.stereotype.Component; import reactor.core.publisher.Mono;
/** Consumes TransactionEvent and persists idempotently into Cassandra. */
@Slf4j @Component @RequiredArgsConstructor
public class TxEventConsumer {
  private final TransactionRepository repository;
  @KafkaListener(topics="${app.kafka.topic}", groupId="${spring.kafka.consumer.group-id}", concurrency="${app.consumer.concurrency:1}")
  public void onMessage(TransactionEvent ev, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset){
    log.debug("Consumed {} ({}-{}@{})", ev.transactionId(), topic, partition, offset);
    TransactionEntity e = new TransactionEntity(ev.transactionId(), ev.accountId(), ev.amount(), ev.currency(), ev.type(), ev.occurredAt(), ev.description());
    repository.save(e).doOnError(ex->log.warn("Cassandra save failed for {}: {}", ev.transactionId(), ex.toString())).onErrorResume(ex->Mono.empty()).subscribe();
  }
}
