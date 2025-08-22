#!/usr/bin/env bash
set -euo pipefail

say() { printf "\n\033[1;34m[%s]\033[0m %s\n" "restore" "$*"; }

# --- create folders ---
say "Creating folders"
mkdir -p logstash/pipeline \
         tx-producer/src/main/java/com/acme/txproducer/{kafka,model,web} \
         tx-producer/src/main/resources \
         tx-consumer/src/main/java/com/acme/txconsumer/{bootstrap,entity,kafka,model,repo,web} \
         tx-consumer/src/main/resources

# --- .gitignore ---
say "Writing .gitignore"
cat > .gitignore <<'EOF'
# OS
.DS_Store

# IDE
.idea/
*.iml
.vscode/

# Java/Maven
target/
*.log
hs_err_pid*
replay_pid*
dependency-reduced-pom.xml
.settings/
.classpath
.project

# Node
node_modules/

# Docker / env
**/.env
.env
EOF

# --- docker-compose.infra.yml ---
say "Writing docker-compose.infra.yml"
cat > docker-compose.infra.yml <<'EOF'
version: "3.8"

services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,DOCKER://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,DOCKER://kafka:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,DOCKER:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    healthcheck:
      test: ["CMD-SHELL", "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 15s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8085:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092
    depends_on:
      kafka:
        condition: service_started

  cassandra:
    image: cassandra:4.1
    container_name: cassandra
    ports:
      - "9042:9042"
    environment:
      - CASSANDRA_CLUSTER_NAME=tx-cluster
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_NUM_TOKENS=128
    volumes:
      - cassandra-data:/var/lib/cassandra
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES' 127.0.0.1 9042 >/dev/null 2>&1 || exit 1"]
      interval: 20s
      timeout: 10s
      retries: 20
    restart: unless-stopped

  cql-init:
    image: cassandra:4.1
    container_name: cql-init
    depends_on:
      cassandra:
        condition: service_healthy
    entrypoint: ["/bin/bash", "-lc"]
    command: |
      set -e
      echo "Waiting for Cassandra..."
      until cqlsh cassandra 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
        echo "  ...not ready yet"; sleep 5; done
      echo "Creating keyspace txks if missing..."
      cqlsh cassandra 9042 -e "CREATE KEYSPACE IF NOT EXISTS txks WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
      echo "Verifying keyspace..."
      cqlsh cassandra 9042 -e "DESCRIBE KEYSPACE txks"
      echo "Keyspace txks is ready."
    restart: "no"

volumes:
  cassandra-data:
EOF

# --- docker-compose.elk.yml ---
say "Writing docker-compose.elk.yml"
cat > docker-compose.elk.yml <<'EOF'
version: "3.8"

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.12.2
    container_name: elasticsearch
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - ES_JAVA_OPTS=-Xms768m -Xmx768m
    ports:
      - "9200:9200"
    healthcheck:
      test: ["CMD-SHELL", "curl -s http://localhost:9200 >/dev/null || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 20

  logstash:
    image: docker.elastic.co/logstash/logstash:8.12.2
    container_name: logstash
    ports:
      - "5500:5000"
    environment:
      - LS_JAVA_OPTS=-Xms256m -Xmx256m
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline:ro
    depends_on:
      elasticsearch:
        condition: service_healthy

  kibana:
    image: docker.elastic.co/kibana/kibana:8.12.2
    container_name: kibana
    ports:
      - "5601:5601"
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    depends_on:
      elasticsearch:
        condition: service_healthy
EOF

# --- logstash/pipeline/logstash.conf ---
say "Writing logstash.conf"
cat > logstash/pipeline/logstash.conf <<'EOF'
input {
  tcp { port => 5000 codec => json_lines }
}
filter {
  if [mdc] and [mdc][correlationId] {
    mutate { add_field => { "correlationId" => "%{[mdc][correlationId]}" } }
  }
  mutate { add_field => { "service" => "%{[host][name]}" "pipeline" => "tx-apps" } }
}
output {
  elasticsearch { hosts => ["http://elasticsearch:9200"] index => "tx-logs-%{+YYYY.MM.dd}" }
  stdout { codec => rubydebug }
}
EOF

# --- tx-producer/pom.xml ---
say "Writing tx-producer/pom.xml"
cat > tx-producer/pom.xml <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.4.0</version>
    <relativePath/>
  </parent>
  <groupId>com.acme</groupId>
  <artifactId>tx-producer</artifactId>
  <version>1.0.0</version>
  <properties><java.version>21</java.version></properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-webflux</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>net.logstash.logback</groupId><artifactId>logstash-logback-encoder</artifactId><version>7.4</version></dependency>
    <dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><scope>provided</scope></dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin><artifactId>maven-compiler-plugin><configuration><release>21</release><compilerArgs><arg>-parameters</arg></compilerArgs></configuration></artifactId></plugin>
      <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
    </plugins>
  </build>
</project>
EOF

# fix a tiny xml typo above (safe guard)
perl -0777 -pe 's/<plugin><artifactId>maven-compiler-plugin><configuration>/<plugin><artifactId>maven-compiler-plugin<\/artifactId><configuration>/g' -i tx-producer/pom.xml

# --- tx-producer resources ---
say "Writing tx-producer resources"
cat > tx-producer/src/main/resources/application.yml <<'EOF'
server:
  port: 8086
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
        linger.ms: 10
        batch.size: 32768
        spring.json.add.type.headers: false
app:
  kafka:
    topic: tx.events
EOF

cat > tx-producer/src/main/resources/logback-spring.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration scan="true">
  <springProperty scope="context" name="LOGSTASH_HOST" source="LOGSTASH_HOST" defaultValue="127.0.0.1"/>
  <springProperty scope="context" name="LOGSTASH_PORT" source="LOGSTASH_PORT" defaultValue="5500"/>
  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
      <providers><timestamp/><logLevel/><loggerName/><threadName/><message/><mdc/><arguments/><stackTrace/></providers>
    </encoder>
  </appender>
  <appender name="LOGSTASH" class="net.logstash.logback.appender.LogstashTcpSocketAppender">
    <destination>${LOGSTASH_HOST}:${LOGSTASH_PORT}</destination>
    <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
      <providers><timestamp/><logLevel/><loggerName/><threadName/><message/><mdc/><context/><arguments/><stackTrace/></providers>
    </encoder>
    <keepAliveDuration>60 seconds</keepAliveDuration>
    <reconnectionDelay>5 seconds</reconnectionDelay>
  </appender>
  <root level="INFO"><appender-ref ref="CONSOLE"/><appender-ref ref="LOGSTASH"/></root>
  <logger name="com.acme" level="DEBUG"/>
</configuration>
EOF

# --- tx-producer java ---
say "Writing tx-producer Java"
cat > tx-producer/src/main/java/com/acme/txproducer/TxProducerApplication.java <<'EOF'
package com.acme.txproducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/** tx-producer: generates dummy transactions and publishes to Kafka. */
@SpringBootApplication
public class TxProducerApplication { public static void main(String[] args){ SpringApplication.run(TxProducerApplication.class, args); } }
EOF

cat > tx-producer/src/main/java/com/acme/txproducer/model/TransactionEvent.java <<'EOF'
package com.acme.txproducer.model;
import lombok.Builder; import lombok.extern.jackson.Jacksonized;
import java.math.BigDecimal; import java.time.Instant; import java.util.UUID;
/** Immutable transaction event published to Kafka. */
@Jacksonized @Builder
public record TransactionEvent(UUID transactionId, String accountId, BigDecimal amount, String currency, String type, Instant occurredAt, String description) {}
EOF

cat > tx-producer/src/main/java/com/acme/txproducer/kafka/TxPublisher.java <<'EOF'
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
EOF

cat > tx-producer/src/main/java/com/acme/txproducer/web/TxController.java <<'EOF'
package com.acme.txproducer.web;
import com.acme.txproducer.kafka.TxPublisher; import com.acme.txproducer.model.TransactionEvent;
import lombok.RequiredArgsConstructor; import org.springframework.http.MediaType; import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux; import reactor.core.publisher.Mono;
import java.math.BigDecimal; import java.time.Duration; import java.time.Instant; import java.util.Random; import java.util.UUID;
/** HTTP endpoints to publish transactions. */
@RestController @RequestMapping(path="/api/tx", produces=MediaType.APPLICATION_JSON_VALUE) @RequiredArgsConstructor
public class TxController {
  private final TxPublisher publisher; private final Random rnd = new Random();
  @PostMapping("/one") public Mono<String> one(){ return publisher.publish(randomEvent()).thenReturn("{\"status\":\"ok\"}"); }
  @PostMapping("/burst") public Mono<String> burst(@RequestParam(defaultValue="100") int count){
    return Flux.range(1,count).flatMap(i->publisher.publish(randomEvent()),64).then(Mono.just("{\"status\":\"ok\",\"sent\":"+count+"}")); }
  @PostMapping("/stream") public Mono<String> stream(@RequestParam(defaultValue="200") int count,@RequestParam(defaultValue="50") int ratePerSec){
    long periodMs = Math.max(1, 1000L/Math.max(1, ratePerSec));
    return Flux.interval(Duration.ofMillis(periodMs)).take(count).flatMap(t->publisher.publish(randomEvent()),8)
      .then(Mono.just("{\"status\":\"ok\",\"sent\":"+count+",\"rate\":"+ratePerSec+"}")); }
  private TransactionEvent randomEvent(){
    UUID txId=UUID.randomUUID(); String acct="ACC-"+(1000+rnd.nextInt(9000));
    BigDecimal amount=BigDecimal.valueOf(1+rnd.nextInt(5000)).movePointLeft(2);
    String currency="USD"; String type=rnd.nextBoolean()?"DEBIT":"CREDIT"; String desc=type.equals("DEBIT")?"Purchase":"Refund";
    return TransactionEvent.builder().transactionId(txId).accountId(acct).amount(amount).currency(currency)
      .type(type).occurredAt(Instant.now()).description(desc).build(); }
}
EOF

# --- tx-consumer/pom.xml ---
say "Writing tx-consumer/pom.xml"
cat > tx-consumer/pom.xml <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.4.0</version>
    <relativePath/>
  </parent>
  <groupId>com.acme</groupId>
  <artifactId>tx-consumer</artifactId>
  <version>1.0.0</version>
  <properties><java.version>21</java.version></properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-webflux</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-cassandra-reactive</artifactId></dependency>
    <dependency><groupId>net.logstash.logback</groupId><artifactId>logstash-logback-encoder</artifactId><version>7.4</version></dependency>
    <dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><scope>provided</scope></dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin><artifactId>maven-compiler-plugin</artifactId><configuration><release>21</release><compilerArgs><arg>-parameters</arg></compilerArgs></configuration></plugin>
      <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
    </plugins>
  </build>
</project>
EOF

# --- tx-consumer resources ---
say "Writing tx-consumer resources"
cat > tx-consumer/src/main/resources/application.yml <<'EOF'
server:
  port: 8087
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: tx-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: com.acme.txconsumer.*
        spring.json.value.default.type: com.acme.txconsumer.model.TransactionEvent
        spring.json.use.type.headers: false
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.ByteArraySerializer
  cassandra:
    contact-points: localhost
    port: 9042
    keyspace-name: txks
    local-datacenter: datacenter1
    schema-action: create_if_not_exists
app:
  kafka:
    topic: tx.events
  consumer:
    concurrency: 2
EOF

cat > tx-consumer/src/main/resources/logback-spring.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration scan="true">
  <springProperty scope="context" name="LOGSTASH_HOST" source="LOGSTASH_HOST" defaultValue="127.0.0.1"/>
  <springProperty scope="context" name="LOGSTASH_PORT" source="LOGSTASH_PORT" defaultValue="5500"/>
  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
      <providers><timestamp/><logLevel/><loggerName/><threadName/><message/><mdc/><arguments/><stackTrace/></providers>
    </encoder>
  </appender>
  <appender name="LOGSTASH" class="net.logstash.logback.appender.LogstashTcpSocketAppender">
    <destination>${LOGSTASH_HOST}:${LOGSTASH_PORT}</destination>
    <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
      <providers><timestamp/><logLevel/><loggerName/><threadName/><message/><mdc/><context/><arguments/><stackTrace/></providers>
    </encoder>
    <keepAliveDuration>60 seconds</keepAliveDuration>
    <reconnectionDelay>5 seconds</reconnectionDelay>
  </appender>
  <root level="INFO"><appender-ref ref="CONSOLE"/><appender-ref ref="LOGSTASH"/></root>
  <logger name="com.acme" level="DEBUG"/>
</configuration>
EOF

# --- tx-consumer java ---
say "Writing tx-consumer Java"
cat > tx-consumer/src/main/java/com/acme/txconsumer/TxConsumerApplication.java <<'EOF'
package com.acme.txconsumer;
import org.springframework.boot.SpringApplication; import org.springframework.boot.autoconfigure.SpringBootApplication;
/** tx-consumer: Consumes Kafka events and persists to Cassandra reactively. */
@SpringBootApplication
public class TxConsumerApplication { public static void main(String[] args){ SpringApplication.run(TxConsumerApplication.class, args); } }
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/bootstrap/KeyspaceBootstrap.java <<'EOF'
package com.acme.txconsumer.bootstrap;
import com.datastax.oss.driver.api.core.CqlSession; import lombok.RequiredArgsConstructor; import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent; import org.springframework.context.event.EventListener; import org.springframework.stereotype.Component;
/** Ensures keyspace exists (harmless if already created by docker init). */
@Slf4j @Component @RequiredArgsConstructor
public class KeyspaceBootstrap {
  private final CqlSession session;
  @EventListener(ApplicationReadyEvent.class)
  public void ensureKeyspace(){ session.execute("CREATE KEYSPACE IF NOT EXISTS txks WITH replication = {'class':'SimpleStrategy','replication_factor':1}"); log.info("Keyspace 'txks' ensured."); }
}
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/model/TransactionEvent.java <<'EOF'
package com.acme.txconsumer.model;
import lombok.Builder; import lombok.extern.jackson.Jacksonized;
import java.math.BigDecimal; import java.time.Instant; import java.util.UUID;
/** DTO matching producer's JSON. */
@Jacksonized @Builder
public record TransactionEvent(UUID transactionId, String accountId, BigDecimal amount, String currency, String type, Instant occurredAt, String description) {}
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/entity/TransactionEntity.java <<'EOF'
package com.acme.txconsumer.entity;
import org.springframework.data.annotation.Id; import org.springframework.data.cassandra.core.mapping.PrimaryKey; import org.springframework.data.cassandra.core.mapping.Table;
import java.math.BigDecimal; import java.time.Instant; import java.util.UUID;
/** Cassandra table mapping for transactions. Idempotency via primary key. */
@Table("transactions")
public class TransactionEntity {
  @Id @PrimaryKey private UUID transactionId; private String accountId; private BigDecimal amount; private String currency; private String type; private Instant occurredAt; private String description;
  public TransactionEntity() {}
  public TransactionEntity(UUID transactionId,String accountId,BigDecimal amount,String currency,String type,Instant occurredAt,String description){this.transactionId=transactionId;this.accountId=accountId;this.amount=amount;this.currency=currency;this.type=type;this.occurredAt=occurredAt;this.description=description;}
  public UUID getTransactionId(){return transactionId;} public String getAccountId(){return accountId;} public BigDecimal getAmount(){return amount;} public String getCurrency(){return currency;}
  public String getType(){return type;} public Instant getOccurredAt(){return occurredAt;} public String getDescription(){return description;}
  public void setTransactionId(UUID v){this.transactionId=v;} public void setAccountId(String v){this.accountId=v;} public void setAmount(BigDecimal v){this.amount=v;}
  public void setCurrency(String v){this.currency=v;} public void setType(String v){this.type=v;} public void setOccurredAt(Instant v){this.occurredAt=v;} public void setDescription(String v){this.description=v;}
}
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/repo/TransactionRepository.java <<'EOF'
package com.acme.txconsumer.repo;
import com.acme.txconsumer.entity.TransactionEntity; import org.springframework.data.cassandra.repository.ReactiveCassandraRepository; import java.util.UUID;
public interface TransactionRepository extends ReactiveCassandraRepository<TransactionEntity, UUID> { }
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/kafka/ConsumerErrorConfig.java <<'EOF'
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
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/kafka/TxEventConsumer.java <<'EOF'
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
EOF

cat > tx-consumer/src/main/java/com/acme/txconsumer/web/TxQueryController.java <<'EOF'
package com.acme.txconsumer.web;
import com.acme.txconsumer.entity.TransactionEntity; import com.acme.txconsumer.repo.TransactionRepository;
import lombok.RequiredArgsConstructor; import org.springframework.http.MediaType; import org.springframework.web.bind.annotation.*; import reactor.core.publisher.Flux;
/** Simple read-back endpoint. */
@RestController @RequestMapping(path="/api/tx", produces=MediaType.APPLICATION_JSON_VALUE) @RequiredArgsConstructor
public class TxQueryController {
  private final TransactionRepository repo;
  @GetMapping("/all") public Flux<TransactionEntity> all(){ return repo.findAll().take(50); }
}
EOF

say "All files written ✅"
