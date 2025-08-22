package com.acme.txconsumer;
import org.springframework.boot.SpringApplication; import org.springframework.boot.autoconfigure.SpringBootApplication;
/** tx-consumer: Consumes Kafka events and persists to Cassandra reactively. */
@SpringBootApplication
public class TxConsumerApplication { public static void main(String[] args){ SpringApplication.run(TxConsumerApplication.class, args); } }
