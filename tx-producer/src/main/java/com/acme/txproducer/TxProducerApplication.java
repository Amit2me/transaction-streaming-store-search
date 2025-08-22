package com.acme.txproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * tx-producer: generates dummy transactions and publishes to Kafka.
 */
@SpringBootApplication
public class TxProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(TxProducerApplication.class, args);
    }
}
