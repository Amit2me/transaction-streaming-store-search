package com.acme.txconsumer.bootstrap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

/**
 * Builds a CqlSession sustainably:
 * 1) Connects without keyspace.
 * 2) Creates keyspace if missing.
 * 3) Returns a session bound to the keyspace.
 */
@Configuration
public class CassandraConfig {

    @Value("${app.cassandra.host:127.0.0.1}")
    String host;

    @Value("${app.cassandra.port:9042}")
    int port;

    @Value("${app.cassandra.local-dc:datacenter1}")
    String localDc;

    @Value("${app.cassandra.keyspace:txks}")
    String keyspace;

    @Value("${app.cassandra.replication.class:SimpleStrategy}")
    String replClass;

    @Value("${app.cassandra.replication.rf:1}")
    int replRf;

    @Bean
    public CqlSession cqlSession() {
        // 1) seed session (no keyspace)
        CqlSession seed = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(localDc)
                .build();

        // 2) create keyspace if missing (idempotent)
        String createKeyspace = """
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {'class':'%s','replication_factor':%d}
                """.formatted(keyspace, replClass, replRf);
        seed.execute(createKeyspace);

        seed.close();

        // 3) return keyspace-bound session
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(CqlIdentifier.fromCql(keyspace))
                .build();
    }
}
