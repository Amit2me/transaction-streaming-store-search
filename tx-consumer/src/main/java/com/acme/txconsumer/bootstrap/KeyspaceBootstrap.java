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
