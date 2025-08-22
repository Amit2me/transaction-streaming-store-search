package com.acme.txconsumer.web;
import com.acme.txconsumer.entity.TransactionEntity; import com.acme.txconsumer.repo.TransactionRepository;
import lombok.RequiredArgsConstructor; import org.springframework.http.MediaType; import org.springframework.web.bind.annotation.*; import reactor.core.publisher.Flux;
/** Simple read-back endpoint. */
@RestController @RequestMapping(path="/api/tx", produces=MediaType.APPLICATION_JSON_VALUE) @RequiredArgsConstructor
public class TxQueryController {
  private final TransactionRepository repo;
  @GetMapping("/all") public Flux<TransactionEntity> all(){ return repo.findAll().take(50); }
}
