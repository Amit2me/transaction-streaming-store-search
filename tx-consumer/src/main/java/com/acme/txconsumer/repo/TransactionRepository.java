package com.acme.txconsumer.repo;
import com.acme.txconsumer.entity.TransactionEntity; import org.springframework.data.cassandra.repository.ReactiveCassandraRepository; import java.util.UUID;
public interface TransactionRepository extends ReactiveCassandraRepository<TransactionEntity, UUID> { }
