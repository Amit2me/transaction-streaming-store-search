package com.acme.txproducer.web;

import org.slf4j.MDC;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

/**
 * Adds/propagates X-Correlation-Id and puts it into MDC for logging.
 */
@Component
public class CorrelationIdFilter implements WebFilter {
    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest req = exchange.getRequest();
        String cid = Optional.ofNullable(req.getHeaders().getFirst(HEADER))
                .filter(s -> !s.isBlank())
                .orElse(UUID.randomUUID().toString());
        MDC.put(MDC_KEY, cid);
        exchange.getResponse().getHeaders().set(HEADER, cid);

        // One INFO per HTTP request ensures at least one doc with MDC present
        org.slf4j.LoggerFactory.getLogger(getClass())
                .info("HTTP {} {} cid={}", req.getMethod(), req.getURI().getPath(), cid);

        return chain.filter(exchange).doFinally(sig -> MDC.remove(MDC_KEY));
    }
}
