package com.lvl.mds.orderapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code order.status-stream.*} - tuning for the
 * {@code GET /orders/{orderId}/status} SSE endpoint.
 *
 * @param timeoutMs how long a subscriber's connection is held open before the
 *                  server closes it; the browser's {@code EventSource} (or any
 *                  well-behaved client) simply reconnects and gets a fresh
 *                  snapshot, so a finite value keeps abandoned connections
 *                  from accumulating.
 */
@ConfigurationProperties(prefix = "order.status-stream")
public record OrderStatusStreamProperties(long timeoutMs) {
}
