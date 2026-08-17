package com.lvl.mds.orderapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code order.redis.*} properties - keeps the Redis Stream key out of
 * code so it stays in sync with the value the Inventory Processing Service
 * consumes from.
 */
@ConfigurationProperties(prefix = "order.redis")
public record OrderStreamProperties(String streamKey) {
}
