package com.lvl.mds.orderapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code order.redis.*} - keeps stream and consumer-group naming out of
 * code so it stays in sync with the Inventory Processing Service on both legs
 * of the integration: {@code streamKey} is what this service publishes orders
 * to, {@code resultStreamKey} is what it consumes reservation outcomes from.
 */
@ConfigurationProperties(prefix = "order.redis")
public record OrderStreamProperties(
		String streamKey,
		String resultStreamKey,
		String resultConsumerGroup,
		String resultConsumerName
) {
}
