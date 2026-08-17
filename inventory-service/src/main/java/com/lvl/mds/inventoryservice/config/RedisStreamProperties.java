package com.lvl.mds.inventoryservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code inventory.redis.*} - stream/consumer group naming shared with
 * the reclaim job and the order-service publisher.
 */
@ConfigurationProperties(prefix = "inventory.redis")
public record RedisStreamProperties(
		String streamKey,
		String dlqStreamKey,
		String consumerGroup,
		String consumerName
) {
}
