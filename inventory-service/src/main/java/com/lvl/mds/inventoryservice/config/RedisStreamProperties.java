package com.lvl.mds.inventoryservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code inventory.redis.*} - stream/consumer group naming shared with
 * the reclaim job and the order-service publisher.
 *
 * <p>{@code resultStreamKey} is the return leg of the integration: the stream
 * this service publishes reservation outcomes to and {@code order-service}
 * consumes from. It has to stay in sync with {@code order.redis.result-stream-key}
 * on the other side.
 */
@ConfigurationProperties(prefix = "inventory.redis")
public record RedisStreamProperties(
		String streamKey,
		String dlqStreamKey,
		String resultStreamKey,
		String consumerGroup,
		String consumerName
) {
}
