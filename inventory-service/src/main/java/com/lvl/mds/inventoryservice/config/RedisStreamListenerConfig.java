package com.lvl.mds.inventoryservice.config;

import com.lvl.mds.inventoryservice.messaging.StreamInitializer;
import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventConsumer;
import com.lvl.mds.inventoryservice.messaging.StreamListenerLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;

import java.time.Duration;

/**
 * Wires the {@link StreamMessageListenerContainer} that drives
 * {@link OrderEventConsumer}. A dedicated container (rather than
 * {@code @Bean StreamListener} auto-wiring) is used so the poll timeout and
 * consumer/group names are explicit and configurable.
 *
 * <p>The container is handed to a {@link StreamListenerLifecycle} rather than
 * exposed as a bean of its own, so that one object owns when consumption
 * starts and - the part that needs care - when it stops. See that class for
 * why stopping needs more than {@code container.stop()}.
 *
 * <p>The {@code @DependsOn} isn't decoration: the container begins polling
 * with {@code XREADGROUP} as soon as it is started, which fails with NOGROUP
 * unless {@link StreamInitializer} has
 * already created the consumer group - and it must not consume orders before
 * {@link InventorySeedInitializer} has put any stock in the repository.
 * Starting from the lifecycle phase now guarantees that ordering on its own,
 * since every singleton is constructed before any {@code start()} runs; the
 * annotation stays because it makes the requirement explicit at the point it
 * is needed rather than resting on that framework detail.
 */
@Configuration
public class RedisStreamListenerConfig {

	@Bean
	@DependsOn({"streamInitializer", "inventorySeedInitializer"})
	public StreamListenerLifecycle orderStreamListenerLifecycle(
			RedisConnectionFactory connectionFactory,
			OrderEventConsumer orderEventConsumer,
			RedisStreamProperties properties,
			ShutdownProperties shutdownProperties) {

		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofSeconds(1))
						.build();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				StreamMessageListenerContainer.create(connectionFactory, options);

		return new StreamListenerLifecycle(
				container,
				Consumer.from(properties.consumerGroup(), properties.consumerName()),
				StreamOffset.create(properties.streamKey(), ReadOffset.lastConsumed()),
				orderEventConsumer,
				Duration.ofMillis(shutdownProperties.listenerDrainTimeoutMs()));
	}
}
