package com.lvl.mds.orderapi.config;

import com.lvl.mds.orderapi.messaging.consumers.ReservationResultConsumer;
import com.lvl.mds.orderapi.messaging.StreamListenerLifecycle;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
 * {@link ReservationResultConsumer} - order-service's half of the feedback
 * loop, mirroring the listener inventory-service runs on
 * {@code orders-stream}.
 *
 * <p>The container is handed to a {@link StreamListenerLifecycle} rather than
 * exposed as a bean of its own, so that one object owns when consumption
 * starts and - the part that needs care - when it stops. See that class for
 * why stopping needs more than {@code container.stop()}.
 *
 * <p>The {@code @DependsOn} isn't decoration: the container begins polling
 * with {@code XREADGROUP} as soon as it is started, which fails with NOGROUP
 * unless the consumer group already exists, and it must not race the startup
 * drain of pending entries either. Starting from the lifecycle phase now
 * guarantees that ordering on its own, since every singleton is constructed
 * before any {@code start()} runs; the annotation stays because it makes the
 * requirement explicit at the point it is needed rather than resting on that
 * framework detail.
 *
 * <p>{@code order.redis.result-listener-enabled=false} turns the whole return
 * leg off, which is what the context test uses to boot the API without a
 * Redis to connect to.
 */
@Configuration
@ConditionalOnProperty(prefix = "order.redis", name = "result-listener-enabled", havingValue = "true", matchIfMissing = true)
public class ResultStreamListenerConfig {

	@Bean
	@DependsOn("resultStreamInitializer")
	public StreamListenerLifecycle resultStreamListenerLifecycle(
			RedisConnectionFactory connectionFactory,
			ReservationResultConsumer reservationResultConsumer,
			OrderStreamProperties properties,
			ShutdownProperties shutdownProperties) {

		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofSeconds(1))
						.build();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				StreamMessageListenerContainer.create(connectionFactory, options);

		return new StreamListenerLifecycle(
				container,
				Consumer.from(properties.resultConsumerGroup(), properties.resultConsumerName()),
				StreamOffset.create(properties.resultStreamKey(), ReadOffset.lastConsumed()),
				reservationResultConsumer,
				Duration.ofMillis(shutdownProperties.listenerDrainTimeoutMs()));
	}
}
