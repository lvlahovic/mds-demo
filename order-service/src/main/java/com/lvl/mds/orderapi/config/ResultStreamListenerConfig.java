package com.lvl.mds.orderapi.config;

import com.lvl.mds.orderapi.messaging.ReservationResultConsumer;
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
 * <p>The {@code @DependsOn} isn't decoration: the container begins polling
 * with {@code XREADGROUP} as soon as it is started, which fails with NOGROUP
 * unless the consumer group already exists, and it must not race the startup
 * drain of pending entries either. Nothing else in the wiring guarantees that
 * ordering between a scanned component and a {@code @Bean} method.
 *
 * <p>{@code order.redis.result-listener-enabled=false} turns the whole return
 * leg off, which is what the context test uses to boot the API without a
 * Redis to connect to.
 */
@Configuration
@ConditionalOnProperty(prefix = "order.redis", name = "result-listener-enabled", havingValue = "true", matchIfMissing = true)
public class ResultStreamListenerConfig {

	@Bean(destroyMethod = "stop")
	@DependsOn("resultStreamInitializer")
	public StreamMessageListenerContainer<String, MapRecord<String, String, String>> resultStreamListenerContainer(
			RedisConnectionFactory connectionFactory,
			ReservationResultConsumer reservationResultConsumer,
			OrderStreamProperties properties) {

		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofSeconds(1))
						.build();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				StreamMessageListenerContainer.create(connectionFactory, options);

		container.receive(
				Consumer.from(properties.resultConsumerGroup(), properties.resultConsumerName()),
				StreamOffset.create(properties.resultStreamKey(), ReadOffset.lastConsumed()),
				reservationResultConsumer);

		container.start();

		return container;
	}
}
