package com.lvl.mds.inventoryservice.config;

import com.lvl.mds.inventoryservice.messaging.OrderEventConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
 */
@Configuration
public class RedisStreamListenerConfig {

	@Bean(destroyMethod = "stop")
	public StreamMessageListenerContainer<String, MapRecord<String, String, String>> orderStreamListenerContainer(
			RedisConnectionFactory connectionFactory,
			OrderEventConsumer orderEventConsumer,
			RedisStreamProperties properties) {

		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofSeconds(1))
						.build();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				StreamMessageListenerContainer.create(connectionFactory, options);

		container.receive(
				Consumer.from(properties.consumerGroup(), properties.consumerName()),
				StreamOffset.create(properties.streamKey(), ReadOffset.lastConsumed()),
				orderEventConsumer);

		container.start();

		return container;
	}
}
