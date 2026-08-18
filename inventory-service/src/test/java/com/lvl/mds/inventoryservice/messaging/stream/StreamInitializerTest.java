package com.lvl.mds.inventoryservice.messaging.stream;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.messaging.StreamInitializer;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class StreamInitializerTest {

	private final RedisStreamProperties properties =
			new RedisStreamProperties("orders-stream", "orders-stream-dlq", "order-results-stream", "inventory-service-group", "consumer-1");

	/**
	 * Regression test: Spring Data Redis wraps the driver's BUSYGROUP error
	 * inside a {@link RedisSystemException} whose own message is a generic
	 * "Error in execution" - the literal "BUSYGROUP" text only appears on
	 * the cause. An earlier version of {@link StreamInitializer} checked
	 * only the outer message and re-threw on every restart once the group
	 * already existed.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void toleratesAlreadyExistingConsumerGroup() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();
		doThrow(new RedisSystemException("Error in execution",
				new InvalidDataAccessApiUsageException("BUSYGROUP Consumer Group name already exists")))
				.when(streamOps).createGroup(any(), any(ReadOffset.class), any());

		StreamInitializer initializer = new StreamInitializer(redisTemplate, properties);

		assertThatCode(initializer::createConsumerGroup).doesNotThrowAnyException();
	}
}
