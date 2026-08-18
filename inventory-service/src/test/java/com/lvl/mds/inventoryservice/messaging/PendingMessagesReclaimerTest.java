package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.config.RetryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Covers the retry-vs-DLQ branch in {@link PendingMessagesReclaimer} without
 * waiting out real {@code inventory.retry.*} delays end-to-end (that path
 * was already exercised live against docker compose).
 */
class PendingMessagesReclaimerTest {

	private static final RedisStreamProperties STREAM_PROPS =
			new RedisStreamProperties("orders-stream", "orders-stream-dlq", "order-results-stream", "inventory-service-group", "consumer-1");
	private static final RetryProperties RETRY_PROPS = new RetryProperties(30_000, 10_000, 3);

	@Test
	@SuppressWarnings("unchecked")
	void reclaimsAndReprocessesWithinMaxAttempts() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();
		OrderEventProcessor processor = mock(OrderEventProcessor.class);
		ReservationResultPublisher resultPublisher = mock(ReservationResultPublisher.class);

		RecordId id = RecordId.of("1-1");
		PendingMessage stale = new PendingMessage(id, Consumer.from("inventory-service-group", "ghost"),
				Duration.ofSeconds(40), 1);
		PendingMessages pending = new PendingMessages("inventory-service-group", List.of(stale));
		doReturn(pending).when(streamOps).pending(eq("orders-stream"), eq("inventory-service-group"), any(), eq(100L));

		MapRecord<String, String, String> record = MapRecord
				.create("orders-stream", Map.of("orderId", "order-x", "itemId", "item-1", "quantity", "1"))
				.withId(id);
		doReturn(List.of(record)).when(streamOps)
				.claim(eq("orders-stream"), eq("inventory-service-group"), eq("consumer-1"), any(Duration.class), eq(id));

		PendingMessagesReclaimer reclaimer = new PendingMessagesReclaimer(redisTemplate, processor, resultPublisher, STREAM_PROPS, RETRY_PROPS);
		reclaimer.reclaimStalePendingMessages();

		verify(processor).process(record);
		verify(streamOps).acknowledge("orders-stream", "inventory-service-group", id);
		verify(streamOps, never()).add(any(MapRecord.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	void movesToDlqWhenAttemptsExhausted() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();
		OrderEventProcessor processor = mock(OrderEventProcessor.class);
		ReservationResultPublisher resultPublisher = mock(ReservationResultPublisher.class);

		RecordId id = RecordId.of("2-1");
		// deliveryCount (4) > maxAttempts (3)
		PendingMessage stale = new PendingMessage(id, Consumer.from("inventory-service-group", "ghost"),
				Duration.ofSeconds(40), 4);
		PendingMessages pending = new PendingMessages("inventory-service-group", List.of(stale));
		doReturn(pending).when(streamOps).pending(eq("orders-stream"), eq("inventory-service-group"), any(), eq(100L));

		MapRecord<String, String, String> record = MapRecord
				.create("orders-stream", Map.of("orderId", "order-y", "itemId", "item-1", "quantity", "1"))
				.withId(id);
		doReturn(List.of(record)).when(streamOps)
				.claim(eq("orders-stream"), eq("inventory-service-group"), eq("consumer-1"), any(Duration.class), eq(id));

		PendingMessagesReclaimer reclaimer = new PendingMessagesReclaimer(redisTemplate, processor, resultPublisher, STREAM_PROPS, RETRY_PROPS);
		reclaimer.reclaimStalePendingMessages();

		verify(processor, never()).process(any());
		verify(streamOps).add(argThat((MapRecord<String, String, String> r) -> r.getStream().equals("orders-stream-dlq")));
		verify(resultPublisher).publishFailure(eq(record), argThat(reason -> reason.contains("max delivery attempts")));
		verify(streamOps).acknowledge("orders-stream", "inventory-service-group", id);
	}
}
