package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ReservationResultPublisherTest {

	private static final RedisStreamProperties PROPS = new RedisStreamProperties(
			"orders-stream", "orders-stream-dlq", "order-results-stream", "inventory-service-group", "consumer-1");

	@SuppressWarnings("unchecked")
	private final StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);

	private ReservationResultPublisher newPublisher() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();
		return new ReservationResultPublisher(redisTemplate, PROPS);
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> capturePublishedFields() {
		ArgumentCaptor<MapRecord<String, String, String>> captor = ArgumentCaptor.forClass(MapRecord.class);
		verify(streamOps).add(captor.capture());
		assertThat(captor.getValue().getStream()).isEqualTo("order-results-stream");
		return captor.getValue().getValue();
	}

	@Test
	void publishesTheOutcomeOntoTheResultStream() {
		newPublisher().publishOutcome("order-1", "item-3", 999, ReservationOutcome.INSUFFICIENT_STOCK);

		assertThat(capturePublishedFields())
				.containsEntry("orderId", "order-1")
				.containsEntry("itemId", "item-3")
				.containsEntry("quantity", "999")
				.containsEntry("outcome", "INSUFFICIENT_STOCK")
				.containsKeys("reason", "processedAt");
	}

	/**
	 * The DLQ path reports on a record whose payload may be exactly what
	 * couldn't be handled, so it is read raw rather than parsed.
	 */
	@Test
	void publishesFailureFromTheRawRecord() {
		MapRecord<String, String, String> failed = MapRecord.create("orders-stream",
				Map.of("orderId", "order-2", "itemId", "item-1", "quantity", "not-a-number"));

		newPublisher().publishFailure(failed, "exceeded max delivery attempts (3)");

		assertThat(capturePublishedFields())
				.containsEntry("orderId", "order-2")
				.containsEntry("quantity", "not-a-number")
				.containsEntry("outcome", "FAILED")
				.containsEntry("reason", "exceeded max delivery attempts (3)");
	}
}
