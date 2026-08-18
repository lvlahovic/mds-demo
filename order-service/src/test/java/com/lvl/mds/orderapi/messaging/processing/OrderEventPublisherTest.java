package com.lvl.mds.orderapi.messaging.processing;

import com.lvl.mds.orderapi.config.OrderStreamProperties;
import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.messaging.event.EventEnvelope;
import com.lvl.mds.orderapi.messaging.event.EventFixtures;
import com.lvl.mds.orderapi.messaging.event.OrderCreatedPayload;
import com.lvl.mds.orderapi.messaging.producers.OrderEventPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.core.type.TypeReference;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderEventPublisherTest {

	private static final OrderStreamProperties PROPS = new OrderStreamProperties(
			"orders-stream", "order-results-stream", "order-service-group", "order-service-1");

	private static final TypeReference<EventEnvelope<OrderCreatedPayload>> ORDER_EVENT =
			new TypeReference<>() {
			};

	@SuppressWarnings("unchecked")
	private final StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);

	@SuppressWarnings("unchecked")
	@Test
	void publishesTheOrderInsideAnEnvelope() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();

		new OrderEventPublisher(redisTemplate, EventFixtures.MAPPER, PROPS)
				.publish(new OrderRequestDto("order-1", "item-1", 2));

		ArgumentCaptor<MapRecord<String, String, String>> captor = ArgumentCaptor.forClass(MapRecord.class);
		verify(streamOps).add(captor.capture());
		assertThat(captor.getValue().getStream()).isEqualTo("orders-stream");

		Map<String, String> fields = captor.getValue().getValue();
		assertThat(fields).containsOnlyKeys(EventEnvelope.STREAM_FIELD);

		EventEnvelope<OrderCreatedPayload> event =
				EventFixtures.MAPPER.readValue(fields.get(EventEnvelope.STREAM_FIELD), ORDER_EVENT);

		assertThat(event.eventType()).isEqualTo("order.created");
		assertThat(event.schemaVersion()).isEqualTo(EventEnvelope.SCHEMA_VERSION);
		assertThat(event.eventId()).isNotBlank();
		assertThat(event.occurredAt()).isNotNull();
		assertThat(event.payload()).isEqualTo(new OrderCreatedPayload("order-1", "item-1", 2));
	}

	/**
	 * {@code occurredAt} has to travel as a string both services can read, not
	 * as a numeric timestamp - this is the one place the two independently
	 * configured ObjectMappers have to agree on more than field names.
	 */
	@Test
	void writesTheTimestampInIsoForm() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();

		new OrderEventPublisher(redisTemplate, EventFixtures.MAPPER, PROPS)
				.publish(new OrderRequestDto("order-1", "item-1", 2));

		@SuppressWarnings("unchecked")
		ArgumentCaptor<MapRecord<String, String, String>> captor = ArgumentCaptor.forClass(MapRecord.class);
		verify(streamOps).add(captor.capture());

		assertThat(captor.getValue().getValue().get(EventEnvelope.STREAM_FIELD))
				// a quoted value, not a numeric timestamp
				.contains("\"occurredAt\":\"");
	}
}
