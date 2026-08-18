package com.lvl.mds.inventoryservice.messaging.processing;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventReader;
import com.lvl.mds.inventoryservice.messaging.event.EventEnvelope;
import com.lvl.mds.inventoryservice.messaging.event.EventFixtures;
import com.lvl.mds.inventoryservice.messaging.event.ReservationResultPayload;
import com.lvl.mds.inventoryservice.messaging.producers.ReservationResultPublisher;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
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

class ReservationResultPublisherTest {

	private static final RedisStreamProperties PROPS = new RedisStreamProperties(
			"orders-stream", "orders-stream-dlq", "order-results-stream", "inventory-service-group", "consumer-1");

	private static final TypeReference<EventEnvelope<ReservationResultPayload>> RESULT_EVENT =
			new TypeReference<>() {
			};

	@SuppressWarnings("unchecked")
	private final StreamOperations<String, String, String> streamOps = mock(StreamOperations.class);

	private ReservationResultPublisher newPublisher() {
		StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
		doReturn(streamOps).when(redisTemplate).opsForStream();
		return new ReservationResultPublisher(redisTemplate, new OrderEventReader(EventFixtures.MAPPER),
				EventFixtures.MAPPER, PROPS);
	}

	@SuppressWarnings("unchecked")
	private EventEnvelope<ReservationResultPayload> capturePublishedEvent() {
		ArgumentCaptor<MapRecord<String, String, String>> captor = ArgumentCaptor.forClass(MapRecord.class);
		verify(streamOps).add(captor.capture());
		assertThat(captor.getValue().getStream()).isEqualTo("order-results-stream");

		Map<String, String> fields = captor.getValue().getValue();
		assertThat(fields).containsOnlyKeys(EventEnvelope.STREAM_FIELD);

		return EventFixtures.MAPPER.readValue(fields.get(EventEnvelope.STREAM_FIELD), RESULT_EVENT);
	}

	@Test
	void publishesTheOutcomeInsideAnEnvelope() {
		newPublisher().publishOutcome("order-1", "item-3", 999, ReservationOutcome.INSUFFICIENT_STOCK);

		EventEnvelope<ReservationResultPayload> event = capturePublishedEvent();

		assertThat(event.eventType()).isEqualTo("order.reservation-completed");
		assertThat(event.schemaVersion()).isEqualTo(EventEnvelope.SCHEMA_VERSION);
		assertThat(event.eventId()).isNotBlank();
		assertThat(event.occurredAt()).isNotNull();
		assertThat(event.payload()).satisfies(payload -> {
			assertThat(payload.orderId()).isEqualTo("order-1");
			assertThat(payload.itemId()).isEqualTo("item-3");
			assertThat(payload.quantity()).isEqualTo(999);
			assertThat(payload.outcome()).isEqualTo("INSUFFICIENT_STOCK");
			assertThat(payload.reason()).contains("insufficient stock");
		});
	}

	@Test
	void publishesFailureForADeadLetteredOrder() {
		MapRecord<String, String, String> failed = EventFixtures.orderCreated("order-2", "item-1", 5);

		newPublisher().publishFailure(failed, "exceeded max delivery attempts (3)");

		assertThat(capturePublishedEvent().payload()).satisfies(payload -> {
			assertThat(payload.orderId()).isEqualTo("order-2");
			assertThat(payload.quantity()).isEqualTo(5);
			assertThat(payload.outcome()).isEqualTo("FAILED");
			assertThat(payload.reason()).isEqualTo("exceeded max delivery attempts (3)");
		});
	}

	/**
	 * An undecodable event is exactly the kind that gets dead-lettered, and
	 * the failure still has to be published - even though nothing in it can
	 * name the order, which is the price of keeping the whole event in one
	 * field.
	 */
	@Test
	void publishesFailureEvenWhenTheOrderCannotBeDecoded() {
		newPublisher().publishFailure(EventFixtures.entry("{not-an-event"), "exceeded max delivery attempts (3)");

		assertThat(capturePublishedEvent().payload()).satisfies(payload -> {
			assertThat(payload.orderId()).isNull();
			assertThat(payload.outcome()).isEqualTo("FAILED");
			assertThat(payload.reason()).isEqualTo("exceeded max delivery attempts (3)");
		});
	}
}
