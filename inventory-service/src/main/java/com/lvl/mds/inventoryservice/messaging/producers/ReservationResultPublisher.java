package com.lvl.mds.inventoryservice.messaging.producers;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.messaging.event.EventEnvelope;
import com.lvl.mds.inventoryservice.messaging.event.OrderCreatedPayload;
import com.lvl.mds.inventoryservice.messaging.event.ReservationResultPayload;
import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventReader;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Optional;

/**
 * Publishes the outcome of every reservation attempt to
 * {@code order-results-stream} - the return leg of the integration, so
 * {@code order-service} can move an order out of {@code PUBLISHED} into a
 * real terminal state.
 *
 * <p>A second stream in the opposite direction was chosen over an HTTP
 * callback to inventory's caller (which would re-introduce exactly the
 * synchronous coupling the broker is there to remove, and would need its own
 * retry story) and over a shared Redis key polled by order-service (no
 * ordering, no redelivery, no backlog if the reader is down).
 *
 * <p>Results are wrapped in the same {@link EventEnvelope} as inbound orders:
 * both legs of the integration speak one message format, and the return leg
 * gets versioning for free.
 *
 * <p>The wire vocabulary is deliberately a superset of
 * {@link ReservationOutcome}: the domain enum answers "did we reserve?",
 * while the event contract also has to be able to say "we never got as far
 * as deciding" - {@value #FAILED_OUTCOME}, emitted when a message is given
 * up on and moved to the DLQ.
 */
@Component
public class ReservationResultPublisher {

	static final String FAILED_OUTCOME = "FAILED";

	private static final Logger log = LoggerFactory.getLogger(ReservationResultPublisher.class);

	private final StreamOperations<String, String, String> streamOps;
	private final OrderEventReader eventReader;
	private final ObjectMapper objectMapper;
	private final String resultStreamKey;

	public ReservationResultPublisher(StringRedisTemplate redisTemplate,
			OrderEventReader eventReader,
			ObjectMapper objectMapper,
			RedisStreamProperties properties) {
		this.streamOps = redisTemplate.opsForStream();
		this.eventReader = eventReader;
		this.objectMapper = objectMapper;
		this.resultStreamKey = properties.resultStreamKey();
	}

	public RecordId publishOutcome(String orderId, String itemId, int quantity, ReservationOutcome outcome) {
		return publish(new ReservationResultPayload(orderId, itemId, quantity, outcome.name(),
				describe(outcome, itemId, quantity)));
	}

	/**
	 * Reports an order this service has permanently given up on (moved to the
	 * DLQ). Takes the raw stream record rather than parsed values, because the
	 * reason a message ends up here can be that its own payload never
	 * decoded - in which case the failure is still published, just without an
	 * order to name.
	 */
	public RecordId publishFailure(MapRecord<String, String, String> failedRecord, String reason) {
		Optional<OrderCreatedPayload> order = eventReader.tryReadPayload(failedRecord);

		return publish(new ReservationResultPayload(
				order.map(OrderCreatedPayload::orderId).orElse(null),
				order.map(OrderCreatedPayload::itemId).orElse(null),
				order.map(OrderCreatedPayload::quantity).orElse(null),
				FAILED_OUTCOME,
				reason));
	}

	private RecordId publish(ReservationResultPayload payload) {
		EventEnvelope<ReservationResultPayload> event =
				EventEnvelope.of(ReservationResultPayload.EVENT_TYPE, payload);

		Map<String, String> fields = Map.of(EventEnvelope.STREAM_FIELD, objectMapper.writeValueAsString(event));
		RecordId recordId = streamOps.add(MapRecord.create(resultStreamKey, fields));

		log.info("Published {} v{} to stream '{}': eventId={}, orderId={}, outcome={}, streamId={}",
				event.eventType(), event.schemaVersion(), resultStreamKey, event.eventId(),
				payload.orderId(), payload.outcome(), recordId);

		return recordId;
	}

	private String describe(ReservationOutcome outcome, String itemId, int quantity) {
		return switch (outcome) {
			case RESERVED -> "reserved " + quantity + " of item '" + itemId + "'";
			case INSUFFICIENT_STOCK -> "insufficient stock for item '" + itemId + "' (requested " + quantity + ")";
			case ITEM_NOT_FOUND -> "unknown item '" + itemId + "'";
		};
	}
}
