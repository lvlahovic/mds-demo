package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.config.RedisStreamProperties;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

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
	private final String resultStreamKey;

	public ReservationResultPublisher(StringRedisTemplate redisTemplate, RedisStreamProperties properties) {
		this.streamOps = redisTemplate.opsForStream();
		this.resultStreamKey = properties.resultStreamKey();
	}

	public RecordId publishOutcome(String orderId, String itemId, int quantity, ReservationOutcome outcome) {
		return publish(orderId, itemId, String.valueOf(quantity), outcome.name(), describe(outcome, itemId, quantity));
	}

	/**
	 * Reports an order this service has permanently given up on (moved to the
	 * DLQ). Takes the raw stream record rather than parsed values, because the
	 * reason a message ends up here can be that its own payload never parsed.
	 */
	public RecordId publishFailure(MapRecord<String, String, String> failedRecord, String reason) {
		Map<String, String> fields = failedRecord.getValue();
		return publish(fields.get("orderId"), fields.get("itemId"), fields.get("quantity"), FAILED_OUTCOME, reason);
	}

	private RecordId publish(String orderId, String itemId, String quantity, String outcome, String reason) {
		Map<String, String> fields = new LinkedHashMap<>();
		fields.put("orderId", orderId == null ? "" : orderId);
		fields.put("itemId", itemId == null ? "" : itemId);
		fields.put("quantity", quantity == null ? "" : quantity);
		fields.put("outcome", outcome);
		fields.put("reason", reason);
		fields.put("processedAt", Instant.now().toString());

		RecordId recordId = streamOps.add(MapRecord.create(resultStreamKey, fields));

		log.info("Published reservation result to stream '{}': orderId={}, outcome={}, streamId={}",
				resultStreamKey, orderId, outcome, recordId);

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
