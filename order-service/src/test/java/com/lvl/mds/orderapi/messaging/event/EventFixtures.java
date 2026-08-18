package com.lvl.mds.orderapi.messaging.event;

import org.springframework.data.redis.connection.stream.MapRecord;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * Builds stream entries in the wire format inventory-service actually
 * produces, so the tests exercise the same decoding path as production
 * instead of hand-assembling fields the consumer would never see. Public and
 * kept in the {@code event} test package alongside the contract types it
 * builds, since both the {@code stream} and {@code processing} test packages
 * need it.
 */
public final class EventFixtures {

	public static final ObjectMapper MAPPER = JsonMapper.builder().build();

	public static MapRecord<String, String, String> reservationResult(String orderId, String outcome, String reason) {
		EventEnvelope<ReservationResultPayload> event = EventEnvelope.of(ReservationResultPayload.EVENT_TYPE,
				new ReservationResultPayload(orderId, "item-1", 2, outcome, reason));

		return entry(MAPPER.writeValueAsString(event));
	}

	public static MapRecord<String, String, String> entry(String eventJson) {
		return MapRecord.create("order-results-stream", Map.of(EventEnvelope.STREAM_FIELD, eventJson));
	}

	private EventFixtures() {
	}
}
