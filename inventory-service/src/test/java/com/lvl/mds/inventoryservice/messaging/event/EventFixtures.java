package com.lvl.mds.inventoryservice.messaging.event;

import org.springframework.data.redis.connection.stream.MapRecord;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * Builds stream entries in the wire format order-service actually produces,
 * so the tests exercise the same decoding path as production instead of
 * hand-assembling fields the reader would never see. Public and kept in the
 * {@code event} test package alongside the contract types it builds, since
 * both the {@code stream} and {@code processing} test packages need it.
 */
public final class EventFixtures {

	public static final ObjectMapper MAPPER = JsonMapper.builder().build();

	public static MapRecord<String, String, String> orderCreated(String orderId, String itemId, int quantity) {
		EventEnvelope<OrderCreatedPayload> event = EventEnvelope.of(OrderCreatedPayload.EVENT_TYPE,
				new OrderCreatedPayload(orderId, itemId, quantity));

		return entry(MAPPER.writeValueAsString(event));
	}

	public static MapRecord<String, String, String> entry(String eventJson) {
		return MapRecord.create("orders-stream", Map.of(EventEnvelope.STREAM_FIELD, eventJson));
	}

	private EventFixtures() {
	}
}
