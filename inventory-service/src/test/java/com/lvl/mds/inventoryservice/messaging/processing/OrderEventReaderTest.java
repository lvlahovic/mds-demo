package com.lvl.mds.inventoryservice.messaging.processing;

import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventReader;
import com.lvl.mds.inventoryservice.messaging.event.EventEnvelope;
import com.lvl.mds.inventoryservice.messaging.event.EventFixtures;
import com.lvl.mds.inventoryservice.messaging.event.OrderCreatedPayload;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The envelope contract lives in two independent Maven builds, so these tests
 * work from literal JSON rather than from this service's own records - a
 * fixture that stopped matching what order-service writes would still
 * round-trip through the local classes and prove nothing.
 */
class OrderEventReaderTest {

	private final OrderEventReader reader = new OrderEventReader(EventFixtures.MAPPER);

	private static final String WIRE_FORMAT = """
			{"eventId":"11111111-2222-3333-4444-555555555555",
			 "eventType":"order.created",
			 "schemaVersion":1,
			 "occurredAt":"2026-08-18T09:14:22.481Z",
			 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2}}
			""";

	@Test
	void readsTheWireFormatOrderServiceProduces() {
		EventEnvelope<OrderCreatedPayload> event = reader.read(EventFixtures.entry(WIRE_FORMAT));

		assertThat(event.eventId()).isEqualTo("11111111-2222-3333-4444-555555555555");
		assertThat(event.eventType()).isEqualTo("order.created");
		assertThat(event.schemaVersion()).isEqualTo(1);
		assertThat(event.occurredAt()).isNotNull();
		assertThat(event.payload()).isEqualTo(new OrderCreatedPayload("order-1", "item-1", 2));
	}

	/**
	 * Tolerant reader: a newer producer adding fields must not break an older
	 * consumer - that is the whole point of not bumping the version for
	 * additive changes.
	 */
	@Test
	void ignoresFieldsItDoesNotKnow() {
		String withExtras = """
				{"eventId":"e-1","eventType":"order.created","schemaVersion":1,
				 "occurredAt":"2026-08-18T09:14:22.481Z","correlationId":"c-9",
				 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2,"priority":"HIGH"}}
				""";

		EventEnvelope<OrderCreatedPayload> event = reader.read(EventFixtures.entry(withExtras));

		assertThat(event.payload().orderId()).isEqualTo("order-1");
	}

	@Test
	void rejectsASchemaVersionItCannotRead() {
		String v2 = """
				{"eventId":"e-1","eventType":"order.created","schemaVersion":2,
				 "occurredAt":"2026-08-18T09:14:22.481Z",
				 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2}}
				""";

		assertThatThrownBy(() -> reader.read(EventFixtures.entry(v2)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Unsupported schemaVersion 2");
	}

	@Test
	void rejectsAnEventTypeItDoesNotHandle() {
		String otherType = """
				{"eventId":"e-1","eventType":"order.cancelled","schemaVersion":1,
				 "occurredAt":"2026-08-18T09:14:22.481Z",
				 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2}}
				""";

		assertThatThrownBy(() -> reader.read(EventFixtures.entry(otherType)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("order.cancelled");
	}

	@Test
	void rejectsAnEntryWithoutTheEnvelopeField() {
		MapRecord<String, String, String> withoutEnvelope =
				MapRecord.create("orders-stream", Map.of("orderId", "order-1"));

		assertThatThrownBy(() -> reader.read(withoutEnvelope))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("has no 'event' field");
	}

	/**
	 * The DLQ path must still publish a failure even when the event it is
	 * giving up on is exactly what could not be decoded.
	 */
	@Test
	void bestEffortReadGivesUpQuietlyOnGarbage() {
		assertThat(reader.tryReadPayload(EventFixtures.entry("not json at all"))).isEmpty();
		assertThat(reader.tryReadPayload(EventFixtures.entry(WIRE_FORMAT)))
				.contains(new OrderCreatedPayload("order-1", "item-1", 2));
	}

	/**
	 * A message dead-lettered because its schema version is unsupported must
	 * still be nameable in the failure report - order-service needs the
	 * orderId to move it out of PUBLISHED, and {@link OrderEventReader#read}
	 * would reject exactly this record for the same reason it is being
	 * dead-lettered.
	 */
	@Test
	void bestEffortReadStillNamesTheOrderForAnUnsupportedSchemaVersion() {
		String v2 = """
				{"eventId":"e-1","eventType":"order.created","schemaVersion":2,
				 "occurredAt":"2026-08-18T09:14:22.481Z",
				 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2}}
				""";

		assertThat(reader.tryReadPayload(EventFixtures.entry(v2)))
				.contains(new OrderCreatedPayload("order-1", "item-1", 2));
	}
}
