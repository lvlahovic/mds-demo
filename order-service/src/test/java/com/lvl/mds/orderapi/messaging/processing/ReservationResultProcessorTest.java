package com.lvl.mds.orderapi.messaging.processing;

import com.lvl.mds.orderapi.messaging.consumers.ReservationResultProcessor;
import com.lvl.mds.orderapi.messaging.event.EventFixtures;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class ReservationResultProcessorTest {

	@Mock
	private OrderService orderService;

	private ReservationResultProcessor newProcessor() {
		return new ReservationResultProcessor(orderService, EventFixtures.MAPPER);
	}

	private MapRecord<String, String, String> resultRecord(String outcome, String reason) {
		return EventFixtures.reservationResult("order-1", outcome, reason);
	}

	/**
	 * The contract lives in two independent builds, so this one works from
	 * literal JSON - a fixture that drifted from what inventory-service writes
	 * would still round-trip through this service's own records and prove
	 * nothing.
	 */
	@Test
	void readsTheWireFormatInventoryServiceProduces() {
		String wireFormat = """
				{"eventId":"11111111-2222-3333-4444-555555555555",
				 "eventType":"order.reservation-completed",
				 "schemaVersion":1,
				 "occurredAt":"2026-08-18T09:14:22.481Z",
				 "payload":{"orderId":"order-1","itemId":"item-1","quantity":2,
				            "outcome":"RESERVED","reason":"reserved 2 of item 'item-1'"}}
				""";

		newProcessor().process(EventFixtures.entry(wireFormat));

		verify(orderService).applyReservationResult("order-1", OrderStatus.RESERVED, "reserved 2 of item 'item-1'");
	}

	@Test
	void mapsReservedToReserved() {
		newProcessor().process(resultRecord("RESERVED", "reserved 2 of item 'item-1'"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.RESERVED, "reserved 2 of item 'item-1'");
	}

	@Test
	void mapsInsufficientStockToRejection() {
		newProcessor().process(resultRecord("INSUFFICIENT_STOCK", "not enough"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.REJECTED_INSUFFICIENT_STOCK, "not enough");
	}

	@Test
	void mapsUnknownItemToRejection() {
		newProcessor().process(resultRecord("ITEM_NOT_FOUND", "unknown item 'item-9'"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.REJECTED_UNKNOWN_ITEM, "unknown item 'item-9'");
	}

	@Test
	void mapsDeadLetteredOrderToFailed() {
		newProcessor().process(resultRecord("FAILED", "exceeded max delivery attempts (3)"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.FAILED, "exceeded max delivery attempts (3)");
	}

	/**
	 * An outcome added by a newer inventory-service must not leave the order
	 * hanging in PUBLISHED forever - it is not a reservation, so FAILED is the
	 * honest reading, with the unknown value kept in the reason. The outcome
	 * vocabulary is allowed to grow within a version; the schema itself is not.
	 */
	@Test
	void treatsAnUnrecognizedOutcomeAsFailed() {
		newProcessor().process(resultRecord("PARTIALLY_RESERVED", null));

		verify(orderService).applyReservationResult("order-1", OrderStatus.FAILED, "reported as 'PARTIALLY_RESERVED'");
	}

	/**
	 * Tolerant reader: fields a newer producer added are skipped rather than
	 * treated as an error.
	 */
	@Test
	void ignoresFieldsItDoesNotKnow() {
		String withExtras = """
				{"eventId":"e-1","eventType":"order.reservation-completed","schemaVersion":1,
				 "occurredAt":"2026-08-18T09:14:22.481Z","correlationId":"c-9",
				 "payload":{"orderId":"order-1","outcome":"RESERVED","reason":"reserved","warehouse":"WH-2"}}
				""";

		newProcessor().process(EventFixtures.entry(withExtras));

		verify(orderService).applyReservationResult("order-1", OrderStatus.RESERVED, "reserved");
	}

	/**
	 * Unlike an unknown outcome, a schema this build cannot read is not an
	 * answer it may guess at - it throws, the consumer leaves the entry
	 * unacknowledged, and the operator sees it in the PEL.
	 */
	@Test
	void rejectsASchemaVersionItCannotRead() {
		String v2 = """
				{"eventId":"e-1","eventType":"order.reservation-completed","schemaVersion":2,
				 "occurredAt":"2026-08-18T09:14:22.481Z",
				 "payload":{"orderId":"order-1","outcome":"RESERVED"}}
				""";

		assertThatThrownBy(() -> newProcessor().process(EventFixtures.entry(v2)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Unsupported schemaVersion 2");
		verifyNoInteractions(orderService);
	}

	@Test
	void discardsAResultWithoutAnOrderId() {
		newProcessor().process(EventFixtures.reservationResult(null, "RESERVED", "reserved"));

		verifyNoInteractions(orderService);
	}

	@Test
	void discardsAnEntryWithoutTheEnvelopeField() {
		newProcessor().process(MapRecord.create("order-results-stream", Map.of("outcome", "RESERVED")));

		verifyNoInteractions(orderService);
	}
}
