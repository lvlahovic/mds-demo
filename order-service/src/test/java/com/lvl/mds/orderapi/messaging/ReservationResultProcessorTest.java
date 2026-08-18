package com.lvl.mds.orderapi.messaging;

import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class ReservationResultProcessorTest {

	@Mock
	private OrderService orderService;

	private MapRecord<String, String, String> resultRecord(String outcome, String reason) {
		Map<String, String> fields = new HashMap<>();
		fields.put("orderId", "order-1");
		fields.put("itemId", "item-1");
		fields.put("quantity", "2");
		fields.put("outcome", outcome);
		if (reason != null) {
			fields.put("reason", reason);
		}
		return MapRecord.create("order-results-stream", fields);
	}

	@Test
	void mapsReservedToReserved() {
		new ReservationResultProcessor(orderService).process(resultRecord("RESERVED", "reserved 2 of item 'item-1'"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.RESERVED, "reserved 2 of item 'item-1'");
	}

	@Test
	void mapsInsufficientStockToRejection() {
		new ReservationResultProcessor(orderService).process(resultRecord("INSUFFICIENT_STOCK", "not enough"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.REJECTED_INSUFFICIENT_STOCK, "not enough");
	}

	@Test
	void mapsUnknownItemToRejection() {
		new ReservationResultProcessor(orderService).process(resultRecord("ITEM_NOT_FOUND", "unknown item 'item-9'"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.REJECTED_UNKNOWN_ITEM, "unknown item 'item-9'");
	}

	@Test
	void mapsDeadLetteredOrderToFailed() {
		new ReservationResultProcessor(orderService).process(resultRecord("FAILED", "exceeded max delivery attempts (3)"));

		verify(orderService).applyReservationResult("order-1", OrderStatus.FAILED, "exceeded max delivery attempts (3)");
	}

	/**
	 * An outcome added by a newer inventory-service must not leave the order
	 * hanging in PUBLISHED forever - it is not a reservation, so FAILED is the
	 * honest reading, with the unknown value kept in the reason.
	 */
	@Test
	void treatsAnUnrecognizedOutcomeAsFailed() {
		new ReservationResultProcessor(orderService).process(resultRecord("PARTIALLY_RESERVED", null));

		verify(orderService).applyReservationResult("order-1", OrderStatus.FAILED, "reported as 'PARTIALLY_RESERVED'");
	}

	@Test
	void discardsAResultWithoutAnOrderId() {
		Map<String, String> fields = new HashMap<>();
		fields.put("outcome", "RESERVED");

		new ReservationResultProcessor(orderService).process(MapRecord.create("order-results-stream", fields));

		verifyNoInteractions(orderService);
	}
}
