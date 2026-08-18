package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import com.lvl.mds.inventoryservice.services.InventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventProcessorTest {

	@Mock
	private InventoryService inventoryService;

	@Mock
	private ReservationResultPublisher resultPublisher;

	private final ProcessedOrdersStore processedOrdersStore = new ProcessedOrdersStore();

	private final MapRecord<String, String, String> record = MapRecord.create("orders-stream",
			Map.of("orderId", "order-1", "itemId", "item-1", "quantity", "2"));

	private OrderEventProcessor newProcessor() {
		return new OrderEventProcessor(inventoryService, processedOrdersStore, resultPublisher);
	}

	@Test
	void reservesOnceAndPublishesTheOutcome() {
		when(inventoryService.reserve("order-1", "item-1", 2)).thenReturn(ReservationOutcome.RESERVED);

		newProcessor().process(record);

		verify(inventoryService).reserve("order-1", "item-1", 2);
		verify(resultPublisher).publishOutcome("order-1", "item-1", 2, ReservationOutcome.RESERVED);
	}

	@Test
	void skipsReprocessingADuplicateOrderId() {
		when(inventoryService.reserve("order-1", "item-1", 2)).thenReturn(ReservationOutcome.RESERVED);
		OrderEventProcessor processor = newProcessor();

		processor.process(record);
		processor.process(record);

		verify(inventoryService, times(1)).reserve("order-1", "item-1", 2);
	}

	/**
	 * A redelivery means the first attempt didn't finish cleanly, so
	 * order-service may never have seen the result. The reservation must not
	 * be repeated, but the answer must be - otherwise the order stays in
	 * {@code PUBLISHED} forever.
	 */
	@Test
	void republishesTheStoredOutcomeOnADuplicateDelivery() {
		when(inventoryService.reserve("order-1", "item-1", 2)).thenReturn(ReservationOutcome.INSUFFICIENT_STOCK);
		OrderEventProcessor processor = newProcessor();

		processor.process(record);
		processor.process(record);

		verify(resultPublisher, times(2))
				.publishOutcome("order-1", "item-1", 2, ReservationOutcome.INSUFFICIENT_STOCK);
	}
}
