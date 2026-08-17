package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.services.InventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderEventProcessorTest {

	@Mock
	private InventoryService inventoryService;

	private final ProcessedOrdersStore processedOrdersStore = new ProcessedOrdersStore();

	@Test
	void skipsReprocessingADuplicateOrderId() {
		OrderEventProcessor processor = new OrderEventProcessor(inventoryService, processedOrdersStore);
		MapRecord<String, String, String> record = MapRecord.create("orders-stream",
				Map.of("orderId", "order-1", "itemId", "item-1", "quantity", "2"));

		processor.process(record);
		processor.process(record);

		verify(inventoryService, times(1)).reserve("order-1", "item-1", 2);
	}
}
