package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.services.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Order-event handling shared by the live consumer ({@link OrderEventConsumer})
 * and the reclaim job ({@link PendingMessagesReclaimer}), so both take the
 * same idempotency-checked path to {@link InventoryService}. Neither
 * acknowledges here - each caller owns its own ack/XCLAIM lifecycle.
 */
@Component
public class OrderEventProcessor {

	private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);

	private final InventoryService inventoryService;
	private final ProcessedOrdersStore processedOrdersStore;

	public OrderEventProcessor(InventoryService inventoryService, ProcessedOrdersStore processedOrdersStore) {
		this.inventoryService = inventoryService;
		this.processedOrdersStore = processedOrdersStore;
	}

	public void process(MapRecord<String, String, String> record) {
		Map<String, String> fields = record.getValue();
		String orderId = fields.get("orderId");

		if (orderId != null && processedOrdersStore.alreadyProcessed(orderId)) {
			log.info("Order {} already processed - skipping duplicate delivery (streamId={})", orderId, record.getId());
			return;
		}

		String itemId = fields.get("itemId");
		int quantity = Integer.parseInt(fields.get("quantity"));

		inventoryService.reserve(orderId, itemId, quantity);

		if (orderId != null) {
			processedOrdersStore.markProcessed(orderId);
		}
	}
}
