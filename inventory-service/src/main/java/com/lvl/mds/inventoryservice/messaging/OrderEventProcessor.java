package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import com.lvl.mds.inventoryservice.services.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Order-event handling shared by the live consumer ({@link OrderEventConsumer})
 * and the reclaim job ({@link PendingMessagesReclaimer}), so both take the
 * same idempotency-checked path to {@link InventoryService}. Neither
 * acknowledges here - each caller owns its own ack/XCLAIM lifecycle.
 *
 * <p>Step order matters and is deliberate: reserve, then record the outcome
 * locally, then publish the result, and only then does the caller
 * {@code XACK}. If the result {@code XADD} fails, the entry stays in the PEL
 * and the redelivery finds the recorded outcome - so it re-publishes the
 * result instead of reserving a second time. The one window that stays open
 * is process death between the reserve and the local record; closing it
 * would need a transactional store, which the task explicitly scopes out.
 */
@Component
public class OrderEventProcessor {

	private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);

	private final InventoryService inventoryService;
	private final ProcessedOrdersStore processedOrdersStore;
	private final ReservationResultPublisher resultPublisher;

	public OrderEventProcessor(InventoryService inventoryService,
			ProcessedOrdersStore processedOrdersStore,
			ReservationResultPublisher resultPublisher) {
		this.inventoryService = inventoryService;
		this.processedOrdersStore = processedOrdersStore;
		this.resultPublisher = resultPublisher;
	}

	public void process(MapRecord<String, String, String> record) {
		Map<String, String> fields = record.getValue();
		String orderId = fields.get("orderId");
		String itemId = fields.get("itemId");
		int quantity = Integer.parseInt(fields.get("quantity"));

		if (orderId != null) {
			Optional<ReservationOutcome> previousOutcome = processedOrdersStore.findOutcome(orderId);
			if (previousOutcome.isPresent()) {
				log.info("Order {} already processed as {} - not reserving again, re-publishing the result (streamId={})",
						orderId, previousOutcome.get(), record.getId());
				resultPublisher.publishOutcome(orderId, itemId, quantity, previousOutcome.get());
				return;
			}
		}

		ReservationOutcome outcome = inventoryService.reserve(orderId, itemId, quantity);

		if (orderId != null) {
			processedOrdersStore.markProcessed(orderId, outcome);
		}

		resultPublisher.publishOutcome(orderId, itemId, quantity, outcome);
	}
}
