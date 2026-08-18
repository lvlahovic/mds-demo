package com.lvl.mds.inventoryservice.messaging;

import com.lvl.mds.inventoryservice.messaging.consumers.OrderEventProcessor;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remembers which orderIds have already been reserved, and what the outcome
 * was, so a redelivered or duplicated stream entry doesn't get reserved
 * twice. In-memory only, as allowed by the task - it resets on restart, same
 * as the inventory itself.
 *
 * <p>The outcome is kept (rather than just the id) because the result stream
 * made a bare "seen it, skip it" answer wrong: a redelivery happens exactly
 * when the first attempt didn't finish cleanly, which is also the case where
 * order-service may never have received the result. Keeping the decision lets
 * {@link OrderEventProcessor} re-emit the original outcome instead of leaving
 * the order stuck in {@code PUBLISHED} forever.
 */
@Component
public class ProcessedOrdersStore {

	private final Map<String, ReservationOutcome> outcomesByOrderId = new ConcurrentHashMap<>();

	public Optional<ReservationOutcome> findOutcome(String orderId) {
		return Optional.ofNullable(outcomesByOrderId.get(orderId));
	}

	public void markProcessed(String orderId, ReservationOutcome outcome) {
		outcomesByOrderId.put(orderId, outcome);
	}
}
