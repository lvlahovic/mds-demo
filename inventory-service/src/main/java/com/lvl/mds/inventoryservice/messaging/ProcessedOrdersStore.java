package com.lvl.mds.inventoryservice.messaging;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which orderIds have already been reserved so a redelivered or
 * duplicated stream entry doesn't get reserved twice. In-memory only, as
 * allowed by the task - it resets on restart, same as the inventory itself.
 */
@Component
public class ProcessedOrdersStore {

	private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();

	public boolean alreadyProcessed(String orderId) {
		return processedOrderIds.contains(orderId);
	}

	public void markProcessed(String orderId) {
		processedOrderIds.add(orderId);
	}
}
