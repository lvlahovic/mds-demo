package com.lvl.mds.inventoryservice.services;

import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import com.lvl.mds.inventoryservice.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Simulates stock reservation: enough quantity on hand -> reserved,
 * otherwise rejected. No external calls, no persistence beyond the
 * in-memory repository - the task explicitly scopes evaluation to the
 * messaging integration, not this business logic.
 */
@Service
public class InventoryService {

	private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

	private final InventoryRepository inventoryRepository;

	public InventoryService(InventoryRepository inventoryRepository) {
		this.inventoryRepository = inventoryRepository;
	}

	public ReservationOutcome reserve(String orderId, String itemId, int quantity) {
		ReservationOutcome outcome = inventoryRepository.reserve(itemId, quantity);

		switch (outcome) {
			case RESERVED -> log.info("Order {}: reserved {} of item '{}'", orderId, quantity, itemId);
			case INSUFFICIENT_STOCK -> log.warn("Order {}: rejected, insufficient stock for item '{}' (requested {}, available {})",
					orderId, itemId, quantity, inventoryRepository.availableQuantity(itemId));
			case ITEM_NOT_FOUND -> log.warn("Order {}: rejected, unknown item '{}'", orderId, itemId);
		}

		return outcome;
	}
}
