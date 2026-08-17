package com.lvl.mds.inventoryservice.repository;

import com.lvl.mds.inventoryservice.model.InventoryItem;
import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stock storage. No database is required for this exercise - a
 * {@link ConcurrentHashMap} of item id to {@link InventoryItem} is the whole
 * "persistence" layer, seeded once at startup.
 */
@Repository
public class InventoryRepository {

	private final Map<String, InventoryItem> items = new ConcurrentHashMap<>();

	public void seed(String itemId, int quantity) {
		items.put(itemId, new InventoryItem(itemId, quantity));
	}

	/**
	 * Atomically checks and decrements stock for {@code itemId}. Synchronizes
	 * on the item itself so concurrent reservations for the same item can't
	 * both pass a stale availability check, while reservations for different
	 * items don't contend with each other.
	 */
	public ReservationOutcome reserve(String itemId, int quantity) {
		InventoryItem item = items.get(itemId);
		if (item == null) {
			return ReservationOutcome.ITEM_NOT_FOUND;
		}
		synchronized (item) {
			if (item.getAvailableQuantity() < quantity) {
				return ReservationOutcome.INSUFFICIENT_STOCK;
			}
			item.setAvailableQuantity(item.getAvailableQuantity() - quantity);
			return ReservationOutcome.RESERVED;
		}
	}

	public int availableQuantity(String itemId) {
		InventoryItem item = items.get(itemId);
		return item == null ? 0 : item.getAvailableQuantity();
	}
}
