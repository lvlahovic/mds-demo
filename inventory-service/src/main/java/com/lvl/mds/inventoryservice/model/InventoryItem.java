package com.lvl.mds.inventoryservice.model;

/**
 * In-memory stock entry for a single item. Mutable and guarded by the
 * repository holding it - not thread-safe on its own.
 */
public class InventoryItem {

	private final String itemId;
	private int availableQuantity;

	public InventoryItem(String itemId, int availableQuantity) {
		this.itemId = itemId;
		this.availableQuantity = availableQuantity;
	}

	public String getItemId() {
		return itemId;
	}

	public int getAvailableQuantity() {
		return availableQuantity;
	}

	public void setAvailableQuantity(int availableQuantity) {
		this.availableQuantity = availableQuantity;
	}
}
