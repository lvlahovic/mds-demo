package com.lvl.mds.inventoryservice.model;

/**
 * Result of attempting to reserve stock for an order.
 */
public enum ReservationOutcome {
	RESERVED,
	INSUFFICIENT_STOCK,
	ITEM_NOT_FOUND
}
