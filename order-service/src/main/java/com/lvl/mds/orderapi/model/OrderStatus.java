package com.lvl.mds.orderapi.model;

/**
 * Lifecycle of an order as tracked by order-service.
 *
 * <p>The first two states are all this service can know on its own: the
 * order was accepted locally, and the event was handed to the broker. The
 * remaining states come back asynchronously over
 * {@code order-results-stream}, published by inventory-service once it has
 * actually decided - that return leg is what makes
 * {@code GET /orders/{orderId}} worth reading.
 */
public enum OrderStatus {

	/** Persisted locally, publish to the broker not yet confirmed. */
	CREATED,

	/** Successfully published to {@code orders-stream}, awaiting a reservation result. */
	PUBLISHED,

	/** inventory-service reserved the requested quantity. */
	RESERVED,

	/** Rejected: the item exists but didn't have enough stock. */
	REJECTED_INSUFFICIENT_STOCK,

	/** Rejected: inventory-service has no such item. */
	REJECTED_UNKNOWN_ITEM,

	/**
	 * inventory-service gave up on the order (dead-lettered it) without ever
	 * reaching a reservation decision. Distinct from a rejection: nothing is
	 * known about stock, only that processing failed permanently.
	 */
	FAILED;

	/**
	 * Whether no further status change is expected. Used to stop pushing SSE
	 * updates for an order that is done, and to ignore a late or duplicate
	 * result for one that already has an answer.
	 */
	public boolean isTerminal() {
		return this != CREATED && this != PUBLISHED;
	}
}
