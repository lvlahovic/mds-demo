package com.lvl.mds.orderapi.model;

/**
 * Lifecycle of an order as tracked by order-service itself.
 *
 * <p>This is deliberately NOT the reservation outcome (reserved / rejected
 * for insufficient stock / etc.) - that decision is made asynchronously by
 * inventory-service and isn't reported back over any channel in this
 * exercise. order-service can only ever know as far as "the event was
 * handed to the broker", so that's all this enum claims to represent.
 */
public enum OrderStatus {

	/** Persisted locally, publish to the broker not yet confirmed. */
	CREATED,

	/** Successfully published to {@code orders-stream}. */
	PUBLISHED
}
