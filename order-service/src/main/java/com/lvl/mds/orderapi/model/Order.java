package com.lvl.mds.orderapi.model;

import java.time.Instant;

/**
 * In-memory record of an order accepted by this service - order-service's
 * own bookkeeping, separate from the event handed to Redis Streams.
 */
public class Order {

	private final String orderId;
	private final String itemId;
	private final int quantity;
	private OrderStatus status;
	private String statusReason;
	private Instant updatedAt;

	public Order(String orderId, String itemId, int quantity, OrderStatus status) {
		this.orderId = orderId;
		this.itemId = itemId;
		this.quantity = quantity;
		this.status = status;
		this.updatedAt = Instant.now();
	}

	public String getOrderId() {
		return orderId;
	}

	public String getItemId() {
		return itemId;
	}

	public int getQuantity() {
		return quantity;
	}

	public OrderStatus getStatus() {
		return status;
	}

	/**
	 * Human-readable explanation for the current status, as reported by
	 * inventory-service (e.g. "insufficient stock for item 'item-3'
	 * (requested 999)"). Null while the order has only been published.
	 */
	public String getStatusReason() {
		return statusReason;
	}

	public Instant getUpdatedAt() {
		return updatedAt;
	}

	public void setStatus(OrderStatus status, String statusReason) {
		this.status = status;
		this.statusReason = statusReason;
		this.updatedAt = Instant.now();
	}
}
