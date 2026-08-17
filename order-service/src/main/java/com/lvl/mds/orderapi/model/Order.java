package com.lvl.mds.orderapi.model;

/**
 * In-memory record of an order accepted by this service - order-service's
 * own bookkeeping, separate from the event handed to Redis Streams.
 */
public class Order {

	private final String orderId;
	private final String itemId;
	private final int quantity;
	private OrderStatus status;

	public Order(String orderId, String itemId, int quantity, OrderStatus status) {
		this.orderId = orderId;
		this.itemId = itemId;
		this.quantity = quantity;
		this.status = status;
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

	public void setStatus(OrderStatus status) {
		this.status = status;
	}
}
