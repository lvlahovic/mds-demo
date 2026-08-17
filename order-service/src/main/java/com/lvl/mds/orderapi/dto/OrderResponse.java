package com.lvl.mds.orderapi.dto;

/**
 * Response returned once an order event has been published to the broker.
 * Publishing is fire-and-forget from the caller's perspective - it does not
 * mean the order was reserved, only that it was accepted and handed off to
 * the Inventory Processing Service for asynchronous processing.
 */
public record OrderResponse(String orderId, String status) {

	public static OrderResponse published(String orderId) {
		return new OrderResponse(orderId, "PUBLISHED");
	}
}
