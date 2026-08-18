package com.lvl.mds.orderapi.dto;

import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;

import java.time.Instant;

/**
 * API representation of an {@link Order}. The model itself never leaves the
 * service layer - {@link com.lvl.mds.orderapi.services.OrderService} maps to
 * this before returning anything to the controller. It is also the payload
 * pushed over the {@code GET /orders/{orderId}/status} SSE stream, so the
 * polling and streaming views of an order are literally the same shape.
 */
public record OrderResponseDto(
		String orderId,
		String itemId,
		int quantity,
		OrderStatus status,
		String statusReason,
		Instant updatedAt
) {

	public static OrderResponseDto from(Order order) {
		return new OrderResponseDto(order.getOrderId(), order.getItemId(), order.getQuantity(),
				order.getStatus(), order.getStatusReason(), order.getUpdatedAt());
	}
}
