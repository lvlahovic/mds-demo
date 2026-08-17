package com.lvl.mds.orderapi.dto;

import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;

/**
 * API representation of an {@link Order}. The model itself never leaves the
 * service layer - {@link com.lvl.mds.orderapi.services.OrderService} maps to
 * this before returning anything to the controller.
 */
public record OrderResponseDto(String orderId, String itemId, int quantity, OrderStatus status) {

	public static OrderResponseDto from(Order order) {
		return new OrderResponseDto(order.getOrderId(), order.getItemId(), order.getQuantity(), order.getStatus());
	}
}
