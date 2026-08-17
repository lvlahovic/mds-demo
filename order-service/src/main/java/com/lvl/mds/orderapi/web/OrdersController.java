package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderRequest;
import com.lvl.mds.orderapi.dto.OrderResponse;
import com.lvl.mds.orderapi.messaging.OrderEventPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrdersController {

	private final OrderEventPublisher orderEventPublisher;

	public OrdersController(OrderEventPublisher orderEventPublisher) {
		this.orderEventPublisher = orderEventPublisher;
	}

	/**
	 * Accepts an order and publishes it to the broker. Returns 202 Accepted -
	 * reservation is processed asynchronously by the Inventory Processing
	 * Service, so success here only means the event was durably published,
	 * not that stock was reserved.
	 */
	@PostMapping
	public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody OrderRequest request) {
		orderEventPublisher.publish(request);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(OrderResponse.published(request.orderId()));
	}
}
