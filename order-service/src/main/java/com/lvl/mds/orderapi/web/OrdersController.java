package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.services.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrdersController {

	private final OrderService orderService;

	public OrdersController(OrderService orderService) {
		this.orderService = orderService;
	}

	/**
	 * Accepts an order and publishes it to the broker. Returns 202 Accepted -
	 * reservation is processed asynchronously by the Inventory Processing
	 * Service, so success here only means the event was durably published,
	 * not that stock was reserved.
	 */
	@PostMapping
	public ResponseEntity<OrderResponseDto> createOrder(@Valid @RequestBody OrderRequestDto request) {
		OrderResponseDto response = orderService.createOrder(request);
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
	}

	@GetMapping
	public List<OrderResponseDto> getAllOrders() {
		return orderService.getAllOrders();
	}

	@GetMapping("/{orderId}")
	public ResponseEntity<OrderResponseDto> getOrder(@PathVariable String orderId) {
		return orderService.getOrder(orderId)
				.map(ResponseEntity::ok)
				.orElseGet(() -> ResponseEntity.notFound().build());
	}

	@DeleteMapping("/{orderId}")
	public ResponseEntity<Void> deleteOrder(@PathVariable String orderId) {
		boolean deleted = orderService.deleteOrder(orderId);
		return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}
}
