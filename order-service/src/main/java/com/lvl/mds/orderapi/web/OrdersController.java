package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.services.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrdersController {

	private final OrderService orderService;
	private final OrderStatusStream orderStatusStream;

	public OrdersController(OrderService orderService, OrderStatusStream orderStatusStream) {
		this.orderService = orderService;
		this.orderStatusStream = orderStatusStream;
	}

	/**
	 * Accepts an order and publishes it to the broker. Returns 202 Accepted -
	 * reservation is processed asynchronously by the Inventory Processing
	 * Service, so success here only means the event was durably published,
	 * not that stock was reserved. The reservation outcome arrives later, over
	 * the result stream; watch for it with {@link #streamOrderStatus} or poll
	 * {@link #getOrder}.
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
	public OrderResponseDto getOrder(@PathVariable String orderId) {
		return orderService.getOrder(orderId)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order '" + orderId + "' not found"));
	}

	/**
	 * Streams the order's status as Server-Sent Events: the current state
	 * immediately, then every change, then the connection closes once the
	 * order reaches a terminal state. Saves clients from polling
	 * {@link #getOrder} while the reservation is being processed.
	 */
	@GetMapping(value = "/{orderId}/status", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public SseEmitter streamOrderStatus(@PathVariable String orderId) {
		return orderStatusStream.subscribe(orderId);
	}

	@DeleteMapping("/{orderId}")
	public ResponseEntity<Void> deleteOrder(@PathVariable String orderId) {
		if (!orderService.deleteOrder(orderId)) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Order '" + orderId + "' not found");
		}
		return ResponseEntity.noContent().build();
	}
}
