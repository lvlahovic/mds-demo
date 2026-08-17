package com.lvl.mds.orderapi.services;

import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.messaging.OrderEventPublisher;
import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.repository.OrderRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

/**
 * CRUD over order-service's own bookkeeping ({@link OrderRepository}), plus
 * the side effect of publishing to the broker on creation. Basic
 * create/read/delete are exposed through {@code OrdersController}; update
 * exists here for completeness of the CRUD layer even without a dedicated
 * endpoint for it yet.
 *
 * <p>{@link Order} is the internal domain model and never leaves this
 * class - every method here returns {@link OrderResponseDto} instead, so
 * the controller only ever deals in DTOs.
 */
@Service
public class OrderService {

	private final OrderRepository orderRepository;
	private final OrderEventPublisher orderEventPublisher;

	public OrderService(OrderRepository orderRepository, OrderEventPublisher orderEventPublisher) {
		this.orderRepository = orderRepository;
		this.orderEventPublisher = orderEventPublisher;
	}

	/**
	 * Persists the order locally as {@code CREATED}, then publishes it to
	 * the broker and flips it to {@code PUBLISHED}. If publishing throws,
	 * the order is left behind in {@code CREATED} - that's a genuine,
	 * visible signal (via {@link #getOrder}) that it never reached the
	 * broker, rather than being silently lost.
	 */
	public OrderResponseDto createOrder(OrderRequestDto request) {
		if (orderRepository.existsById(request.orderId())) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "Order '" + request.orderId() + "' already exists");
		}

		Order order = new Order(request.orderId(), request.itemId(), request.quantity(), OrderStatus.CREATED);
		orderRepository.save(order);

		orderEventPublisher.publish(request);
		order.setStatus(OrderStatus.PUBLISHED);
		orderRepository.save(order);

		return OrderResponseDto.from(order);
	}

	public Optional<OrderResponseDto> getOrder(String orderId) {
		return orderRepository.findById(orderId).map(OrderResponseDto::from);
	}

	public List<OrderResponseDto> getAllOrders() {
		return orderRepository.findAll().stream().map(OrderResponseDto::from).toList();
	}

	public OrderResponseDto updateStatus(String orderId, OrderStatus status) {
		Order order = orderRepository.findById(orderId)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order '" + orderId + "' not found"));
		order.setStatus(status);
		orderRepository.save(order);
		return OrderResponseDto.from(order);
	}

	public boolean deleteOrder(String orderId) {
		return orderRepository.deleteById(orderId);
	}
}
