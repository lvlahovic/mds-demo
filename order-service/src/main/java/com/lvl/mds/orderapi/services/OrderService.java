package com.lvl.mds.orderapi.services;

import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.messaging.producers.OrderEventPublisher;
import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

/**
 * CRUD over order-service's own bookkeeping ({@link OrderRepository}), plus
 * the side effect of publishing to the broker on creation and the state
 * transitions driven by reservation results coming back from
 * inventory-service.
 *
 * <p>{@link Order} is the internal domain model and never leaves this
 * class - every method here returns {@link OrderResponseDto} instead, so
 * the controller only ever deals in DTOs.
 */
@Service
public class OrderService {

	private static final Logger log = LoggerFactory.getLogger(OrderService.class);

	private final OrderRepository orderRepository;
	private final OrderEventPublisher orderEventPublisher;
	private final ApplicationEventPublisher applicationEventPublisher;

	public OrderService(OrderRepository orderRepository,
			OrderEventPublisher orderEventPublisher,
			ApplicationEventPublisher applicationEventPublisher) {
		this.orderRepository = orderRepository;
		this.orderEventPublisher = orderEventPublisher;
		this.applicationEventPublisher = applicationEventPublisher;
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
		order.setStatus(OrderStatus.PUBLISHED, "published to orders-stream");
		orderRepository.save(order);

		return OrderResponseDto.from(order);
	}

	public Optional<OrderResponseDto> getOrder(String orderId) {
		return orderRepository.findById(orderId).map(OrderResponseDto::from);
	}

	public List<OrderResponseDto> getAllOrders() {
		return orderRepository.findAll().stream().map(OrderResponseDto::from).toList();
	}

	/**
	 * Applies a reservation outcome received over {@code order-results-stream}.
	 *
	 * <p>Two deliberate no-ops, because at-least-once delivery makes both
	 * routine rather than exceptional:
	 * <ul>
	 *   <li>an unknown orderId (the order was deleted, or this service was
	 *       restarted and lost its in-memory state) is logged and ignored -
	 *       the message is still acknowledged by the caller, since retrying it
	 *       would never start succeeding;</li>
	 *   <li>an order already in a terminal state keeps its first answer. A
	 *       redelivered result is by definition the same decision, and this
	 *       also stops a late duplicate from resurrecting a finished order.</li>
	 * </ul>
	 *
	 * @return whether the order actually changed state
	 */
	public boolean applyReservationResult(String orderId, OrderStatus status, String reason) {
		Order order = orderRepository.findById(orderId).orElse(null);
		if (order == null) {
			log.warn("Received reservation result {} for unknown order {} - ignoring", status, orderId);
			return false;
		}
		if (order.getStatus().isTerminal()) {
			log.info("Order {} is already {} - ignoring duplicate result {}", orderId, order.getStatus(), status);
			return false;
		}

		order.setStatus(status, reason);
		orderRepository.save(order);
		log.info("Order {} is now {} ({})", orderId, status, reason);

		applicationEventPublisher.publishEvent(new OrderStatusChangedEvent(OrderResponseDto.from(order)));
		return true;
	}

	public boolean deleteOrder(String orderId) {
		return orderRepository.deleteById(orderId);
	}
}
