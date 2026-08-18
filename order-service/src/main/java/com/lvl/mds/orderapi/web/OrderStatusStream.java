package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.config.OrderStatusStreamProperties;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.services.OrderService;
import com.lvl.mds.orderapi.services.OrderStatusChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps the open SSE connections for {@code GET /orders/{orderId}/status} and
 * pushes each order's status changes to whoever is watching it.
 *
 * <p>The point of the endpoint is that the reservation result arrives
 * asynchronously: without it a client has to poll {@code GET /orders/{orderId}}
 * until the status stops being {@code PUBLISHED}. Server-Sent Events fit
 * better than WebSockets here because the traffic is strictly one-way and an
 * {@code EventSource} reconnects on its own; the connection closes as soon as
 * the order reaches a terminal state, so it is a bounded subscription rather
 * than an open-ended channel.
 *
 * <p>Subscribing sends the current state immediately and only then starts
 * following updates. That ordering is what makes the endpoint safe to call at
 * any moment - including after the result already arrived, in which case the
 * client gets one event and the stream completes.
 *
 * <p>Registration and delivery are guarded by one lock, which is what
 * guarantees a subscriber can't slip in between the snapshot and an update
 * and miss it. The cost is that a slow client can hold up delivery to others;
 * for the scale this exercise runs at that trade is worth the simplicity, and
 * the production answer would be a per-connection queue with dispatch off the
 * caller's thread.
 */
@Component
public class OrderStatusStream {

	private static final Logger log = LoggerFactory.getLogger(OrderStatusStream.class);

	private static final String STATUS_EVENT = "status";

	private final OrderService orderService;
	private final long timeoutMs;

	private final Map<String, List<SseEmitter>> subscribersByOrderId = new HashMap<>();

	public OrderStatusStream(OrderService orderService, OrderStatusStreamProperties properties) {
		this.orderService = orderService;
		this.timeoutMs = properties.timeoutMs();
	}

	/**
	 * @throws ResponseStatusException 404 if there is no such order - streaming
	 *                                 the status of something that doesn't
	 *                                 exist would just hang the client
	 */
	public SseEmitter subscribe(String orderId) {
		OrderResponseDto current = orderService.getOrder(orderId)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order '" + orderId + "' not found"));

		SseEmitter emitter = new SseEmitter(timeoutMs);
		emitter.onCompletion(() -> detach(orderId, emitter));
		emitter.onTimeout(() -> detach(orderId, emitter));
		emitter.onError(ex -> detach(orderId, emitter));

		synchronized (this) {
			if (current.status().isTerminal()) {
				// Already decided: one event, then done - no subscription needed.
				if (send(orderId, emitter, current)) {
					emitter.complete();
				}
				return emitter;
			}

			subscribersByOrderId.computeIfAbsent(orderId, id -> new ArrayList<>()).add(emitter);
			send(orderId, emitter, current);
		}

		log.debug("SSE subscriber attached to order {}", orderId);
		return emitter;
	}

	@EventListener
	public synchronized void onOrderStatusChanged(OrderStatusChangedEvent event) {
		OrderResponseDto order = event.order();
		List<SseEmitter> subscribers = subscribersByOrderId.get(order.orderId());
		if (subscribers == null) {
			return;
		}

		// Copy: a failing send detaches its own emitter from this same list.
		for (SseEmitter emitter : List.copyOf(subscribers)) {
			if (send(order.orderId(), emitter, order) && order.status().isTerminal()) {
				emitter.complete();
			}
		}

		if (order.status().isTerminal()) {
			subscribersByOrderId.remove(order.orderId());
		}
	}

	private boolean send(String orderId, SseEmitter emitter, OrderResponseDto order) {
		try {
			emitter.send(SseEmitter.event().name(STATUS_EVENT).data(order, MediaType.APPLICATION_JSON));
			return true;
		} catch (Exception ex) {
			// Almost always just a client that hung up mid-stream.
			log.debug("Dropping SSE subscriber for order {}: {}", orderId, ex.toString());
			detach(orderId, emitter);
			return false;
		}
	}

	/**
	 * Called both directly and from the emitter's own completion callbacks,
	 * possibly on the thread already inside {@link #onOrderStatusChanged} -
	 * {@code synchronized} being reentrant is what makes that safe.
	 */
	private synchronized void detach(String orderId, SseEmitter emitter) {
		List<SseEmitter> subscribers = subscribersByOrderId.get(orderId);
		if (subscribers != null && subscribers.remove(emitter) && subscribers.isEmpty()) {
			subscribersByOrderId.remove(orderId);
		}
	}
}
