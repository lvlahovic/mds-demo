package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.config.OrderStatusStreamProperties;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.services.OrderService;
import com.lvl.mds.orderapi.services.OrderStatusChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.SmartLifecycle;
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
 *
 * <p>This is also a {@link SmartLifecycle}, and that is not incidental. With
 * {@code server.shutdown=graceful} the web server waits for active requests to
 * finish before it stops, and an SSE subscription is an active (async)
 * request - one that by design does not finish until the order is decided or
 * the 5-minute timeout expires. Left alone, a single client watching a pending
 * order would stall every shutdown until
 * {@code spring.lifecycle.timeout-per-shutdown-phase} ran out. So the open
 * emitters are completed here first, one phase above
 * {@link WebServerApplicationContext#GRACEFUL_SHUTDOWN_PHASE}: clients see a
 * clean end of stream and reconnect to whatever instance replaces this one,
 * rather than a connection reset mid-shutdown.
 */
@Component
public class OrderStatusStream implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(OrderStatusStream.class);

	private static final String STATUS_EVENT = "status";

	private final OrderService orderService;
	private final long timeoutMs;

	private final Map<String, List<SseEmitter>> subscribersByOrderId = new HashMap<>();

	private volatile boolean running;

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
				sendAndComplete(orderId, emitter, current);
				return emitter;
			}

			if (!running) {
				// Shutting down: this instance will never deliver an update, so
				// answer with the snapshot and close rather than hold open a
				// connection that only delays the shutdown it raced.
				log.debug("Refusing to hold a status subscription for order {} during shutdown", orderId);
				sendAndComplete(orderId, emitter, current);
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

	@Override
	public void start() {
		running = true;
	}

	/**
	 * Ends every open subscription cleanly. Runs before the web server starts
	 * waiting for in-flight requests, and after the result stream listener has
	 * stopped (it sits at {@link Integer#MAX_VALUE}), so any result that
	 * arrived during shutdown has already been pushed to these clients.
	 */
	@Override
	public synchronized void stop() {
		running = false;

		List<SseEmitter> open = new ArrayList<>();
		for (List<SseEmitter> perOrder : subscribersByOrderId.values()) {
			open.addAll(perOrder);
		}
		subscribersByOrderId.clear();

		for (SseEmitter emitter : open) {
			emitter.complete();
		}

		if (!open.isEmpty()) {
			log.info("Closed {} open status subscription(s) before shutdown", open.size());
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	/**
	 * One above the web server's graceful-shutdown phase: higher phases stop
	 * first, so the subscriptions are closed before anything starts waiting for
	 * them to close by themselves.
	 */
	@Override
	public int getPhase() {
		return WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE + 1;
	}

	private void sendAndComplete(String orderId, SseEmitter emitter, OrderResponseDto order) {
		if (send(orderId, emitter, order)) {
			emitter.complete();
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
	 * possibly on the thread already inside {@link #onOrderStatusChanged} or
	 * {@link #stop()} - {@code synchronized} being reentrant is what makes that
	 * safe.
	 */
	private synchronized void detach(String orderId, SseEmitter emitter) {
		List<SseEmitter> subscribers = subscribersByOrderId.get(orderId);
		if (subscribers != null && subscribers.remove(emitter) && subscribers.isEmpty()) {
			subscribersByOrderId.remove(orderId);
		}
	}
}
