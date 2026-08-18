package com.lvl.mds.orderapi.services;

import com.lvl.mds.orderapi.dto.OrderResponseDto;

/**
 * Raised whenever an order's status actually changes, carrying the new
 * snapshot.
 *
 * <p>An in-JVM Spring application event (rather than the service calling the
 * SSE registry directly) keeps the dependency pointing the right way: the
 * service layer knows nothing about HTTP, and the web layer subscribes to
 * what it cares about. It also means the messaging layer, which is what
 * triggers these changes, doesn't need to know that a streaming endpoint
 * exists at all.
 */
public record OrderStatusChangedEvent(OrderResponseDto order) {
}
