package com.lvl.mds.orderapi.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Business content of an {@code order.reservation-completed} event - the
 * inbound half of the contract, published by inventory-service and consumed
 * here.
 *
 * <p>{@code outcome} stays a plain string rather than an enum: it is
 * inventory-service's vocabulary, and this service must be able to receive a
 * value a newer version of that service invented (see
 * {@link ReservationResultProcessor}). {@code quantity} is boxed because a
 * dead-lettered event may not know it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReservationResultPayload(
		String orderId,
		String itemId,
		Integer quantity,
		String outcome,
		String reason
) {

	public static final String EVENT_TYPE = "order.reservation-completed";
}
