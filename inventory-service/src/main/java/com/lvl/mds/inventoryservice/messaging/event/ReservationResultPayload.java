package com.lvl.mds.inventoryservice.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Business content of an {@code order.reservation-completed} event - the
 * outbound half of the contract, published by this service and consumed by
 * order-service.
 *
 * <p>{@code outcome} is a plain string rather than {@code ReservationOutcome}:
 * the wire vocabulary is deliberately a superset of the domain enum, because
 * it also has to be able to say {@code FAILED} - "we never got as far as
 * deciding". {@code quantity} is boxed because a dead-lettered event may not
 * know it.
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
