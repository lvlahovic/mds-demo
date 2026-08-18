package com.lvl.mds.orderapi.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Business content of an {@code order.created} event - the outbound half of
 * the contract with inventory-service.
 *
 * <p>Separate from {@code OrderRequestDto} even though the fields currently
 * match: that one is the HTTP contract and carries validation constraints,
 * this one is the event contract. Sharing a single type would make every
 * change to the API a change to the event, and vice versa.
 *
 * <p>{@code ignoreUnknown} is what makes the reading side a tolerant reader:
 * a newer producer may add fields, and an older consumer skips them instead
 * of failing.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedPayload(String orderId, String itemId, int quantity) {

	public static final String EVENT_TYPE = "order.created";
}
