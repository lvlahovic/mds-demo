package com.lvl.mds.inventoryservice.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Business content of an {@code order.created} event - the inbound half of
 * the contract with order-service, and the only part of an order this
 * service ever sees.
 *
 * <p>{@code ignoreUnknown} is what makes this a tolerant reader: a newer
 * order-service may add fields, and this consumer skips them instead of
 * failing on them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedPayload(String orderId, String itemId, int quantity) {

	public static final String EVENT_TYPE = "order.created";
}
