package com.lvl.mds.orderapi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * Request payload for {@code POST /orders}.
 */
public record OrderRequest(

		@NotBlank(message = "orderId is required")
		String orderId,

		@NotBlank(message = "itemId is required")
		String itemId,

		@NotNull(message = "quantity is required")
		@Positive(message = "quantity must be greater than zero")
		Integer quantity
) {
}
