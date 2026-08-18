package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrdersController.class)
class OrdersControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private OrderService orderService;

	@MockitoBean
	private OrderStatusStream orderStatusStream;

	private static OrderResponseDto order(OrderStatus status) {
		return new OrderResponseDto("order-1", "item-1", 2, status, "published to orders-stream", Instant.now());
	}

	@Test
	void acceptsValidOrderAndReturnsIt() throws Exception {
		when(orderService.createOrder(any())).thenReturn(order(OrderStatus.PUBLISHED));

		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":2}
								"""))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.orderId").value("order-1"))
				.andExpect(jsonPath("$.status").value("PUBLISHED"));
	}

	@Test
	void rejectsNonPositiveQuantity() throws Exception {
		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":0}
								"""))
				.andExpect(status().isBadRequest())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.title").value("Validation failed"))
				.andExpect(jsonPath("$.status").value(400))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/bad-request"))
				.andExpect(jsonPath("$.errors.quantity").value("quantity must be greater than zero"));

		verifyNoInteractions(orderService);
	}

	/**
	 * {@link com.lvl.mds.orderapi.services.OrderService#createOrder} reports a
	 * duplicate {@code orderId} as a {@code ResponseStatusException} - proves
	 * that path also comes back as the same {@code application/problem+json}
	 * shape as everything {@link ApiExceptionHandler} handles directly.
	 */
	@Test
	void createOrderConflictIsReportedAsProblemDetail() throws Exception {
		when(orderService.createOrder(any()))
				.thenThrow(new ResponseStatusException(HttpStatus.CONFLICT, "Order 'order-1' already exists"));

		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":2}
								"""))
				.andExpect(status().isConflict())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.detail").value("Order 'order-1' already exists"))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/conflict"));
	}

	/**
	 * Redis being down while {@code OrderEventPublisher} publishes is a known,
	 * distinct failure mode - not a bug - so it gets 503 with a
	 * {@code Retry-After} hint rather than {@link ApiExceptionHandler}'s
	 * generic 500 fallback.
	 */
	@Test
	void brokerUnavailableIsReportedAsServiceUnavailable() throws Exception {
		when(orderService.createOrder(any()))
				.thenThrow(new RedisConnectionFailureException("Unable to connect to Redis"));

		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":2}
								"""))
				.andExpect(status().isServiceUnavailable())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(header().string(HttpHeaders.RETRY_AFTER, "5"))
				.andExpect(jsonPath("$.detail").value("Order broker is temporarily unavailable - try again shortly"))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/service-unavailable"))
				.andExpect(jsonPath("$.detail", not(containsString("Unable to connect"))));
	}

	/**
	 * A bug or a dependency failure (e.g. Redis unreachable) that escapes the
	 * service layer as a plain {@code RuntimeException} must still come back
	 * as {@code application/problem+json} - not a stack trace - with the real
	 * cause kept out of the response body.
	 */
	@Test
	void unexpectedExceptionIsReportedAsProblemDetailWithoutLeakingDetails() throws Exception {
		when(orderService.createOrder(any())).thenThrow(new RuntimeException("redis down"));

		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":2}
								"""))
				.andExpect(status().isInternalServerError())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.detail").value("An unexpected error occurred"))
				.andExpect(jsonPath("$.detail", not(containsString("redis down"))))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/internal-server-error"));
	}

	/**
	 * Malformed JSON never reaches {@link jakarta.validation.Valid} - it fails
	 * one layer earlier, as {@code HttpMessageNotReadableException}. Same
	 * {@code application/problem+json} contract applies regardless.
	 */
	@Test
	void malformedJsonBodyIsReportedAsProblemDetail() throws Exception {
		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{not-json"))
				.andExpect(status().isBadRequest())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/bad-request"));

		verifyNoInteractions(orderService);
	}

	@Test
	void getOrderReturns200WhenFound() throws Exception {
		when(orderService.getOrder("order-1")).thenReturn(Optional.of(order(OrderStatus.RESERVED)));

		mockMvc.perform(get("/orders/order-1"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.orderId").value("order-1"))
				.andExpect(jsonPath("$.itemId").value("item-1"))
				.andExpect(jsonPath("$.quantity").value(2))
				.andExpect(jsonPath("$.status").value("RESERVED"))
				.andExpect(jsonPath("$.statusReason").exists())
				.andExpect(jsonPath("$.updatedAt").exists());
	}

	@Test
	void getOrderReturns404WhenMissing() throws Exception {
		when(orderService.getOrder("missing")).thenReturn(Optional.empty());

		mockMvc.perform(get("/orders/missing"))
				.andExpect(status().isNotFound())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.detail").value("Order 'missing' not found"))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/not-found"));
	}

	@Test
	void getAllOrdersReturnsList() throws Exception {
		when(orderService.getAllOrders()).thenReturn(List.of(order(OrderStatus.PUBLISHED)));

		mockMvc.perform(get("/orders"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].orderId").value("order-1"));
	}

	@Test
	void statusStreamOpensAnEventStream() throws Exception {
		when(orderStatusStream.subscribe("order-1")).thenReturn(new SseEmitter(1_000L));

		mockMvc.perform(get("/orders/order-1/status"))
				.andExpect(request().asyncStarted());
	}

	@Test
	void statusStreamReturns404ForAnUnknownOrder() throws Exception {
		when(orderStatusStream.subscribe("missing"))
				.thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND, "Order 'missing' not found"));

		mockMvc.perform(get("/orders/missing/status"))
				.andExpect(status().isNotFound());
	}

	@Test
	void deleteOrderReturns204WhenDeleted() throws Exception {
		when(orderService.deleteOrder("order-1")).thenReturn(true);

		mockMvc.perform(delete("/orders/order-1"))
				.andExpect(status().isNoContent());

		verify(orderService).deleteOrder("order-1");
	}

	@Test
	void deleteOrderReturns404WhenMissing() throws Exception {
		when(orderService.deleteOrder(eq("missing"))).thenReturn(false);

		mockMvc.perform(delete("/orders/missing"))
				.andExpect(status().isNotFound())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.detail").value("Order 'missing' not found"))
				.andExpect(jsonPath("$.type").value("https://order-service.mds-demo/problems/not-found"));
	}
}
