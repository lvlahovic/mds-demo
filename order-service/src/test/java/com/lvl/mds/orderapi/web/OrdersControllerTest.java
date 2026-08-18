package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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
				.andExpect(status().isBadRequest());

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
				.andExpect(status().isNotFound());
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
				.andExpect(status().isNotFound());
	}
}
