package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrdersController.class)
class OrdersControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private OrderService orderService;

	@Test
	void acceptsValidOrderAndReturnsIt() throws Exception {
		OrderResponseDto created = new OrderResponseDto("order-1", "item-1", 2, OrderStatus.PUBLISHED);
		when(orderService.createOrder(any())).thenReturn(created);

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
		OrderResponseDto order = new OrderResponseDto("order-1", "item-1", 2, OrderStatus.PUBLISHED);
		when(orderService.getOrder("order-1")).thenReturn(Optional.of(order));

		mockMvc.perform(get("/orders/order-1"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.orderId").value("order-1"))
				.andExpect(jsonPath("$.itemId").value("item-1"))
				.andExpect(jsonPath("$.quantity").value(2));
	}

	@Test
	void getOrderReturns404WhenMissing() throws Exception {
		when(orderService.getOrder("missing")).thenReturn(Optional.empty());

		mockMvc.perform(get("/orders/missing"))
				.andExpect(status().isNotFound());
	}

	@Test
	void getAllOrdersReturnsList() throws Exception {
		when(orderService.getAllOrders()).thenReturn(
				List.of(new OrderResponseDto("order-1", "item-1", 2, OrderStatus.PUBLISHED)));

		mockMvc.perform(get("/orders"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].orderId").value("order-1"));
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
