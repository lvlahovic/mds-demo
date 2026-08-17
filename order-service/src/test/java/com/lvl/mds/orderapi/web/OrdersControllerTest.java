package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.messaging.OrderEventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrdersController.class)
class OrdersControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private OrderEventPublisher orderEventPublisher;

	@Test
	void acceptsValidOrderAndPublishesIt() throws Exception {
		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":2}
								"""))
				.andExpect(status().isAccepted());

		verify(orderEventPublisher).publish(any());
	}

	@Test
	void rejectsNonPositiveQuantity() throws Exception {
		mockMvc.perform(post("/orders")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"orderId":"order-1","itemId":"item-1","quantity":0}
								"""))
				.andExpect(status().isBadRequest());

		verifyNoInteractions(orderEventPublisher);
	}
}
