package com.lvl.mds.orderapi.web;

import com.lvl.mds.orderapi.config.OrderStatusStreamProperties;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.services.OrderService;
import com.lvl.mds.orderapi.services.OrderStatusChangedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Drives the SSE endpoint through the real {@link OrderStatusStream} (only
 * {@link OrderService} is stubbed) so what is asserted is what a client would
 * actually receive on the wire.
 */
@WebMvcTest(OrdersController.class)
@Import(OrderStatusStream.class)
@EnableConfigurationProperties(OrderStatusStreamProperties.class)
@TestPropertySource(properties = "order.status-stream.timeout-ms=5000")
class OrderStatusStreamTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private OrderStatusStream orderStatusStream;

	@MockitoBean
	private OrderService orderService;

	private static OrderResponseDto order(OrderStatus status, String reason) {
		return new OrderResponseDto("order-1", "item-1", 2, status, reason, Instant.now());
	}

	@Test
	void sendsTheCurrentStatusImmediatelyThenEveryChange() throws Exception {
		when(orderService.getOrder("order-1"))
				.thenReturn(Optional.of(order(OrderStatus.PUBLISHED, "published to orders-stream")));

		MvcResult result = mockMvc.perform(get("/orders/order-1/status"))
				.andExpect(request().asyncStarted())
				.andReturn();

		assertThat(result.getResponse().getContentAsString())
				.contains("event:status")
				.contains("\"status\":\"PUBLISHED\"");

		orderStatusStream.onOrderStatusChanged(
				new OrderStatusChangedEvent(order(OrderStatus.RESERVED, "reserved 2 of item 'item-1'")));

		assertThat(result.getResponse().getContentAsString())
				.contains("\"status\":\"RESERVED\"")
				.contains("reserved 2 of item");
	}

	/**
	 * A client that subscribes after the result already arrived must still get
	 * an answer - the snapshot - rather than an idle connection.
	 */
	@Test
	void completesImmediatelyForAnOrderThatIsAlreadyDecided() throws Exception {
		when(orderService.getOrder("order-1"))
				.thenReturn(Optional.of(order(OrderStatus.REJECTED_INSUFFICIENT_STOCK, "insufficient stock")));

		MvcResult result = mockMvc.perform(get("/orders/order-1/status")).andReturn();

		assertThat(result.getResponse().getContentAsString())
				.contains("\"status\":\"REJECTED_INSUFFICIENT_STOCK\"");
	}

	@Test
	void returns404ForAnUnknownOrder() throws Exception {
		when(orderService.getOrder("missing")).thenReturn(Optional.empty());

		mockMvc.perform(get("/orders/missing/status"))
				.andExpect(status().isNotFound());
	}

	/**
	 * {@code stop()} is what {@link OrderStatusStream}'s graceful-shutdown
	 * phase calls: a subscriber watching a still-pending order must see a
	 * clean end of stream rather than have its connection reset when the
	 * server actually goes down, and must not be delivered anything that
	 * arrives afterwards - by then this instance is no longer the one
	 * publishing results.
	 */
	@Test
	@DirtiesContext
	void stopClosesOpenSubscriptionsSoTheClientSeesACleanEndOfStream() throws Exception {
		when(orderService.getOrder("order-1"))
				.thenReturn(Optional.of(order(OrderStatus.PUBLISHED, "published to orders-stream")));

		MvcResult result = mockMvc.perform(get("/orders/order-1/status"))
				.andExpect(request().asyncStarted())
				.andReturn();

		orderStatusStream.stop();

		// Arrives after the subscription was already closed by stop() - must
		// not be delivered to a subscriber that no longer exists.
		orderStatusStream.onOrderStatusChanged(
				new OrderStatusChangedEvent(order(OrderStatus.RESERVED, "reserved 2 of item 'item-1'")));

		assertThat(result.getResponse().getContentAsString())
				.contains("event:status")
				.contains("\"status\":\"PUBLISHED\"")
				.doesNotContain("\"status\":\"RESERVED\"");
	}

	/**
	 * Once {@code stop()} has run, this instance will never deliver an
	 * update - holding a new subscription open would just be a connection
	 * that outlives the shutdown for no reason, so it gets the snapshot and
	 * an immediate close instead, the same as an already-decided order.
	 */
	@Test
	@DirtiesContext
	void subscribingWhileShuttingDownGetsTheSnapshotThenClosesImmediately() throws Exception {
		when(orderService.getOrder("order-1"))
				.thenReturn(Optional.of(order(OrderStatus.PUBLISHED, "published to orders-stream")));

		orderStatusStream.stop();

		MvcResult result = mockMvc.perform(get("/orders/order-1/status")).andReturn();

		assertThat(result.getResponse().getContentAsString())
				.contains("\"status\":\"PUBLISHED\"");
	}
}
