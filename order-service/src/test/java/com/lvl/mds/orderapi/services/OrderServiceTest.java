package com.lvl.mds.orderapi.services;

import com.lvl.mds.orderapi.dto.OrderRequestDto;
import com.lvl.mds.orderapi.dto.OrderResponseDto;
import com.lvl.mds.orderapi.messaging.OrderEventPublisher;
import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;
import com.lvl.mds.orderapi.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

	@Mock
	private OrderEventPublisher orderEventPublisher;

	@Mock
	private ApplicationEventPublisher applicationEventPublisher;

	private final OrderRepository orderRepository = new OrderRepository();

	private OrderService newService() {
		return new OrderService(orderRepository, orderEventPublisher, applicationEventPublisher);
	}

	@Test
	void createOrderPersistsAsPublishedOnSuccess() {
		OrderService service = newService();
		OrderRequestDto request = new OrderRequestDto("order-1", "item-1", 2);

		OrderResponseDto created = service.createOrder(request);

		assertThat(created.status()).isEqualTo(OrderStatus.PUBLISHED);
		assertThat(orderRepository.findById("order-1")).get()
				.extracting(Order::getStatus).isEqualTo(OrderStatus.PUBLISHED);
		verify(orderEventPublisher).publish(request);
	}

	@Test
	void createOrderLeavesOrderAsCreatedWhenPublishFails() {
		OrderService service = newService();
		OrderRequestDto request = new OrderRequestDto("order-1", "item-1", 2);
		doThrow(new RuntimeException("redis down")).when(orderEventPublisher).publish(request);

		assertThatThrownBy(() -> service.createOrder(request)).isInstanceOf(RuntimeException.class);

		assertThat(orderRepository.findById("order-1")).get()
				.extracting(Order::getStatus).isEqualTo(OrderStatus.CREATED);
	}

	@Test
	void createOrderRejectsDuplicateOrderId() {
		OrderService service = newService();
		OrderRequestDto request = new OrderRequestDto("order-1", "item-1", 2);
		service.createOrder(request);

		assertThatThrownBy(() -> service.createOrder(request))
				.isInstanceOf(ResponseStatusException.class)
				.hasMessageContaining("already exists");
	}

	@Test
	void deleteOrderReturnsWhetherOrderExisted() {
		OrderService service = newService();
		service.createOrder(new OrderRequestDto("order-1", "item-1", 2));

		assertThat(service.deleteOrder("order-1")).isTrue();
		assertThat(service.getOrder("order-1")).isEmpty();
		assertThat(service.deleteOrder("order-1")).isFalse();
	}

	@Test
	void reservationResultMovesOrderToItsTerminalStateAndAnnouncesIt() {
		OrderService service = newService();
		service.createOrder(new OrderRequestDto("order-1", "item-1", 2));

		boolean applied = service.applyReservationResult("order-1", OrderStatus.RESERVED, "reserved 2 of item 'item-1'");

		assertThat(applied).isTrue();
		assertThat(service.getOrder("order-1")).get()
				.satisfies(order -> {
					assertThat(order.status()).isEqualTo(OrderStatus.RESERVED);
					assertThat(order.statusReason()).isEqualTo("reserved 2 of item 'item-1'");
				});
		verify(applicationEventPublisher).publishEvent(any(OrderStatusChangedEvent.class));
	}

	/**
	 * At-least-once delivery means a redelivered result is normal traffic,
	 * not an error - the first answer stands and nothing is re-announced.
	 */
	@Test
	void reservationResultForAnAlreadyTerminalOrderIsIgnored() {
		OrderService service = newService();
		service.createOrder(new OrderRequestDto("order-1", "item-1", 2));
		service.applyReservationResult("order-1", OrderStatus.RESERVED, "reserved");

		boolean applied = service.applyReservationResult("order-1", OrderStatus.FAILED, "late duplicate");

		assertThat(applied).isFalse();
		assertThat(service.getOrder("order-1")).get()
				.extracting(OrderResponseDto::status).isEqualTo(OrderStatus.RESERVED);
	}

	@Test
	void reservationResultForAnUnknownOrderIsIgnored() {
		OrderService service = newService();

		assertThat(service.applyReservationResult("order-gone", OrderStatus.RESERVED, "reserved")).isFalse();
		verify(applicationEventPublisher, never()).publishEvent(any(OrderStatusChangedEvent.class));
	}
}
