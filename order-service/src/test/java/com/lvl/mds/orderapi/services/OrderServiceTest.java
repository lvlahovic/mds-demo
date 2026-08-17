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
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

	@Mock
	private OrderEventPublisher orderEventPublisher;

	private final OrderRepository orderRepository = new OrderRepository();

	private OrderService newService() {
		return new OrderService(orderRepository, orderEventPublisher);
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
}
