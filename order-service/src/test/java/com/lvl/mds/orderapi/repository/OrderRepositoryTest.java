package com.lvl.mds.orderapi.repository;

import com.lvl.mds.orderapi.model.Order;
import com.lvl.mds.orderapi.model.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderRepositoryTest {

	private final OrderRepository repository = new OrderRepository();

	@Test
	void savesAndFindsById() {
		repository.save(new Order("order-1", "item-1", 2, OrderStatus.CREATED));

		assertThat(repository.findById("order-1"))
				.get()
				.extracting(Order::getStatus)
				.isEqualTo(OrderStatus.CREATED);
	}

	@Test
	void findByIdIsEmptyWhenMissing() {
		assertThat(repository.findById("missing")).isEmpty();
	}

	@Test
	void existsByIdReflectsStoredOrders() {
		assertThat(repository.existsById("order-1")).isFalse();

		repository.save(new Order("order-1", "item-1", 1, OrderStatus.CREATED));

		assertThat(repository.existsById("order-1")).isTrue();
	}

	@Test
	void deleteByIdRemovesOrderAndReturnsWhetherItExisted() {
		repository.save(new Order("order-1", "item-1", 1, OrderStatus.CREATED));

		assertThat(repository.deleteById("order-1")).isTrue();
		assertThat(repository.findById("order-1")).isEmpty();
		assertThat(repository.deleteById("order-1")).isFalse();
	}

	@Test
	void findAllReturnsEveryStoredOrder() {
		repository.save(new Order("order-1", "item-1", 1, OrderStatus.CREATED));
		repository.save(new Order("order-2", "item-2", 2, OrderStatus.PUBLISHED));

		assertThat(repository.findAll()).hasSize(2);
	}
}
