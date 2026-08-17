package com.lvl.mds.inventoryservice.repository;

import com.lvl.mds.inventoryservice.model.ReservationOutcome;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryRepositoryTest {

	private final InventoryRepository repository = new InventoryRepository();

	@Test
	void reservesStockWhenAvailable() {
		repository.seed("item-1", 10);

		ReservationOutcome outcome = repository.reserve("item-1", 4);

		assertThat(outcome).isEqualTo(ReservationOutcome.RESERVED);
		assertThat(repository.availableQuantity("item-1")).isEqualTo(6);
	}

	@Test
	void rejectsReservationWhenInsufficientStock() {
		repository.seed("item-1", 3);

		ReservationOutcome outcome = repository.reserve("item-1", 5);

		assertThat(outcome).isEqualTo(ReservationOutcome.INSUFFICIENT_STOCK);
		assertThat(repository.availableQuantity("item-1")).isEqualTo(3);
	}

	@Test
	void rejectsReservationForUnknownItem() {
		ReservationOutcome outcome = repository.reserve("unknown-item", 1);

		assertThat(outcome).isEqualTo(ReservationOutcome.ITEM_NOT_FOUND);
	}
}
