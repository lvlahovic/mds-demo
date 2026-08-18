package com.lvl.mds.inventoryservice.config;

import com.lvl.mds.inventoryservice.repository.InventoryRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Seeds a handful of items at startup - no database, so this is the only
 * place stock quantities come from.
 *
 * <p>Seeding runs in {@code @PostConstruct}, not as a {@code CommandLineRunner}:
 * runners execute after the context is fully refreshed, by which point the
 * stream listener container has already started consuming. On a restart with
 * a backlog waiting on {@code orders-stream} that lost the race - orders were
 * processed against an empty inventory and rejected as unknown items. The
 * listener container declares {@code @DependsOn} on this bean to make the
 * ordering explicit rather than incidental.
 */
@Component
public class InventorySeedInitializer {

	private static final Logger log = LoggerFactory.getLogger(InventorySeedInitializer.class);

	private final InventoryRepository inventoryRepository;

	public InventorySeedInitializer(InventoryRepository inventoryRepository) {
		this.inventoryRepository = inventoryRepository;
	}

	@PostConstruct
	public void seedInventory() {
		inventoryRepository.seed("item-1", 100);
		inventoryRepository.seed("item-2", 50);
		inventoryRepository.seed("item-3", 5);
		log.info("Seeded in-memory inventory: item-1=100, item-2=50, item-3=5");
	}
}
