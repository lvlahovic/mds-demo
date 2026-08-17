package com.lvl.mds.inventoryservice.config;

import com.lvl.mds.inventoryservice.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Seeds a handful of items at startup - no database, so this is the only
 * place stock quantities come from.
 */
@Component
public class InventorySeedInitializer implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(InventorySeedInitializer.class);

	private final InventoryRepository inventoryRepository;

	public InventorySeedInitializer(InventoryRepository inventoryRepository) {
		this.inventoryRepository = inventoryRepository;
	}

	@Override
	public void run(String... args) {
		inventoryRepository.seed("item-1", 100);
		inventoryRepository.seed("item-2", 50);
		inventoryRepository.seed("item-3", 5);
		log.info("Seeded in-memory inventory: item-1=100, item-2=50, item-3=5");
	}
}
