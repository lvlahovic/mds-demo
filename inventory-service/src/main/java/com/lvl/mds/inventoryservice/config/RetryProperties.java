package com.lvl.mds.inventoryservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code inventory.retry.*} - controls the reclaim job that recovers
 * messages left unacknowledged in the Pending Entries List, e.g. because a
 * consumer crashed mid-processing.
 */
@ConfigurationProperties(prefix = "inventory.retry")
public record RetryProperties(
		long pendingThresholdMs,
		long scanIntervalMs,
		int maxAttempts
) {
}
