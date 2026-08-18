package com.lvl.mds.inventoryservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code inventory.shutdown.*} - how long shutdown is willing to wait
 * for work that is already in progress.
 *
 * @param listenerDrainTimeoutMs how long to wait for the stream listener to
 *                               finish the order it is processing before the
 *                               context closes anyway. Keep it below
 *                               {@code spring.lifecycle.timeout-per-shutdown-phase},
 *                               and keep that below the container runtime's
 *                               grace period ({@code stop_grace_period} in
 *                               compose), or the process is killed mid-drain
 *                               and the wait buys nothing.
 */
@ConfigurationProperties(prefix = "inventory.shutdown")
public record ShutdownProperties(long listenerDrainTimeoutMs) {
}
